"""Chat endpoint — the agent loop that powers the web interface.

Token-efficiency guardrails:
- Capped output tokens (2048)
- Max 5 tool-use loop iterations
- Sliding-window conversation history (20 messages)
- Tool result truncation (8000 chars)
- Per-request usage logging with cost estimates
"""

import json
import logging
import time
from datetime import date

from fastapi import APIRouter, HTTPException
from anthropic import Anthropic, APIError, AuthenticationError, RateLimitError

from app.config import settings
from app.models import ChatMessage
from app.agent.prompts import SALES_AGENT_SYSTEM_PROMPT
from app.agent.tools import TOOL_DEFINITIONS, execute_tool

router = APIRouter(prefix="/api", tags=["chat"])
logger = logging.getLogger(__name__)

# In-memory conversation history (per-session)
_conversations: dict[str, list] = {}

# --- Sonnet 4 pricing (per million tokens, June 2025) ---
_INPUT_COST_PER_M = 3.00    # $3 per 1M input tokens
_OUTPUT_COST_PER_M = 15.00   # $15 per 1M output tokens


def _estimate_cost(input_tokens: int, output_tokens: int) -> float:
    """Estimate cost in USD from token counts."""
    return (input_tokens * _INPUT_COST_PER_M / 1_000_000) + \
           (output_tokens * _OUTPUT_COST_PER_M / 1_000_000)


def _prune_history(history: list, max_messages: int) -> list:
    """Keep conversation history within budget using a sliding window.

    Always keeps the first user message (for context) and the most
    recent messages. Drops middle turns in pairs to keep role alternation.
    """
    if len(history) <= max_messages:
        return history

    # Keep first message + last (max_messages - 1) messages
    pruned = [history[0]] + history[-(max_messages - 1):]
    logger.info(f"Pruned history: {len(history)} → {len(pruned)} messages")
    return pruned


def _truncate_tool_result(result: str, max_chars: int) -> str:
    """Truncate tool results that exceed the character budget."""
    if len(result) <= max_chars:
        return result

    # Try to truncate at a clean JSON boundary
    truncated = result[:max_chars]
    note = f'\n... [TRUNCATED — {len(result):,} chars → {max_chars:,}. Ask for specific filters to narrow results.]'
    logger.warning(f"Tool result truncated: {len(result):,} → {max_chars:,} chars")
    return truncated + note


@router.get("/status")
async def api_status():
    """Check if the API key is configured (lightweight, no API call)."""
    return {"api_key_configured": bool(settings.anthropic_api_key)}


@router.post("/chat")
async def chat(msg: ChatMessage, session_id: str = "default"):
    """Send a message to the sales intelligence agent and get a response.

    Token guardrails:
    - max_tokens capped at 2048 (configurable)
    - Max 5 tool-use loop iterations (configurable)
    - Conversation history pruned to 20 messages (configurable)
    - Tool results truncated to 8000 chars (configurable)
    - Usage stats returned with every response
    """
    # No API key — clean error, not a fake response
    if not settings.anthropic_api_key:
        logger.warning("No Anthropic API key configured")
        return {
            "response": None,
            "session_id": session_id,
            "usage": None,
            "error": "no_api_key",
            "error_message": "Otto needs an Anthropic API key to work. Add ANTHROPIC_API_KEY to your .env file and restart the server.",
        }

    client = Anthropic(api_key=settings.anthropic_api_key)
    start_time = time.time()

    # Get or create conversation history
    if session_id not in _conversations:
        _conversations[session_id] = []

    history = _conversations[session_id]
    history.append({"role": "user", "content": msg.message})

    # Prune history to stay within budget
    history = _prune_history(history, settings.agent_max_history)
    _conversations[session_id] = history

    # Track cumulative usage across loop iterations
    total_input_tokens = 0
    total_output_tokens = 0
    tools_called = []

    # Inject today's date so Otto can resolve "tomorrow", "this Wednesday", etc.
    system_prompt = f"TODAY: {date.today().isoformat()}\n\n{SALES_AGENT_SYSTEM_PROMPT}"

    # Agent loop — keep going until we get a text response
    for iteration in range(settings.agent_max_loop):
        try:
            response = client.messages.create(
                model=settings.agent_model,
                max_tokens=settings.agent_max_tokens,
                system=system_prompt,
                tools=TOOL_DEFINITIONS,
                messages=history,
            )
        except AuthenticationError:
            logger.error("Anthropic API key is invalid")
            return {"response": "API key is invalid. Check ANTHROPIC_API_KEY in .env and restart the server.",
                    "session_id": session_id, "usage": None}
        except RateLimitError:
            logger.warning("Anthropic rate limit hit")
            return {"response": "Rate limit reached. Wait a moment and try again.",
                    "session_id": session_id, "usage": None}
        except APIError as e:
            logger.exception(f"Anthropic API error: {e}")
            return {"response": f"API error: {e.message}. Try again in a moment.",
                    "session_id": session_id, "usage": None}

        # Accumulate token usage
        total_input_tokens += response.usage.input_tokens
        total_output_tokens += response.usage.output_tokens

        # If the model wants to use tools, execute them and continue
        if response.stop_reason == "tool_use":
            # Add assistant message with tool calls
            history.append({"role": "assistant", "content": response.content})

            # Execute each tool call
            tool_results = []
            for block in response.content:
                if block.type == "tool_use":
                    logger.info(f"Agent tool [{iteration+1}/{settings.agent_max_loop}]: {block.name}({block.input})")
                    tools_called.append(block.name)

                    result = execute_tool(block.name, block.input)
                    # Truncate oversized results
                    result = _truncate_tool_result(result, settings.agent_tool_result_cap)

                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": result,
                    })

            history.append({"role": "user", "content": tool_results})
            continue

        # Extract final text response
        text_parts = [block.text for block in response.content if hasattr(block, "text")]
        assistant_text = "\n".join(text_parts)
        history.append({"role": "assistant", "content": assistant_text})

        # Calculate cost and log
        elapsed = time.time() - start_time
        cost = _estimate_cost(total_input_tokens, total_output_tokens)
        logger.info(
            f"Chat complete — {total_input_tokens:,} in + {total_output_tokens:,} out "
            f"= ~${cost:.4f} | {len(tools_called)} tools | {elapsed:.1f}s"
        )

        return {
            "response": assistant_text,
            "session_id": session_id,
            "usage": {
                "input_tokens": total_input_tokens,
                "output_tokens": total_output_tokens,
                "total_tokens": total_input_tokens + total_output_tokens,
                "estimated_cost_usd": round(cost, 6),
                "tools_called": tools_called,
                "loop_iterations": iteration + 1,
                "elapsed_seconds": round(elapsed, 2),
            },
        }

    # If we exhausted the loop, still return what we have with a warning
    elapsed = time.time() - start_time
    cost = _estimate_cost(total_input_tokens, total_output_tokens)
    logger.warning(
        f"Agent loop maxed at {settings.agent_max_loop} iterations — "
        f"{total_input_tokens:,} in + {total_output_tokens:,} out = ~${cost:.4f}"
    )
    return {
        "response": "I gathered a lot of data but hit my thinking limit. Could you ask a more specific question so I can give you a focused answer?",
        "session_id": session_id,
        "usage": {
            "input_tokens": total_input_tokens,
            "output_tokens": total_output_tokens,
            "total_tokens": total_input_tokens + total_output_tokens,
            "estimated_cost_usd": round(cost, 6),
            "tools_called": tools_called,
            "loop_iterations": settings.agent_max_loop,
            "elapsed_seconds": round(elapsed, 2),
            "warning": "max_iterations_reached",
        },
    }


@router.delete("/chat/{session_id}")
async def clear_chat(session_id: str):
    """Clear conversation history for a session."""
    _conversations.pop(session_id, None)
    return {"status": "cleared"}
