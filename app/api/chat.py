"""Chat endpoint — the agent loop that powers the web interface.

Token-efficiency guardrails:
- Capped output tokens (4096)
- Max 5 tool-use loop iterations
- Sliding-window conversation history (30 messages)
- Tool result truncation (12000 chars)
- Per-request usage logging with cost estimates
"""

import asyncio
import json
import logging
import time
from datetime import date

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from anthropic import Anthropic, APIError, APITimeoutError, AuthenticationError, RateLimitError

from app.config import settings
from app.models import ChatMessage
from app.agent.prompts import SALES_AGENT_SYSTEM_PROMPT, SALES_KNOWLEDGE_BASE
from app.agent.tools import TOOL_DEFINITIONS, execute_tool

router = APIRouter(prefix="/api", tags=["chat"])
logger = logging.getLogger(__name__)

# Human-friendly tool status messages
TOOL_STATUS_MAP = {
    "search_dealers": "Searching dealers...",
    "find_nearby_dealers": "Finding nearby dealers...",
    "get_dealer_briefing": "Pulling dealer briefing...",
    "get_territory_summary": "Analyzing territory...",
    "get_dealer_trend": "Loading dealer trends...",
    "get_territory_trend": "Loading territory trends...",
    "get_alerts": "Checking alerts...",
    "get_lead_scores": "Scoring leads...",
    "get_route_dealers": "Planning route...",
    "get_dealer_intel": "Gathering dealer intel...",
    "get_dealer_places": "Looking up business details...",
    "search_vehicles": "Searching vehicles...",
    "get_dealer_inventory": "Loading inventory...",
    "get_inventory_changes": "Checking inventory changes...",
    "get_price_analytics": "Analyzing pricing...",
    "get_market_intel": "Gathering market intel...",
    "suggest_travel_plan": "Building travel plan...",
    "get_upload_report": "Loading report data...",
}

import re

# Simple query classifier — routes easy questions to Haiku for speed
_SIMPLE_PATTERNS = [
    r"^how many\b",
    r"^what('?s| is) the (total|count|number)",
    r"^(list|show|give me) .{0,30}(states?|brands?|dealers?)\s*$",
    r"^(hi|hello|hey|thanks|thank you|ok|okay|got it|cool|bye)",
    r"^what (do|can) you (do|help)",
    r"^who (are|r) you",
]
_SIMPLE_RE = re.compile("|".join(_SIMPLE_PATTERNS), re.IGNORECASE)


def _pick_model(message: str) -> str:
    """Route simple queries to Haiku, complex ones to Sonnet."""
    msg = message.strip()
    # Short off-topic / greeting / simple count queries → fast model
    if len(msg) < 60 and _SIMPLE_RE.search(msg):
        logger.info(f"Routing to fast model (Haiku): {msg[:50]}")
        return settings.agent_model_fast
    return settings.agent_model


# In-memory conversation history (per-session) with timestamps for expiry
_conversations: dict[str, list] = {}
_session_last_active: dict[str, float] = {}
_SESSION_TTL = 3600  # 1 hour — expire idle sessions

# Rate limiter: max 20 chat requests per minute per session
_rate_window: dict[str, list[float]] = {}
_RATE_LIMIT = 20
_RATE_WINDOW_SEC = 60

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


def _prepare_chat(msg: ChatMessage, session_id: str):
    """Shared setup for both streaming and non-streaming chat endpoints.
    Returns (history, system_prompt, client) or raises HTTPException.
    """
    # Rate limiting
    now = time.time()
    hits = _rate_window.get(session_id, [])
    hits = [t for t in hits if now - t < _RATE_WINDOW_SEC]
    if len(hits) >= _RATE_LIMIT:
        raise HTTPException(429, "Rate limit exceeded. Please wait a moment before sending another message.")
    hits.append(now)
    _rate_window[session_id] = hits

    # Expire stale sessions
    _session_last_active[session_id] = now
    stale = [sid for sid, ts in _session_last_active.items() if now - ts > _SESSION_TTL]
    for sid_key in stale:
        _conversations.pop(sid_key, None)
        _session_last_active.pop(sid_key, None)
        _rate_window.pop(sid_key, None)

    # No API key
    if not settings.anthropic_api_key:
        logger.warning("No Anthropic API key configured")
        return None

    client = Anthropic(api_key=settings.anthropic_api_key, timeout=180.0)

    # History recovery & pruning
    if session_id not in _conversations:
        if msg.history:
            _conversations[session_id] = msg.history
            logger.info(f"Recovered {len(msg.history)} messages from client for session {session_id}")
        else:
            _conversations[session_id] = []

    history = _conversations[session_id]
    history.append({"role": "user", "content": msg.message})
    history = _prune_history(history, settings.agent_max_history)
    _conversations[session_id] = history

    system_prompt = f"TODAY: {date.today().isoformat()}\n\n{SALES_KNOWLEDGE_BASE}\n\n{SALES_AGENT_SYSTEM_PROMPT}"
    model = _pick_model(msg.message)

    return history, system_prompt, client, model


@router.post("/chat")
async def chat(msg: ChatMessage, session_id: str = "default"):
    """Non-streaming chat endpoint (kept as fallback)."""
    result = _prepare_chat(msg, session_id)
    if result is None:
        return {
            "response": None, "session_id": session_id, "usage": None,
            "error": "no_api_key",
            "error_message": "Otto needs an Anthropic API key to work. Add ANTHROPIC_API_KEY to your .env file and restart the server.",
        }

    history, system_prompt, client, model = result
    start_time = time.time()
    total_input_tokens = 0
    total_output_tokens = 0
    tools_called = []

    for iteration in range(settings.agent_max_loop):
        try:
            response = client.messages.create(
                model=model,
                max_tokens=settings.agent_max_tokens,
                system=system_prompt,
                tools=TOOL_DEFINITIONS,
                messages=history,
            )
        except AuthenticationError:
            return {"response": "API key is invalid.", "session_id": session_id, "usage": None}
        except RateLimitError:
            return {"response": "Rate limit reached. Wait a moment.", "session_id": session_id, "usage": None}
        except APITimeoutError:
            return {"response": "Request timed out. Try a simpler question.", "session_id": session_id, "usage": None}
        except APIError as e:
            logger.exception(f"Anthropic API error: {e}")
            return {"response": "Something went wrong.", "session_id": session_id, "usage": None}

        total_input_tokens += response.usage.input_tokens
        total_output_tokens += response.usage.output_tokens

        if response.stop_reason == "tool_use":
            history.append({"role": "assistant", "content": response.content})
            tool_blocks = [b for b in response.content if b.type == "tool_use"]
            for block in tool_blocks:
                logger.info(f"Agent tool [{iteration+1}/{settings.agent_max_loop}]: {block.name}({block.input})")
                tools_called.append(block.name)

            if len(tool_blocks) > 1:
                # Parallel execution for multiple tools
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor() as pool:
                    futures = {pool.submit(execute_tool, b.name, b.input): b for b in tool_blocks}
                    tool_results = []
                    for future in concurrent.futures.as_completed(futures):
                        block = futures[future]
                        result = _truncate_tool_result(future.result(), settings.agent_tool_result_cap)
                        tool_results.append({"type": "tool_result", "tool_use_id": block.id, "content": result})
            else:
                block = tool_blocks[0]
                result = execute_tool(block.name, block.input)
                result = _truncate_tool_result(result, settings.agent_tool_result_cap)
                tool_results = [{"type": "tool_result", "tool_use_id": block.id, "content": result}]

            history.append({"role": "user", "content": tool_results})
            continue

        text_parts = [block.text for block in response.content if hasattr(block, "text")]
        assistant_text = "\n".join(text_parts)
        history.append({"role": "assistant", "content": assistant_text})

        elapsed = time.time() - start_time
        cost = _estimate_cost(total_input_tokens, total_output_tokens)
        logger.info(f"Chat complete — {total_input_tokens:,} in + {total_output_tokens:,} out = ~${cost:.4f} | {len(tools_called)} tools | {elapsed:.1f}s")

        return {
            "response": assistant_text, "session_id": session_id,
            "usage": {
                "input_tokens": total_input_tokens, "output_tokens": total_output_tokens,
                "total_tokens": total_input_tokens + total_output_tokens,
                "estimated_cost_usd": round(cost, 6), "tools_called": tools_called,
                "loop_iterations": iteration + 1, "elapsed_seconds": round(elapsed, 2),
            },
        }

    elapsed = time.time() - start_time
    cost = _estimate_cost(total_input_tokens, total_output_tokens)
    return {
        "response": "I gathered a lot of data but hit my thinking limit. Try a more specific question.",
        "session_id": session_id,
        "usage": {
            "input_tokens": total_input_tokens, "output_tokens": total_output_tokens,
            "total_tokens": total_input_tokens + total_output_tokens,
            "estimated_cost_usd": round(cost, 6), "tools_called": tools_called,
            "loop_iterations": settings.agent_max_loop, "elapsed_seconds": round(elapsed, 2),
        },
    }


def _sse_event(event: str, data: dict) -> str:
    """Format a Server-Sent Event."""
    return f"event: {event}\ndata: {json.dumps(data)}\n\n"


@router.post("/chat/stream")
async def chat_stream(msg: ChatMessage, session_id: str = "default"):
    """Streaming chat endpoint — SSE events for real-time UI updates."""
    result = _prepare_chat(msg, session_id)
    if result is None:
        return StreamingResponse(
            iter([_sse_event("error", {"message": "No API key configured."})]),
            media_type="text/event-stream",
        )

    history, system_prompt, client, model = result

    async def generate():
        start_time = time.time()
        total_input_tokens = 0
        total_output_tokens = 0
        tools_called = []

        try:
            _SENTINEL = object()
            loop = asyncio.get_running_loop()

            for iteration in range(settings.agent_max_loop):
                # Stream every API call. Tool-use iterations accumulate the
                # full response before processing; the final text response
                # yields deltas to the client in real-time.
                chunk_q = asyncio.Queue()
                final_msg_box = []

                def _make_stream_runner(msgs, tools):
                    def _run():
                        kwargs = dict(
                            model=model,
                            max_tokens=settings.agent_max_tokens,
                            system=system_prompt,
                            messages=msgs,
                        )
                        if tools:
                            kwargs["tools"] = tools
                        with client.messages.stream(**kwargs) as stream:
                            for text in stream.text_stream:
                                asyncio.run_coroutine_threadsafe(chunk_q.put(text), loop)
                            asyncio.run_coroutine_threadsafe(chunk_q.put(_SENTINEL), loop)
                            final_msg_box.append(stream.get_final_message())
                    return _run

                try:
                    future = loop.run_in_executor(
                        None, _make_stream_runner(history, TOOL_DEFINITIONS)
                    )

                    # Yield text deltas in real-time as they arrive from the API.
                    # For tool-use iterations this is usually preamble text like
                    # "Let me look that up..." which is fine to show.
                    full_text = ""
                    while True:
                        chunk = await chunk_q.get()
                        if chunk is _SENTINEL:
                            break
                        full_text += chunk
                        yield _sse_event("delta", {"text": chunk})

                    await future  # propagate exceptions
                except AuthenticationError:
                    yield _sse_event("error", {"message": "API key is invalid."})
                    return
                except RateLimitError:
                    yield _sse_event("error", {"message": "Rate limit reached. Wait a moment."})
                    return
                except APITimeoutError:
                    yield _sse_event("error", {"message": "Request timed out. Try a simpler question."})
                    return
                except APIError as e:
                    logger.exception(f"Anthropic API error: {e}")
                    yield _sse_event("error", {"message": "Something went wrong."})
                    return

                response = final_msg_box[0]
                total_input_tokens += response.usage.input_tokens
                total_output_tokens += response.usage.output_tokens

                if response.stop_reason == "tool_use":
                    history.append({"role": "assistant", "content": response.content})
                    tool_blocks = [b for b in response.content if b.type == "tool_use"]

                    # Emit status for all tools first
                    for block in tool_blocks:
                        status_msg = TOOL_STATUS_MAP.get(block.name, "Working...")
                        yield _sse_event("status", {"status": status_msg, "tool": block.name})
                        logger.info(f"Agent tool [{iteration+1}/{settings.agent_max_loop}]: {block.name}")
                        tools_called.append(block.name)

                    # Execute tools in parallel when multiple
                    if len(tool_blocks) > 1:
                        async def _run_tool(b):
                            return b.id, await asyncio.to_thread(execute_tool, b.name, b.input)
                        results = await asyncio.gather(*[_run_tool(b) for b in tool_blocks])
                        tool_results = [
                            {"type": "tool_result", "tool_use_id": tid,
                             "content": _truncate_tool_result(res, settings.agent_tool_result_cap)}
                            for tid, res in results
                        ]
                    else:
                        block = tool_blocks[0]
                        tool_result = await asyncio.to_thread(execute_tool, block.name, block.input)
                        tool_result = _truncate_tool_result(tool_result, settings.agent_tool_result_cap)
                        tool_results = [{"type": "tool_result", "tool_use_id": block.id, "content": tool_result}]

                    history.append({"role": "user", "content": tool_results})
                    continue

                # Final text response — deltas already yielded above
                history.append({"role": "assistant", "content": full_text})

                # Done event with usage
                elapsed = time.time() - start_time
                cost = _estimate_cost(total_input_tokens, total_output_tokens)
                logger.info(f"Stream complete — {total_input_tokens:,} in + {total_output_tokens:,} out = ~${cost:.4f} | {len(tools_called)} tools | {elapsed:.1f}s")

                yield _sse_event("done", {
                    "full_text": full_text,
                    "usage": {
                        "input_tokens": total_input_tokens, "output_tokens": total_output_tokens,
                        "total_tokens": total_input_tokens + total_output_tokens,
                        "estimated_cost_usd": round(cost, 6), "tools_called": tools_called,
                        "loop_iterations": iteration + 1, "elapsed_seconds": round(elapsed, 2),
                    },
                })
                return

            # Max iterations exhausted
            yield _sse_event("done", {
                "full_text": "I gathered a lot of data but hit my thinking limit. Try a more specific question.",
                "usage": {
                    "input_tokens": total_input_tokens, "output_tokens": total_output_tokens,
                    "total_tokens": total_input_tokens + total_output_tokens,
                    "estimated_cost_usd": round(_estimate_cost(total_input_tokens, total_output_tokens), 6),
                    "tools_called": tools_called,
                    "loop_iterations": settings.agent_max_loop,
                    "elapsed_seconds": round(time.time() - start_time, 2),
                },
            })

        except Exception as e:
            logger.exception(f"Stream error: {e}")
            yield _sse_event("error", {"message": "Something went wrong."})

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.delete("/chat/{session_id}")
async def clear_chat(session_id: str):
    """Clear conversation history for a session."""
    _conversations.pop(session_id, None)
    return {"status": "cleared"}
