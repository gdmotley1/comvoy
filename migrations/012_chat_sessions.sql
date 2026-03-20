-- Chat session persistence + conversation memory
CREATE TABLE IF NOT EXISTS chat_sessions (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id      TEXT NOT NULL,
    user_name       TEXT NOT NULL DEFAULT 'unknown',
    messages        JSONB NOT NULL DEFAULT '[]'::jsonb,
    summary         TEXT,
    summary_tsv     tsvector GENERATED ALWAYS AS (to_tsvector('english', COALESCE(summary, ''))) STORED,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    last_active     TIMESTAMPTZ DEFAULT NOW(),
    is_active       BOOLEAN DEFAULT true
);

CREATE INDEX IF NOT EXISTS idx_chat_sessions_user ON chat_sessions(user_name, last_active DESC);
CREATE INDEX IF NOT EXISTS idx_chat_sessions_active ON chat_sessions(is_active, user_name);
CREATE INDEX IF NOT EXISTS idx_chat_sessions_tsv ON chat_sessions USING GIN(summary_tsv);
CREATE UNIQUE INDEX IF NOT EXISTS idx_chat_sessions_sid ON chat_sessions(session_id);

-- Full-text search RPC for conversation memory
CREATE OR REPLACE FUNCTION search_chat_summaries(
    search_query TEXT,
    p_user_name TEXT,
    p_limit INT DEFAULT 3
)
RETURNS TABLE(id UUID, summary TEXT, last_active TIMESTAMPTZ)
LANGUAGE sql STABLE
AS $$
    SELECT id, summary, last_active
    FROM chat_sessions
    WHERE user_name = p_user_name
      AND is_active = false
      AND summary IS NOT NULL
      AND summary_tsv @@ to_tsquery('english', search_query)
    ORDER BY ts_rank(summary_tsv, to_tsquery('english', search_query)) DESC
    LIMIT p_limit;
$$;
