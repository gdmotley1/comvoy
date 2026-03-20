-- Pre-computed market metrics per snapshot for agent intelligence
CREATE TABLE snapshot_metrics (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    snapshot_id     UUID NOT NULL REFERENCES report_snapshots(id) ON DELETE CASCADE,
    computed_at     TIMESTAMPTZ DEFAULT NOW(),
    metrics         JSONB NOT NULL,
    UNIQUE(snapshot_id)
);

CREATE INDEX idx_snapshot_metrics_snap ON snapshot_metrics(snapshot_id);
