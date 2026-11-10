CREATE TABLE fraud_alert (
    id              UUID PRIMARY KEY,
    transaction_id  UUID          NOT NULL,
    card_id         VARCHAR(64)   NOT NULL,
    rule            VARCHAR(40)   NOT NULL,
    amount          NUMERIC(19,2) NOT NULL,
    details         JSONB         NOT NULL,
    status          VARCHAR(20)   NOT NULL DEFAULT 'OPEN'
                    CHECK (status IN ('OPEN','CONFIRMED','FALSE_POSITIVE')),
    transaction_at  TIMESTAMPTZ   NOT NULL,
    detected_at     TIMESTAMPTZ   NOT NULL,
    CONSTRAINT uk_alert_transaction_rule UNIQUE (transaction_id, rule)
);
CREATE INDEX idx_alert_card ON fraud_alert (card_id);
CREATE INDEX idx_alert_detected_at ON fraud_alert (detected_at DESC);
