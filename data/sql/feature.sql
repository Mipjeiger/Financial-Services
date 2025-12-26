-- ========================================
-- SETUP SCHEMAS
-- ========================================
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS feature;

-- ========================================
-- SETUP TABLE DENGAN AUTO CUSTOMER_ID (AA001, AA002, ...)
-- ========================================

DROP TABLE IF EXISTS raw.marketing CASCADE;
DROP SEQUENCE IF EXISTS customer_id_seq CASCADE;

-- Buat Sequence
CREATE SEQUENCE customer_id_seq START 1;

-- Buat Table
CREATE TABLE raw.marketing (
    customer_id VARCHAR(10) PRIMARY KEY,
    clicks INT,
    impressions INT,
    conversion INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Function untuk Generate Customer ID
CREATE OR REPLACE FUNCTION generate_customer_id_aa()
RETURNS TRIGGER AS $$
DECLARE
    seq_num INT;
    padding INT;
BEGIN
    IF NEW.customer_id IS NULL THEN
        seq_num := nextval('customer_id_seq');
        -- Minimal 3 digit: AA001, AA002, ..., AA999, AA1000
        padding := GREATEST(3, LENGTH(seq_num::TEXT));
        NEW.customer_id := 'AA' || LPAD(seq_num::TEXT, padding, '0');
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger
CREATE TRIGGER trg_generate_customer_id
    BEFORE INSERT ON raw.marketing
    FOR EACH ROW
    WHEN (NEW.customer_id IS NULL)
    EXECUTE FUNCTION generate_customer_id_aa();


-- Cek beberapa data pertama
SELECT * FROM raw.marketing ORDER BY customer_id LIMIT 10;

-- Cek beberapa data terakhir
SELECT * FROM raw.marketing ORDER BY customer_id DESC LIMIT 10;

-- Cek total
SELECT COUNT(*) FROM raw.marketing;

-- Feature finance
CREATE TABLE IF NOT EXISTS feature.finance_fraud_features AS
SELECT
    DATE_TRUNC('hour', created_at) AS event_hour,

    COUNT(*) AS tx_count,
    SUM(transaction_amount) AS total_tx_amount,
    AVG(transaction_amount) AS avg_tx_amount,
    MAX(transaction_amount) AS max_tx_amount,
    STDDEV(transaction_amount) AS std_tx_amount,

    AVG(account_balance) AS avg_account_balance,

    SUM(CASE WHEN transaction_amount > account_balance * 0.8 THEN 1 ELSE 0 END) AS risky_tx_count
FROM raw.finance
GROUP BY 1;

-- feature marketing fraud features
DROP TABLE IF EXISTS feature.marketing_fraud_features;
CREATE TABLE feature.marketing_fraud_features AS
SELECT
    DATE_TRUNC('hour', created_at) AS event_hour,

    COUNT(DISTINCT customer_id)    AS active_users,
    SUM(clicks)                    AS total_clicks,
    SUM(impressions)               AS total_impressions,
    AVG(conversion)                AS conversion_rate,

    AVG(clicks)                    AS avg_clicks_per_user

FROM raw.marketing
GROUP BY 1;

-- feature join (core table fraud detection)
-- Cek dulu apakah source tables exist dan ada data
SELECT 'finance_fraud_features' AS table_name, COUNT(*) AS row_count FROM feature.finance_fraud_features
UNION ALL
SELECT 'marketing_fraud_features', COUNT(*) FROM feature.marketing_fraud_features;

DROP TABLE IF EXISTS feature.feature_fraud;
CREATE TABLE feature.feature_fraud AS
SELECT
    f.event_hour,

    -- finance signals
    f.tx_count,
    f.total_tx_amount,
    f.avg_tx_amount,
    f.max_tx_amount,
    f.std_tx_amount,
    f.avg_account_balance,
    f.risky_tx_count,

    -- marketing signals
    m.active_users,
    m.total_clicks,
    m.total_impressions,
    m.conversion_rate,
    m.avg_clicks_per_user,

    -- derived
    f.total_tx_amount / NULLIF(f.tx_count,0)      AS avg_tx_per_event,
    m.total_clicks / NULLIF(m.total_impressions,0) AS ctr

FROM feature.finance_fraud_features f
LEFT JOIN feature.marketing_fraud_features m
ON f.event_hour = m.event_hour;

-- fraud label(Rule-Based)
CREATE TABLE IF NOT EXISTS label.fraud_label AS
SELECT
	event_hour,

	CASE
		WHEN
			risky_tx_count >= 3
			AND tx_count >= 10
			AND ctr < 0.02
		THEN 1
		ELSE 0
	END AS fraud_label,

	CASE
		WHEN risky_tx_count >= 3 THEN 'HIGH_TX_VS_BALANCE'
		WHEN ctr < 0.02 THEN 'LOW_MARKETING_QUALITY'
        ELSE 'NORMAL'
    END AS fraud_reason
FROM feature.feature_fraud;

-- Training dataset (final)
CREATE TABLE IF NOT EXISTS feature.training_fradu_dataset AS
SELECT
	f.*,
	l.fraud_label,
	l.fraud_reason
FROM feature.feature_fraud f
JOIN label.fraud_label l
USING (event_hour);

-- Daily fraud features
DROP TABLE IF EXISTS feature.finance_fraud_daily;

CREATE TABLE feature.finance_fraud_daily AS
SELECT
    DATE(created_at)                    AS event_day,

    COUNT(*)                            AS tx_count,
    SUM(transaction_amount)             AS total_tx_amount,
    AVG(transaction_amount)             AS avg_tx_amount,
    MAX(transaction_amount)             AS max_tx_amount,
    STDDEV(transaction_amount)          AS std_tx_amount,

    AVG(account_balance)                AS avg_account_balance,

    SUM(
        CASE
            WHEN transaction_amount > account_balance * 0.8
            THEN 1 ELSE 0
        END
    )                                   AS risky_tx_count

FROM raw.finance
GROUP BY 1;

-- Marketing daily fraud features
DROP TABLE IF EXISTS feature.marketing_fraud_daily;

CREATE TABLE feature.marketing_fraud_daily AS
SELECT
    DATE(created_at)                    AS event_day,

    COUNT(DISTINCT customer_id)        AS active_users,
    SUM(clicks)                        AS total_clicks,
    SUM(impressions)                   AS total_impressions,
    AVG(conversion::INT)               AS conversion_rate,

    AVG(clicks)                        AS avg_clicks_per_user

FROM raw.marketing
GROUP BY 1;

-- Join feature fraud daily
DROP TABLE IF EXISTS feature.feature_fraud_daily;

CREATE TABLE feature.feature_fraud_daily AS
SELECT
    f.event_day,

    -- finance
    f.tx_count,
    f.total_tx_amount,
    f.avg_tx_amount,
    f.max_tx_amount,
    f.std_tx_amount,
    f.avg_account_balance,
    f.risky_tx_count,

    -- marketing
    m.active_users,
    m.total_clicks,
    m.total_impressions,
    m.conversion_rate,
    m.avg_clicks_per_user,

    -- derived
    f.total_tx_amount / NULLIF(f.tx_count,0)       AS avg_tx_per_event,
    m.total_clicks / NULLIF(m.total_impressions,0) AS ctr

FROM feature.finance_fraud_daily f
LEFT JOIN feature.marketing_fraud_daily m
ON f.event_day = m.event_day;

-- label fraud daily
DROP TABLE IF EXISTS label.fraud_label_daily;

CREATE TABLE label.fraud_label_daily AS
SELECT
    event_day,

    /* =========================
       FRAUD SCORE (0 – 1)
       ========================= */
    LEAST(
        1.0,
        (
            (risky_tx_count::FLOAT / NULLIF(tx_count,1)) * 0.4 +
            (avg_tx_amount / NULLIF(avg_account_balance,1)) * 0.3 +
            (std_tx_amount / NULLIF(avg_tx_amount,1)) * 0.2 +
            (1 - COALESCE(ctr,0)) * 0.1
        )
    ) AS fraud_score,

    /* =========================
       FRAUD LABEL
       ========================= */
    CASE
        WHEN
            (
                (risky_tx_count::FLOAT / NULLIF(tx_count,1)) * 0.4 +
                (avg_tx_amount / NULLIF(avg_account_balance,1)) * 0.3 +
                (std_tx_amount / NULLIF(avg_tx_amount,1)) * 0.2 +
                (1 - COALESCE(ctr,0)) * 0.1
            ) >= 0.7
        THEN 1
        ELSE 0
    END AS fraud_label,

    /* =========================
       REASON (DEBUG / AUDIT)
       ========================= */
    CASE
        WHEN risky_tx_count >= 3 THEN 'HIGH_TX_VS_BALANCE'
        WHEN ctr < 0.01 THEN 'LOW_CTR'
        WHEN std_tx_amount > avg_tx_amount * 2 THEN 'TX_VARIANCE_SPIKE'
        ELSE 'NORMAL'
    END AS fraud_reason

FROM feature.feature_fraud_daily;

DROP TABLE IF EXISTS feature.training_fraud_daily;

CREATE TABLE feature.training_fraud_daily AS
SELECT
    f.*,
    l.fraud_score,
    l.fraud_label,
    l.fraud_reason
FROM feature.feature_fraud_daily f
JOIN label.fraud_label_daily l
USING (event_day);

-- Daily Fraud Risk (SQL Daily + fraud_score)
SELECT
    event_day,
    tx_count,
    ctr,
    fraud_label,
    fraud_score
FROM feature.training_fraud_daily
ORDER BY event_day;

CREATE SCHEMA IF NOT EXISTS alert;

-- Fraud alerting (business-friendly)
CREATE TABLE alert.daily_fraud_alert AS
SELECT
    event_day,
    fraud_score,
    fraud_label,
    CASE
        WHEN fraud_score >= 0.8 THEN 'HIGH RISK'
        WHEN fraud_score >= 0.5 THEN 'MEDIUM RISK'
        ELSE 'LOW RISK'
    END AS alert_level
FROM label.fraud_label_daily;

-- fraud label dataset
DROP TABLE IF EXISTS feature.training_fraud_dataset;
-- 
CREATE TABLE feature.training_fraud_dataset AS
WITH finance_daily AS (
    SELECT
        DATE(created_at) AS event_date,
        COUNT(*) AS tx_count,
        SUM(transaction_amount) AS total_tx_amount,
        AVG(transaction_amount) AS avg_tx_amount,
        MAX(transaction_amount) AS max_tx_amount,
        STDDEV(transaction_amount) AS std_tx_amount,
        AVG(account_balance) AS avg_account_balance
    FROM raw.finance
    GROUP BY DATE(created_at)
),

marketing_daily AS (
    SELECT
        customer_id,
        DATE(created_at) AS event_date,
        SUM(clicks) AS total_clicks,
        SUM(impressions) AS total_impressions,
        SUM(conversion) AS total_conversion,
        CASE
            WHEN SUM(impressions) > 0
            THEN SUM(clicks)::FLOAT / SUM(impressions)
            ELSE 0
        END AS ctr
    FROM raw.marketing
    GROUP BY customer_id, DATE(created_at)
),

scored AS (
    SELECT
        m.customer_id AS user_id,
        m.event_date,

        f.tx_count,
        f.total_tx_amount,
        f.avg_tx_amount,
        f.max_tx_amount,
        f.std_tx_amount,
        f.avg_account_balance,

        m.total_clicks,
        m.total_impressions,
        m.total_conversion,
        m.ctr,

        EXTRACT(DOW FROM m.event_date)   AS weekday,
        EXTRACT(MONTH FROM m.event_date) AS month,
        EXTRACT(YEAR FROM m.event_date)  AS year,

        -- CONTINUOUS FRAUD SCORE
        (f.max_tx_amount / NULLIF(f.avg_tx_amount, 1)) + m.ctr
        AS fraud_score

    FROM marketing_daily m
    JOIN finance_daily f
      ON m.event_date = f.event_date
),

threshold AS (
    SELECT
        PERCENTILE_CONT(0.8)
        WITHIN GROUP (ORDER BY fraud_score) AS fraud_threshold
    FROM scored
)

SELECT
    s.*,
    CASE
        WHEN s.fraud_score >= t.fraud_threshold
        THEN 1 ELSE 0
    END AS fraud_label
FROM scored s
CROSS JOIN threshold t;

SELECT COUNT(*) FROM feature.training_fraud_dataset;

SELECT fraud_label, COUNT(*) 
FROM feature.training_fraud_dataset
GROUP BY fraud_label;

SELECT * FROM feature.training_fraud_dataset;

SELECT * FROM raw.finance;

SELECT * FROM raw.marketing;

SELECT * FROM feature.marketing;

SELECT * FROM feature.finance;

SELECT * FROM marketing_scores;

SELECT * FROM feature.finance_fraud_features;

SELECT * FROM feature.marketing_fraud_features;

SELECT * FROM feature.feature_fraud;

SELECT * FROM label.fraud_label;	

SELECT * FROM feature.training_fraud_dataset;

SELECT * FROM feature.finance_fraud_daily;

SELECT * FROM feature.marketing_fraud_daily;

SELECT * FROM feature.feature_fraud_daily;

SELECT * FROM label.fraud_label_daily;

SELECT * FROM feature.training_fraud_daily;

SELECT * FROM alert.daily_fraud_alert;