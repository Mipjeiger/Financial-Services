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



SELECT * FROM raw.finance;

SELECT * FROM raw.marketing;

SELECT * FROM feature.marketing;

SELECT * FROM feature.finance;

SELECT * FROM marketing_scores;

SELECT * FROM feature.finance_fraud_features;