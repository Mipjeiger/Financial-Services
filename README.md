# Kredivo Project

## Logs progress

- Use postgreSQL to connect on local host ≠ in Docker for using postgreSQL
- Create SCHEMA raw, feature, scores in postgresql for ingestion load CSV dataset for analyzing
- Set environment in kredivo project for reliabling project
- Success exporting data SQL from python to postgreSQL with table
    - Data loaded into finance table.
    Data loaded into marketing table.
    Finance table row count: 2784
    Marketing table row count: 41188
- Set experiment on notebook divided by marketing and finance .ipynb
- Use Tensorflow as framework machine/deep learning model to preprocess data
- Success export csv to notebook from postgreSQL ingesting in .py
- Create customer_id for unique values for each data in SQL
- Create new business insight for fraud detection
- SQL table ready for financial services project
    - SELECT * FROM raw.finance;
    - SELECT * FROM raw.marketing;
    - SELECT * FROM feature.marketing;
    - SELECT * FROM feature.finance;
    - SELECT * FROM marketing_scores;
    - SELECT * FROM feature.finance_fraud_features;
    - SELECT * FROM feature.marketing_fraud_features;
    - SELECT * FROM feature.feature_fraud;
    - SELECT * FROM label.fraud_label;
    - SELECT * FROM feature.training_fraud_dataset;
    - *SELECT* *** *FROM* feature.finance_fraud_daily;
    - *SELECT* *** *FROM* feature.marketing_fraud_daily;
    - *SELECT* *** *FROM* feature.feature_fraud_daily;
    - *SELECT* *** *FROM* *label*.fraud_label_daily;
    - *SELECT* *** *FROM* feature.training_fraud_daily;
    - *SELECT* *** *FROM* alert.daily_fraud_alert;
- Renew feature.training_fraud_dataset; for compatible dividen fraud_label between 0 and 1 scores to 20% fraud_label = 1 and 80% for fraud_label = 0

## Workflow image project
![alt text](<images/ChatGPT Image Dec 24, 2025 at 11_04_36 AM.png>)
![alt text](<images/mlops engineer ready.webp>)

# 1️⃣ OUTPUT — Daily Fraud Risk (SQL daily + fraud_score)

📌 **Ini yang PALING SESUAI dengan data CSV kamu sekarang**

### 🎯 Artinya

> “Hari ini sistem menunjukkan risiko fraud sekian”
> 

### Contoh output (`feature.training_fraud_daily`)

| event_day | tx_count | avg_tx_amount | risky_tx_count | ctr | fraud_score | fraud_label | fraud_reason |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 2025-12-23 | 120 | 180,000 | 1 | 0.05 | 0.22 | 0 | NORMAL |
| 2025-12-24 | 95 | 210,000 | 2 | 0.04 | 0.46 | 0 | NORMAL |
| 2025-12-25 | 140 | 490,000 | 6 | 0.01 | **0.82** | **1** | HIGH_TX_VS_BALANCE |

📌 **Interpretasi**

- ❌ Bukan “transaksi mana”
- ✅ “Tanggal 25 Desember risk tinggi”
- Cocok untuk **alert / monitoring**

---

# 2️⃣ OUTPUT — Fraud Alert (Business-Friendly)

Biasanya dipakai **tanpa ML dulu**.

### Contoh output (alert table)

| event_day | fraud_score | alert_level | action |
| --- | --- | --- | --- |
| 2025-12-25 | 0.82 | HIGH | Freeze high-value tx |
| 2025-12-24 | 0.46 | MEDIUM | Monitor |
| 2025-12-23 | 0.22 | LOW | Ignore |

📌 **Ini yang dibaca manajemen / risk team**

---

# 3️⃣ OUTPUT — Unsupervised (Anomaly Detection per transaksi)

📌 **Kalau kamu ingin “per baris transaksi” TANPA label**

### Contoh output (Isolation Forest)

| transaction_amount | account_balance | anomaly_score | is_anomaly |
| --- | --- | --- | --- |
| 120,000 | 300,000 | -0.05 | 0 |
| 180,000 | 250,000 | -0.12 | 0 |
| 950,000 | 1,000,000 | **-0.71** | **1** |
| 1,200,000 | 1,300,000 | **-0.89** | **1** |

📌 **Interpretasi**

- ❌ bukan “fraud”
- ✅ “transaksi ini TIDAK NORMAL”

⚠️ **Anomali ≠ Fraud**

Tapi ini sering dipakai **sebelum ada label**

---

# 4️⃣ OUTPUT — Synthetic Fraud (UNTUK BELAJAR SAJA)

📌 **HANYA untuk demo / latihan**

### Contoh output

| transaction_amount | account_balance | fraud |
| --- | --- | --- |
| 120,000 | 500,000 | 0 |
| 180,000 | 400,000 | 0 |
| 480,000 | 500,000 | 0 |
| 490,000 | 500,000 | **1** |
| 520,000 | 500,000 | **1** |

Rule:

```
transaction_amount > 0.95 * account_balance → fraud

```

⚠️ **Ini bukan fraud nyata**

Tapi cukup untuk:

- latihan MLflow
- CI/CD
- API serving

---

# 5️⃣ OUTPUT — ML Model (kalau nanti datanya lengkap)

📌 **INILAH target akhir fraud detection**

### Contoh output inference

| transaction_id | fraud_probability | fraud_label |
| --- | --- | --- |
| TX1001 | 0.03 | 0 |
| TX1002 | 0.12 | 0 |
| TX1003 | **0.91** | **1** |
| TX1004 | 0.08 | 0 |

📌 **Ini BARU fraud detection sesungguhnya**