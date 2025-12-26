# Financial Services Project

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

## Workflow image project
![alt text](<ChatGPT Image Dec 24, 2025 at 11_04_36 AM.png>)
![alt text](<mlops engineer ready.webp>)