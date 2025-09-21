DROP TABLE IF EXISTS STV2025061321__DWH.global_metrics;

CREATE TABLE IF NOT EXISTS STV2025061321__DWH.global_metrics (
    date_update DATE,
    currency_from INT,
    amount_total NUMERIC(18,2),
    cnt_transactions INT,
    avg_transactions_per_account NUMERIC(18,2),
    cnt_accounts_make_transactions INT
)
ORDER BY date_update
SEGMENTED BY HASH(date_update, currency_from) ALL NODES KSAFE 1
PARTITION BY date_update;
