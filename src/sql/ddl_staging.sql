DROP TABLE IF EXISTS STV2025061321__STAGING.transactions;

CREATE TABLE IF NOT EXISTS STV2025061321__STAGING.transactions (
    operation_id       VARCHAR(60),
    account_number_from INT,
    account_number_to   INT,
    currency_code       INT,
    country             VARCHAR(30),
    status              VARCHAR(30),
    transaction_type    VARCHAR(30),
    amount              INT,
    transaction_dt      TIMESTAMP
)
order by transaction_dt, operation_id
segmented by hash(transaction_dt, operation_id) all nodes ksafe 1
partition by transaction_dt::date 
group by calendar_hierarchy_day(transaction_dt::date, 3, 2);

DROP TABLE IF EXISTS STV2025061321__STAGING.currencies;

CREATE TABLE IF NOT EXISTS STV2025061321__STAGING.currencies (
    date_update         TIMESTAMP,
    currency_code       INT,
    currency_code_with  INT,
    currency_with_div   NUMERIC(5,3)
)
order by date_update
segmented by hash(date_update) all nodes ksafe 1
partition by date_update::date
group by calendar_hierarchy_day(date_update::date, 3, 2);
