-- SANYATTSUYANDEXRU__STAGING
-- table SANYATTSUYANDEXRU__STAGING.currencies;
CREATE TABLE SANYATTSUYANDEXRU__STAGING.currencies
(
    id IDENTITY(1,1),
    currency_code int NOT NULL,
    currency_code_with int NOT NULL,
    date_update date NOT NULL,
    currency_with_div float NOT NULL,
    CONSTRAINT currencies_PK PRIMARY KEY (id) DISABLED
)
ORDER BY id
SEGMENTED BY HASH(id, date_update) ALL NODES
PARTITION BY date_update::date
GROUP BY calendar_hierarchy_day(date_update::date, 3, 2);

-- table SANYATTSUYANDEXRU__STAGING.transactions;
CREATE TABLE SANYATTSUYANDEXRU__STAGING.transactions
(
    operation_id uuid NOT NULL,
	account_number_from int NOT NULL,
	account_number_to int NOT NULL,  
	currency_code int NOT NULL,  
	country varchar NOT NULL,  
	status varchar NOT NULL,  
	transaction_type varchar NOT NULL,  
	amount int NOT NULL,  
	transaction_dt TIMESTAMP NOT NULL,  
    CONSTRAINT transactions_PK PRIMARY KEY (operation_id) DISABLED
)
ORDER BY operation_id
SEGMENTED BY HASH(operation_id, transaction_dt) ALL NODES
PARTITION BY transaction_dt::date
GROUP BY calendar_hierarchy_day(transaction_dt::date, 3, 2);


-- SANYATTSUYANDEXRU__DWH
-- table SANYATTSUYANDEXRU__DWH.global_metrics
CREATE TABLE SANYATTSUYANDEXRU__DWH.global_metrics
(
	date_update date NOT NULL,
	currency_from int NOT NULL,
	amount_total int NOT NULL,
	cnt_transactions int NOT NULL,
	avg_transactions_per_account FLOAT NOT NULL,
	cnt_accounts_make_transactions int NOT NULL
)
ORDER BY date_update, currency_from
SEGMENTED BY HASH(currency_from, date_update) ALL NODES
PARTITION BY date_update::date
GROUP BY calendar_hierarchy_day(date_update::date, 3, 2);