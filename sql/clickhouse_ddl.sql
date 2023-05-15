-- table dwh.currencies
CREATE TABLE dwh.currencies
(
	currency_code Int32 ,
	currency_code_with Int32,  
	date_update Date,  
	currency_with_div Float32,  
)
engine = MergeTree
ORDER BY toDate(date_update)
PARTITION BY toDate(date_update);

-- table dwh.transactions
CREATE TABLE dwh.transactions
(
    operation_id UUID,
	account_number_from Int32 ,
	account_number_to Int32,  
	currency_code Int32,  
	country String,  
	status String,  
	transaction_type String NOT NULL,  
	amount Int32 NOT NULL,  
	transaction_dt DateTime64(3, 'UTC'), 
)
engine = MergeTree
ORDER BY toDate(date_update)
PARTITION BY toDate(transaction_dt);

-- dwh.global_metrics
CREATE TABLE dwh.global_metrics
(
	date_update Date,
	currency_from Int32,
	amount_total Int32,
	cnt_transactions Int32,
	avg_transactions_per_account Float32,
	cnt_accounts_make_transactions Int32
)
engine = MergeTree
ORDER BY toDate(date_update)
PARTITION BY toDate(date_update);

-- dwh.currencies_names
CREATE TABLE IF NOT EXISTS dwh.currencies_names ENGINE = MergeTree
ORDER BY currency_code AS
SELECT DISTINCT country,
                currency_code
FROM dwh.transactions;