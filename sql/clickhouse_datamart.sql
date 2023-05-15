INSERT INTO dwh.global_metrics
(date_update, currency_from, amount_total, cnt_transactions, avg_transactions_per_account, cnt_accounts_make_transactions, status)
WITH c AS
  (SELECT currency_code,
          date_update,
          currency_with_div
   FROM dwh.currencies
   WHERE currency_code_with = 420),
     t AS
  (SELECT toDate(transaction_dt) date_update,
          currency_code,
          status,
          account_number_from,
          sum(amount) amount_total,
          COUNT(1) cnt_usr_transactions
   FROM dwh.transactions
   WHERE account_number_from > 0
     AND account_number_to > 0
     AND toDate(transaction_dt) = toDate(%(business_dt)s) --business_dt
   GROUP BY date_update,
            currency_code,
            status,
            account_number_from)
SELECT t.date_update,
       t.currency_code,
       ROUND(sum(amount_total *
	       CASE
		       WHEN currency_with_div > 0 THEN currency_with_div
		       ELSE 1
	       END)) amount_total,
       sum(cnt_usr_transactions) cnt_transactions,
       ROUND(avg(cnt_usr_transactions)) avg_transactions_per_account,
       count(DISTINCT account_number_from) cnt_accounts_make_transactions,
       t.status
FROM c
RIGHT JOIN t ON t.date_update = c.date_update
AND t.currency_code = c.currency_code
GROUP BY t.date_update,
         t.currency_code,
         t.status
ORDER BY t.date_update,
         t.currency_code ;