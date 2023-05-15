INSERT INTO SANYATTSUYANDEXRU__DWH.global_metrics
(date_update, currency_from, amount_total, cnt_transactions, avg_transactions_per_account, cnt_accounts_make_transactions)
WITH c AS
  (SELECT currency_code,
          date_update,
          currency_with_div
   FROM SANYATTSUYANDEXRU__STAGING.currencies
   WHERE currency_code_with = 420 ),
     t AS
  (SELECT DATE_TRUNC('DAY', transaction_dt) date_update,
          currency_code,
          account_number_from,
          SUM(amount) amount_total,
          COUNT(1) cnt_usr_transactions
   FROM SANYATTSUYANDEXRU__STAGING.transactions
   WHERE account_number_from > 0
     AND account_number_to > 0
     AND DATE_TRUNC('DAY', transaction_dt) = '2022-10-01'::date --test date
   GROUP BY DATE_TRUNC('DAY', transaction_dt),
            currency_code,
            account_number_from)
SELECT t.date_update,
       t.currency_code,
       ROUND(sum(amount_total * COALESCE(currency_with_div, 1))) amount_total,
       SUM(cnt_usr_transactions) cnt_transactions,
       ROUND(avg(cnt_usr_transactions)) avg_transactions_per_account,
       COUNT(DISTINCT account_number_from) cnt_accounts_make_transactions
FROM c
RIGHT JOIN t ON t.date_update = c.date_update
AND t.currency_code = c.currency_code
GROUP BY t.date_update,
         t.currency_code
ORDER BY t.date_update,
         t.currency_code ;