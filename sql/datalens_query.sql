SELECT date_update,
       currency_from,
       concat(country, ' | cur') currency_name,
       amount_total,
       cnt_transactions,
       avg_transactions_per_account,
       cnt_accounts_make_transactions
FROM dwh.global_metrics gm
JOIN dwh.currencies_names cn ON cn.currency_code = gm.currency_from ;