create table TIM_ALEINIKOV_YANDEX_RU__DWH.global_metrics
(
	date_update TIMESTAMP(0), -- дата расчёта,
	currency_from INT, -- код валюты транзакции;
	amount_total NUMERIC(5, 3) NULL, -- общая сумма транзакций по валюте в долларах;
	cnt_transactions NUMERIC(5, 3) NULL, -- общий объём транзакций по валюте;
	avg_transactions_per_account NUMERIC(5, 3) NULL, -- средний объём транзакций с аккаунта;
	cnt_accounts_make_transactions INT -- количество уникальных аккаунтов с совершёнными транзакциями по валюте.
)
order by date_update
segmented by hash(date_update, currency_from) all nodes
partition by COALESCE (date_update::date,'1900-01-01')