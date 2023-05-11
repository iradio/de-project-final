drop table if exists TIM_ALEINIKOV_YANDEX_RU__DWH.global_metrics;

create table if not exists TIM_ALEINIKOV_YANDEX_RU__DWH.global_metrics
(
	date_update TIMESTAMP(0) NOT NULL, -- дата расчёта,
	currency_from INT NOT NULL, -- код валюты транзакции;
	amount_total NUMERIC(18, 2) NOT NULL, -- общая сумма транзакций по валюте в долларах;
	cnt_transactions INT NOT NULL, -- общий объём транзакций по валюте;
	cnt_accounts_make_transactions INT, -- количество уникальных аккаунтов с совершёнными транзакциями по валюте.	
	avg_transactions_per_account NUMERIC(16, 2) NULL, -- средний объём транзакций с аккаунта;
	CONSTRAINT global_metrics_pk PRIMARY KEY (date_update, currency_from) ENABLED
)
ORDER BY date_update, currency_from
SEGMENTED BY hash(currency_from) ALL NODES
KSAFE 1
PARTITION BY EXTRACT(DOY from date_update);
