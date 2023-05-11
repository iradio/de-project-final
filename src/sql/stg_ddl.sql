-- TIM_ALEINIKOV_YANDEX_RU__STAGING.transactions definition

-- Drop table

DROP TABLE IF EXISTS TIM_ALEINIKOV_YANDEX_RU__STAGING.transactions;

CREATE TABLE IF NOT EXISTS TIM_ALEINIKOV_YANDEX_RU__STAGING.transactions (
	operation_id varchar(60) NOT NULL,
	account_number_from int NULL,
	account_number_to int NULL,
	currency_code int NULL,
	country varchar(30) NULL,
	status varchar(30) NULL,
	transaction_type varchar(30) NULL,
	amount int NULL,
	transaction_dt TIMESTAMP(0) NULL,
	CONSTRAINT transactions_pk PRIMARY KEY (operation_id, transaction_dt,status) ENABLED
)
ORDER BY transaction_dt, currency_code
SEGMENTED BY hash(currency_code) ALL NODES
KSAFE 1
PARTITION BY (EXTRACT(DOY from transaction_dt));

-- TIM_ALEINIKOV_YANDEX_RU__STAGING.currencies definition

-- Drop table

DROP TABLE IF EXISTS TIM_ALEINIKOV_YANDEX_RU__STAGING.currencies;

CREATE TABLE IF NOT EXISTS TIM_ALEINIKOV_YANDEX_RU__STAGING.currencies (
	date_update TIMESTAMP(0) NULL,
	currency_code int NULL,
	currency_code_with int NULL,
	currency_with_div NUMERIC(5, 3) NULL,
	CONSTRAINT currencies_pk PRIMARY KEY (currency_code, currency_code_with,date_update) ENABLED
)
ORDER BY date_update, currency_code
SEGMENTED BY hash(currency_code,currency_code_with) ALL NODES
KSAFE 1
PARTITION BY EXTRACT(DOY from date_update);
