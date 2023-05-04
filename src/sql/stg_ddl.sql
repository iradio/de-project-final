-- TIM_ALEINIKOV_YANDEX_RU__STAGING.transactions definition

-- Drop table

-- DROP TABLE TIM_ALEINIKOV_YANDEX_RU__STAGING.transactions;

CREATE TABLE TIM_ALEINIKOV_YANDEX_RU__STAGING.transactions (
	operation_id varchar(60) NULL,
	account_number_from int NULL,
	account_number_to int NULL,
	currency_code int NULL,
	country varchar(30) NULL,
	status varchar(30) NULL,
	transaction_type varchar(30) NULL,
	amount int NULL,
	transaction_dt TIMESTAMP(0) NULL
)
order by transaction_dt
SEGMENTED BY hash(operation_id,transaction_dt) all nodes
PARTITION BY COALESCE(transaction_dt::date,'1900-01-01');

-- TIM_ALEINIKOV_YANDEX_RU__STAGING.currencies definition

-- Drop table

-- DROP TABLE TIM_ALEINIKOV_YANDEX_RU__STAGING.currencies;

CREATE TABLE TIM_ALEINIKOV_YANDEX_RU__STAGING.currencies (
	date_update TIMESTAMP(0) NULL,
	currency_code int NULL,
	currency_code_with int NULL,
	currency_with_div NUMERIC(5, 3) NULL
)
order by date_update
SEGMENTED BY hash(currency_code,currency_code_with) all nodes;
