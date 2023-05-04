# Итоговый проект

## Описание
Репозиторий итогового проекта курса Data Engineer.

### Структура репозитория
Файлы в репозитории будут использоваться для проверки и обратной связи по проекту. Поэтому постарайтесь публиковать ваше решение согласно установленной структуре: так будет проще соотнести задания с решениями.

Внутри `src` расположены папки:
- `/src/dags` - вложите в эту папку код DAG, который поставляет данные из источника в хранилище. Назовите DAG `1_data_import.py`. Также разместите здесь DAG, который обновляет витрины данных. Назовите DAG `2_datamart_update.py`.
- `/src/sql` - сюда вложите SQL-запрос формирования таблиц в `STAGING`- и `DWH`-слоях, а также скрипт подготовки данных для итоговой витрины.
- `/src/py` - если источником вы выберете Kafka, то в этой папке разместите код запуска генерации и чтения данных в топик.
- `/src/img` - здесь разместите скриншот реализованного над витриной дашборда.

## Архитектура
Выбраная архитектура решения

![Project Architecture](./archi.png)

## Запуск инфраструктуры 
### Локальная
```bash
docker run -d -p 8998:8998 -p 8280:8280 -p 15432:5432 --name=de-final-prj-local sindb/de-final-prj:latest
```
Информация по размещению инструментов есть внутри образа, в частности:
- Адрес Metabase — 8998. http://localhost:8998/
- Адрес Airflow — 8280.  http://localhost:8280/airflow/ 
- Postgres

### Облачная

Установите **виртуальную машину с помощью Telegram-бота. ВМ будет генерировать сообщения в Kafka, чтобы создать подключение по SSH. IP-адрес для ВМ бот выдаст. 
При запуске инфраструктуры в веб-интерфейсах, требующих авторизации, вам будет предоставлен логин и пароль. Если вы удаляете инфраструктуру, то всё содержимое тоже удаляется: не забудьте сохранить свои наработки локально.


### Extract data from
В компании запущена PostgreSQL для продакшена. Для построения инфраструктуры аналитики и поставки данных предоставлена отдельная PostgreSQL, копия продовой БД. В таблицах public.transactions и public.currencies есть доступ на загрузку данных.
Для доступа к БД создана учётная запись:
```
"database": "db1",
"host" : "rc1b-w5d285tmxa8jimyn.mdb.yandexcloud.net",
"port" : 6432,
"username" : “[secrets]”,
"password" : "[secrets]".
```
Перед тем как подключиться к БД, скачайте [сертификат](https://storage.yandexcloud.net/cloud-certs/CA.pem).

### Load data to
Vertica
- Адрес сервера:  vertica.tgcloudenv.ru
- Креды: [secrets](./secrets/secrets.md)


## Data

### transactions
Данные `transactions` содержат в себе информацию о движении денежных средств между клиентами в разных валютах.

Структура данных:
- `operation_id` — id транзакции;
- `account_number_from` — внутренний бухгалтерский номер счёта транзакции ОТ КОГО;
- `account_number_to` — внутренний бухгалтерский номер счёта транзакции К КОМУ;
- `currency_code` — трёхзначный код валюты страны, из которой идёт транзакция;
- `country` — страна-источник транзакции;
- `status` — статус проведения транзакции: queued («транзакция в очереди на обработку сервисом»), in_progress («транзакция в обработке»), blocked («транзакция заблокирована сервисом»), done («транзакция выполнена успешно»), chargeback («пользователь осуществил возврат по транзакции»).
- `transaction_type` — тип транзакции во внутреннем учёте: authorisation («авторизационная транзакция, подтверждающая наличие счёта пользователя»), sbp_incoming («входящий перевод по системе быстрых платежей»), sbp_outgoing («исходящий перевод по системе быстрых платежей»), transfer_incoming («входящий перевод по счёту»), transfer_outgoing («исходящий перевод по счёту»), c2b_partner_incoming («перевод от юридического лица»), c2b_partner_outgoing («перевод юридическому лицу»).
- `amount` — целочисленная сумма транзакции в минимальной единице валюты страны (копейка, цент, куруш);
- `transaction_dt` — дата и время исполнения транзакции до миллисекунд.

### сurrencies
Данные `сurrencies` — это справочник, который содержит в себе информацию об обновлениях курсов валют и взаимоотношениях валютных пар друг с другом.

Структура данных:
- `date_update` — дата обновления курса валют;
- `currency_code` — трёхзначный код валюты транзакции;
- `currency_code_with` — отношение другой валюты к валюте трёхзначного кода;
- `currency_code_div` — значение отношения единицы одной валюты к единице валюты транзакции.

