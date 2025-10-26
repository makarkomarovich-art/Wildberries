# Supplies to Warehouses ETL Pipeline

## 📋 Обзор

ETL pipeline для загрузки данных о принятых поставках товаров на склады Wildberries. Основная цель - расчет логистического коэффициента (`deliveryAndStorageExpr`) для каждой поставки и мониторинг целостности маркетплейса.

## 🎯 Основные задачи

1. **Загрузка данных поставок** из API `/api/v1/supplier/incomes`
2. **Получение логистических коэффициентов** через curl API `supplyDetails`
3. **Обогащение данных** связями с таблицей `products`
4. **Fallback логика** для поставок без коэффициентов
5. **Хранение в БД** с возможностью обновления

## 🏗️ Архитектура

### Компоненты системы

```
main_function/supplies_to_warehouses_mf/
├── supplies_to_warehouses_supabase.py    # Главный оркестратор
└── README_SUPPLIES_TO_WAREHOUSES.md      # Документация

wb_api/supplies_to_wh/
├── incomes_api.py                         # API-клиент для /incomes
└── supply_details_curl.py                # Curl-клиент для supplyDetails

excel_actions/supplies_to_warehouses_ea/
├── structure_validator.py                 # Валидатор /incomes
├── supply_details_validator.py           # Валидатор supplyDetails
├── transform.py                           # Трансформация данных
└── supabase_writer.py                    # Запись в БД

excel_actions/utils/schemas/
├── incomes.schema.json                    # JSON-схема /incomes
└── supply_details.schema.json            # JSON-схема supplyDetails

supabase/migrations/
└── 20251026_create_supplies_to_warehouses.sql  # Миграция БД
```

## 📊 Структура данных

### Таблица `supplies_to_warehouses`

| Поле | Тип | Описание |
|------|-----|----------|
| `id` | UUID | Первичный ключ |
| `product_id` | UUID | FK к `products.id` |
| `income_id` | INTEGER | ID поставки от WB |
| `nm_id` | BIGINT | Артикул товара |
| `supplier_article` | TEXT | Артикул поставщика |
| `barcode` | TEXT | Штрихкод |
| `tech_size` | TEXT | Технический размер |
| `quantity` | INTEGER | Количество |
| `warehouse_name` | TEXT | Название склада |
| `delivery_and_storage_expr` | NUMERIC | Коэффициент логистики |
| `date` | TIMESTAMPTZ | Дата поставки |
| `last_change_date` | TIMESTAMPTZ | Дата последнего изменения |
| `number` | TEXT | Номер поставки |
| `created_at` | TIMESTAMPTZ | Дата создания записи |
| `updated_at` | TIMESTAMPTZ | Дата обновления записи |

### Индексы

- **Уникальный**: `(income_id, nm_id)` - предотвращает дубликаты
- **Обычные**: `income_id`, `nm_id`, `product_id` - для быстрого поиска

## 🔄 Логика работы ETL

### 1. Инициализация и подключение

```python
# Подключение к Supabase
supabase = get_supabase_client()
```

### 2. Анализ существующих данных

```python
# Загрузка существующих поставок
existing = get_existing_supplies(supabase)

# Поиск поставок без delivery_expr
incomes_without_delivery = get_incomes_without_delivery_expr(supabase)
```

### 3. Определение периода загрузки

```python
# Настройка даты (строка 75)
date_from = None  # None = последний месяц
# Для всех данных: date_from = "2020-01-01T00:00:00"
```

### 4. Загрузка данных из API /incomes

**Особенности:**
- **Rate limit**: 1 запрос в минуту
- **Пагинация**: автоматическая по `lastChangeDate`
- **Логирование**: API ключ с маской

```python
# Запрос с пагинацией
incomes_data = fetch_incomes(date_from)
```

### 5. Валидация структуры данных

```python
# Проверка структуры ответа
validate_incomes_structure(incomes_data)
```

**Проверяемые поля:**
- `incomeId`, `date`, `lastChangeDate` - обязательные
- `supplierArticle`, `barcode`, `quantity` - обязательные
- `warehouseName`, `nmId`, `status` - обязательные
- `number`, `techSize` - обязательные (тип не проверяется)

### 6. Фильтрация по статусу

```python
# Только поставки со статусом "Принято"
accepted = filter_accepted_supplies(incomes_data)
```

### 7. Трансформация данных

```python
# Подготовка для БД
records = prepare_for_db(accepted)
```

**Преобразования:**
- `incomeId` → `income_id`
- `nmId` → `nm_id`
- `supplierArticle` → `supplier_article`
- `date` → ISO формат
- `lastChangeDate` → ISO формат

### 8. Обогащение product_id

```python
# Получение product_id из таблицы products
enriched_records = enrich_with_product_ids(records, supabase)
```

**Логика:**
- Загружает ВСЕ products из БД
- Создает словарь `nm_id → product_id`
- Фильтрует записи без `product_id`

### 9. Определение поставок для загрузки delivery_expr

```python
# Только для обогащенных записей
income_ids_to_fetch = set()
for record in enriched_records:
    # Новая поставка или изменилась
    if key not in existing or existing[key]['last_change_date'] != record['last_change_date']:
        income_ids_to_fetch.add(income_id)
```

### 10. Загрузка deliveryAndStorageExpr

**Особенности:**
- **Rate limit**: 0.5 секунды между запросами
- **Аутентификация**: `AUTHORIZEV3_TOKEN` + `COOKIES`
- **Формат**: JSON-RPC 2.0

```python
for income_id in income_ids_to_fetch:
    details = fetch_supply_details(income_id)
    validate_supply_details_structure(details)
    expr = extract_delivery_expr(details)
```

### 11. Добавление delivery_expr к записям

```python
# Привязка коэффициентов к записям
enriched_accepted = add_delivery_expr_to_records(enriched_records, delivery_data)
```

### 12. Fallback логика

**Проблема**: WB API иногда не возвращает `deliveryAndStorageExpr`

**Решение**: Поиск предыдущих поставок того же товара на том же складе

```python
def find_fallback_delivery_expr(supabase, warehouse_name, nm_id, current_date):
    # SQL запрос для поиска предыдущей поставки
    response = supabase.table("supplies_to_warehouses")\
        .select("delivery_and_storage_expr, date")\
        .eq("warehouse_name", warehouse_name)\
        .eq("nm_id", nm_id)\
        .not_.is_("delivery_and_storage_expr", "null")\
        .lt("date", current_date)\
        .order("date", desc=True)\
        .limit(1)\
        .execute()
```

#### ⚠️ **Важная особенность Fallback логики**

**Проблема доприемки WB:**
- WB часто допринимает товары через **2-3 дня** после основной поставки
- При первом запуске основная поставка еще **не загружена** в БД
- Fallback может найти **более ранние поставки** (некорректно)
- При повторном запуске уже есть **правильная поставка** для fallback

**Сценарии работы:**

1. **✅ Идеальный случай**: Есть предыдущая поставка того же товара на том же складе
2. **⚠️ Проблемный случай**: Нет предыдущих поставок → fallback не сработает → остается NULL
3. **🔄 Многоэтапный случай**: Нужно **минимум 2 запуска** для полного заполнения

**Рекомендации:**
- **Первый запуск**: загружает основные поставки, некоторые остаются с NULL
- **Повторный запуск**: допринятые товары получают правильный fallback
- **NULL значения**: могут остаться только при полном отсутствии предыдущих поставок

### 13. Запись в Supabase

```python
# Upsert записей
new_count, updated_count = upsert_records(enriched_accepted, supabase)
```

**Логика upsert:**
- **Новые записи**: INSERT
- **Существующие**: UPDATE если изменился `last_change_date`
- **Уникальность**: по `(income_id, nm_id)`

### 14. Валидация записанных данных

```python
# Проверка корректности записи
validate_inserted_data(enriched_accepted, supabase)
```

## 🔧 Настройки

### Период загрузки

```python
# В файле supplies_to_warehouses_supabase.py, строка 75
date_from = None  # Последний месяц (по умолчанию)
# date_from = "2020-01-01T00:00:00"  # Все данные
```

### Rate Limiting

```python
# /incomes API
time.sleep(60)  # 1 запрос в минуту

# supplyDetails API  
time.sleep(0.5)  # 0.5 секунды между запросами
```

### Аутентификация

```python
# Из api_keys.py для пользователя "NOSOV"
WB_API_TOKEN = "..."           # Для /incomes
AUTHORIZEV3_TOKEN = "..."      # Для supplyDetails
COOKIES = "..."                # Для supplyDetails
```

## 📈 Мониторинг и логирование

### Логирование операций

- ✅ **API запросы**: статус, количество записей, маскированные ключи
- ✅ **Валидация**: результаты проверки структуры
- ✅ **Фильтрация**: статистика по статусам
- ✅ **Обогащение**: количество найденных products
- ✅ **Fallback**: применение резервных значений
- ✅ **Запись в БД**: количество новых/обновленных записей

### Примеры логов

```
📡 Статус ответа: 200
✅ Получено записей: 84
📊 Статистика статусов: {'Принято': 84, 'Приемка': 6}
✅ Обогащено записей: 83
📦 Найден fallback delivery_expr: 160 (от 2025-08-12T00:00:00+00:00)
✅ Записано записей в БД: 83
```

## 🚀 Запуск

```bash
cd "/Users/makar/Проекты Cursor/Only Wildberries"
python3 main_function/supplies_to_warehouses_mf/supplies_to_warehouses_supabase.py
```

### ⚠️ **Важно: Многоэтапный запуск**

**Для нового заполнения таблицы рекомендуется:**

1. **Первый запуск**: загружает основные поставки
2. **Ожидание 2-3 дня**: для доприемки WB
3. **Повторный запуск**: заполняет fallback для допринятых товаров

**Проверка полноты данных:**
```sql
-- Количество записей без delivery_expr
SELECT COUNT(*) FROM supplies_to_warehouses WHERE delivery_and_storage_expr IS NULL;
```

## 🔍 Отладка

### Проверка данных в БД

```sql
-- Общее количество записей
SELECT COUNT(*) FROM supplies_to_warehouses;

-- Записи без delivery_expr
SELECT COUNT(*) FROM supplies_to_warehouses WHERE delivery_and_storage_expr IS NULL;

-- Статистика по складам
SELECT warehouse_name, COUNT(*) as records_count, SUM(quantity) as total_quantity 
FROM supplies_to_warehouses 
GROUP BY warehouse_name;

-- Средний коэффициент логистики
SELECT AVG(delivery_and_storage_expr) FROM supplies_to_warehouses;
```

### Пересоздание таблицы

```bash
# Удаление таблицы
psql postgresql://postgres:postgres@127.0.0.1:54322/postgres -c "DROP TABLE IF EXISTS supplies_to_warehouses CASCADE;"

# Создание заново
psql postgresql://postgres:postgres@127.0.0.1:54322/postgres -f supabase/migrations/20251026_create_supplies_to_warehouses.sql
```

## ⚠️ Ограничения и особенности

### Ограничения API

- **Rate limiting**: соблюдение задержек между запросами
- **Пагинация**: максимум 100,000 записей за запрос
- **Аутентификация**: токены имеют срок действия

### Особенности данных

- **Статус "Принято"**: только такие поставки сохраняются
- **Обязательные поля**: `nm_id` должен существовать в `products`
- **Fallback**: используется только для записей без `delivery_expr`
- **Доприемка WB**: товары допринимаются через 2-3 дня после основной поставки
- **Многоэтапность**: требуется минимум 2 запуска для полного заполнения fallback

### Производительность

- **Обогащение**: загружает ВСЕ products (оптимизация возможна)
- **Fallback**: SQL запросы для каждой записи без `delivery_expr`
- **Upsert**: пакетная обработка записей

## 🔮 Возможные улучшения

1. **Кэширование products**: избежать повторной загрузки
2. **Батчинг fallback**: группировка SQL запросов
3. **Параллельная обработка**: для больших объемов данных
4. **Мониторинг**: алерты при ошибках API
5. **Метрики**: детальная статистика по коэффициентам

## 📞 Поддержка

При возникновении проблем проверьте:
1. Подключение к Supabase
2. Валидность API токенов
3. Наличие данных в таблице `products`
4. Логи выполнения для диагностики ошибок
