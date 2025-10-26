<!-- 87ad5424-0cae-44f0-a87c-3c805c150375 62ddbf6d-4602-4c60-b29e-00b412959a0e -->
# План разработки supplies_to_warehouses

## Предподготовка (анализ) ✅ ВЫПОЛНЕНО

### Результаты анализа реальных данных:

#### 1. API `/api/v1/supplier/incomes` - структура ответа:

**Формат ответа:** Массив объектов (не объект с полем data!)

**Пример записи:**
```json
{
  "incomeId": 33604958,
  "number": "",  // может быть пустой строкой
  "date": "2025-10-21T00:00:00",
  "lastChangeDate": "2025-10-23T12:10:34",
  "supplierArticle": "album_big_hearts",
  "techSize": "0",  // строка, не число!
  "barcode": "2046540126600",
  "quantity": 20,
  "totalPrice": 0,
  "dateClose": "2025-10-23T00:00:00",
  "warehouseName": "Электросталь",
  "nmId": 558118822,
  "status": "Принято"
}
```

**Ключевые особенности:**
- Ответ - это **массив объектов** напрямую (list[dict])
- `number` может быть пустой строкой ""
- `techSize` - **строка**, а не число
- `totalPrice` может быть 0
- Одна поставка (incomeId) может содержать несколько товаров (nmId)
- Уникальность по паре: (incomeId, nmId)

#### 2. Curl `supplyDetails` - структура ответа:

**Формат ответа:** JSON-RPC 2.0

**Структура:**
```json
{
  "id": "json-rpc_28",
  "jsonrpc": "2.0",
  "result": {
    "supply": {
      "supplyId": 33604958,
      "deliveryAndStorage": {
        "deliveryAndStorageExpr": "170",  // строка!
        "storageVolumeCut": "1",
        "deliveryValueBase": "78,2",
        "storageValue": "0,14"
      },
      "volume": 1191,
      "acceptanceCost": 0
    }
  }
}
```

**Ключевые особенности:**
- Путь к нужному полю: `result.supply.deliveryAndStorage.deliveryAndStorageExpr`
- Значение приходит как **строка** ("170"), нужно конвертировать в NUMERIC для БД
- Один запрос возвращает данные для одного incomeId (supplyId)

#### 3. Проверка credentials ✅

AUTHORIZEV3_TOKEN и COOKIES для пользователя NOSOV **совпадают** с curl-запросом из спецификации. Можно использовать `api_keys.USERS["NOSOV"]["DISCOUNTS"]`.

---

## Структура реализации

### 1. Создание миграции БД

**Файл:** `supabase/migrations/20251026_create_supplies_to_warehouses.sql`

Создать таблицу с полями:

- `id` (UUID, auto-generated, PRIMARY KEY)
- `product_id` (UUID, FK → products.id)
- `income_id` (INTEGER, NOT NULL) - из API
- `nm_id` (BIGINT, NOT NULL)
- `supplier_article` (TEXT, NOT NULL)
- `barcode` (TEXT, NOT NULL)
- `tech_size` (TEXT) - строка!
- `quantity` (INTEGER, NOT NULL)
- `warehouse_name` (TEXT, NOT NULL)
- `delivery_and_storage_expr` (NUMERIC) - из curl, конвертируем из строки
- `date` (TIMESTAMPTZ, NOT NULL)
- `last_change_date` (TIMESTAMPTZ, NOT NULL)
- `number` (TEXT) - номер УПД, может быть пустым
- `created_at` (TIMESTAMPTZ, DEFAULT NOW())
- `updated_at` (TIMESTAMPTZ, DEFAULT NOW())

**Уникальность:** `(income_id, nm_id)` - одна поставка может содержать несколько товаров

**Индексы:** `income_id`, `nm_id`, `product_id`

**Триггер:** Автообновление `updated_at`

### 2. API-клиент для /incomes

**Файл:** `wb_api/supplies_to_wh/incomes_api.py`

Реализовать:

- Функцию `fetch_incomes(date_from: str = None)` с параметром `dateFrom` (RFC3339)
- **По умолчанию:** последний месяц от текущей даты
- **Настройка:** Можно передать любую дату для загрузки с начала (например, "2020-01-01T00:00:00")
- **Пагинация:** Если получено 100000 записей, автоматически запрашивать следующую страницу с `lastChangeDate` последней строки
- **Важно:** Ответ приходит как массив напрямую, не `response['data']`
- Учесть лимит 1 запрос/минуту (пауза между запросами пагинации)
- Логирование с маской API-ключа (первые 10 символов)
- При standalone запуске сохранять JSON в `wb_api/supplies_to_wh/`
- Обработку кодов ошибок (400, 401, 429, 500+)

### 3. Валидатор структуры для /incomes

**Файл:** `excel_actions/utils/schemas/incomes.schema.json`
**Файл:** `excel_actions/supplies_to_warehouses_ea/structure_validator.py`

Создать JSON-схему для валидации:

**Обязательные поля:**
- `incomeId` (integer)
- `date` (string)
- `lastChangeDate` (string)
- `supplierArticle` (string)
- `barcode` (string)
- `quantity` (integer)
- `warehouseName` (string)
- `nmId` (integer)
- `status` (string) - просто проверяем что это string
- `number` (string) - может быть пустым
- `techSize` (string)

Валидатор использует `schema_utils.py` аналогично CR Daily Stats

### 4. Curl-клиент для supplyDetails

**Файл:** `wb_api/supplies_to_wh/supply_details_curl.py`

Реализовать:

- Функцию `fetch_supply_details(supply_id: int)` для запроса деталей поставки
- Использовать `api_keys.USERS[ACTIVE_USER]["DISCOUNTS"]`:
  - `AUTHORIZEV3_TOKEN` (header: `authorizev3`)
  - `COOKIES` (header: `Cookie`)
  - `USER_AGENT`
- **Минимальная задержка между запросами: 0.5 секунды**
- Логирование с маской:
  - Ключ: первые 20 символов + "..."
  - Cookies: первые 50 символов + "..."
- При standalone запуске сохранять JSON в `wb_api/supplies_to_wh/`
- Обработку ошибок (403, 429, 500+)
- **Важно:** Ответ в формате JSON-RPC 2.0

### 5. Валидатор для supplyDetails

**Файл:** `excel_actions/utils/schemas/supply_details.schema.json`
**Файл:** `excel_actions/supplies_to_warehouses_ea/supply_details_validator.py`

Валидировать структуру JSON-RPC и поле:

- Путь: `result.supply.deliveryAndStorage.deliveryAndStorageExpr`
- Тип: string (не number!)
- Может быть null

### 6. Трансформер данных

**Файл:** `excel_actions/supplies_to_warehouses_ea/transform.py`

Функции:

1. `filter_accepted_supplies(incomes: list) -> list`
   - Фильтрует только записи со статусом "Принято" (после валидации)

2. `extract_delivery_expr(supply_details: dict) -> str | None`
   - Извлекает значение из `result.supply.deliveryAndStorage.deliveryAndStorageExpr`
   - Возвращает строку или None

3. `convert_delivery_expr_to_numeric(value: str) -> Decimal | None`
   - Конвертирует строку "170" в Decimal для БД

4. `prepare_for_db(incomes: list) -> list[dict]`
   - Преобразует даты в datetime
   - Подготавливает поля для upsert

### 7. Обогащение и запись в БД

**Файл:** `excel_actions/supplies_to_warehouses_ea/supabase_writer.py`

Функции:

1. `enrich_with_product_ids(records: list, supabase: Client) -> list`
   - Обогащает записи `product_id` из таблицы `products` по `nm_id`
   - Логирует записи без `product_id` (пропускаем их)

2. `get_existing_supplies(supabase: Client) -> dict`
   - Получает существующие поставки из БД
   - Возвращает: `{(income_id, nm_id): {'last_change_date': ..., 'has_delivery_expr': bool}}`

3. `get_incomes_without_delivery_expr(supabase: Client) -> set`
   - Получает список income_id, у которых delivery_and_storage_expr = NULL
   - Возвращает: `set[int]` - income_id для повторного запроса

4. `upsert_records(records: list, supabase: Client) -> tuple[int, int]`
   - Выполняет upsert с `on_conflict=(income_id, nm_id)`
   - Возвращает: (количество новых, количество обновленных)

### 8. Оркестратор (main function)

**Файл:** `main_function/supplies_to_warehouses_mf/supplies_to_warehouses_supabase.py`

**Основная логика:**

```python
def main():
    # 1. Подключение к Supabase
    supabase = get_supabase_client()
    
    # 2. Получить существующие поставки
    existing = get_existing_supplies(supabase)
    
    # 3. Получить income_id без delivery_and_storage_expr
    incomes_without_delivery = get_incomes_without_delivery_expr(supabase)
    
    # 4. Определить date_from
    # 🔧 НАСТРОЙКА: измени здесь дату для загрузки всех данных
    # По умолчанию: последний месяц
    # Для всех данных: date_from = "2020-01-01T00:00:00"
    date_from = None  # None = последний месяц
    
    # 5. Загрузить данные из API /incomes (с пагинацией)
    incomes = fetch_incomes(date_from)  # возвращает list[dict]
    validate_incomes_structure(incomes)
    
    # 6. Фильтр: только status="Принято"
    accepted = filter_accepted_supplies(incomes)
    
    # 7. Определить income_id для запроса delivery_expr:
    #    - новые поставки
    #    - существующие с изменениями
    #    - существующие без delivery_expr
    income_ids_to_fetch = set()
    
    for record in accepted:
        income_id = record['incomeId']
        key = (income_id, record['nmId'])
        
        # Новая поставка или изменилась
        if key not in existing:
            income_ids_to_fetch.add(income_id)
        elif existing[key]['last_change_date'] != record['lastChangeDate']:
            income_ids_to_fetch.add(income_id)
    
    # Добавляем income_id без delivery_expr
    income_ids_to_fetch.update(incomes_without_delivery)
    
    # 8. Загрузить deliveryAndStorageExpr
    delivery_data = {}
    for income_id in income_ids_to_fetch:
        try:
            details = fetch_supply_details(income_id)
            validate_supply_details_structure(details)
            expr = extract_delivery_expr(details)
            delivery_data[income_id] = expr
        except Exception as e:
            print(f"❌ Ошибка загрузки delivery_expr для {income_id}: {e}")
            delivery_data[income_id] = None
        time.sleep(0.5)  # задержка
    
    # 9. Добавить delivery_expr к записям
    for record in accepted:
        income_id = record['incomeId']
        if income_id in delivery_data:
            record['delivery_and_storage_expr'] = delivery_data[income_id]
    
    # 10. Трансформация
    records = prepare_for_db(accepted)
    
    # 11. Обогащение product_id
    enriched = enrich_with_product_ids(records, supabase)
    
    # 12. Upsert в БД
    new_count, updated_count = upsert_records(enriched, supabase)
    
    # 13. Логирование итогов
    print(f"✅ Новых поставок: {new_count}")
    print(f"🔄 Обновленных: {updated_count}")
    print(f"⚠️  Пропущено (без product_id): {len(records) - len(enriched)}")
    print(f"🔄 Обновлено delivery_expr: {len(income_ids_to_fetch)}")
```

### 9. Data validator

**Файл:** `excel_actions/supplies_to_warehouses_ea/data_validator.py`

Проверки после записи в БД:

- Количество записей в БД соответствует ожидаемому
- Все обязательные поля заполнены
- `product_id` корректно связан с `products`
- Нет дублей по уникальному ключу `(income_id, nm_id)`
- `delivery_and_storage_expr` заполнен для новых поставок

---

## Логирование

Во всех модулях использовать единый подход:

- Маскирование API-ключей: показывать только первые 10 символов + "..."
- Маскирование cookies: первые 50 символов + "..."
- Подробное логирование для каждого этапа с эмодзи (🔄, ✅, ❌, ⚠️)
- Логировать статистику: количество новых, обновленных, пропущенных записей

## Зависимости

- Таблица `products` должна содержать все `nm_id` из поставок
- Для записей без `product_id` выводить предупреждение и пропускать
- Использовать Supabase client из `api_keys.py` (LOCAL/PROD переключатель)

## Паттерны из существующего кода

Следовать структуре из `cr_daily_stats`:

- API-клиент в `wb_api/`
- Валидаторы и трансформеры в `excel_actions/`
- Главный оркестратор в `main_function/`
- Схемы в `excel_actions/utils/schemas/`
- Единый стиль обработки ошибок и логирования

## Граничные случаи

1. **Пагинация:** Если получено ровно 100000 записей, автоматически запросить следующую страницу
2. **Отсутствие product_id:** Логировать и пропускать запись
3. **Ошибка supplyDetails:** Записать в БД с `delivery_and_storage_expr` = NULL, при следующем запуске повторно запросить
4. **Записи с NULL delivery_expr:** При каждом запуске проверять БД на записи с NULL и повторно запрашивать для них delivery_expr
5. **Rate limiting:** Соблюдать задержки между запросами (1 мин для /incomes, 0.5 сек для supplyDetails)
6. **Пустые поля:** `number` может быть пустой строкой "", записываем как есть
7. **Настройка периода:** По умолчанию последний месяц, можно изменить в коде оркестратора для загрузки всех данных

---

## To-dos

- [x] Загрузить и проанализировать данные из API /incomes
- [x] Загрузить и проанализировать данные из curl supplyDetails
- [x] Проверить совпадение AUTHORIZEV3_TOKEN и COOKIES для пользователя NOSOV
- [x] Проанализировать структуру ответов и обновить план
- [ ] Создать миграцию БД для таблицы supplies_to_warehouses
- [ ] Реализовать API-клиент для /incomes с пагинацией и логированием
- [ ] Создать JSON-схему и валидатор для ответа /incomes
- [ ] Реализовать curl-клиент для supplyDetails
- [ ] Создать JSON-схему и валидатор для supplyDetails
- [ ] Реализовать трансформер данных
- [ ] Реализовать обогащение и запись в БД
- [ ] Создать главный оркестратор с полным ETL циклом
- [ ] Реализовать валидатор записанных данных в БД

