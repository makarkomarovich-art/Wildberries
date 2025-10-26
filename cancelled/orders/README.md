# Orders - Заказы Wildberries (CANCELLED)

## Статус
✅ Функция полностью реализована, перенесена в cancelled

## Что было создано

### 1. API клиент
- `wb_api/orders/orders.py` → `orders/orders.py`
- Получение заказов из WB API `/api/v1/supplier/orders`
- Пагинация по `lastChangeDate`
- Лимиты: 1 запрос/минуту

### 2. Валидатор структуры
- `excel_actions/orders_ea/structure_validator.py` → `orders_ea/structure_validator.py`
- Проверка структуры данных перед записью
- Схема: `orders.schema.json`

### 3. Writer в Supabase
- `excel_actions/orders_ea/supabase_writer.py` → `orders_ea/supabase_writer.py`
- Обогащение данными (product_id, delivery_and_storage_expr)
- UPSERT в таблицу orders

### 4. Валидатор данных
- `excel_actions/orders_ea/data_validator.py` → `orders_ea/data_validator.py`
- Проверка данных после записи в БД

### 5. Главная функция
- `main_function/orders_mf/orders_supabase.py` → `orders_mf/orders_supabase.py`
- Оркестрация всего процесса

### 6. Схема БД
- `supabase/migrations/20251026_create_orders_table.sql`
- Таблица orders с FK на products и supplies_to_warehouses

### 7. JSON схема
- `excel_actions/utils/schemas/orders.schema.json` → `orders.schema.json`
- Структура данных для валидации

## Особенности реализации

### Обогащение данными
1. **product_id** - из таблицы `products` по `nm_id`
   - Если не найден → запись пропускается
   
2. **delivery_and_storage_expr** - из таблицы `supplies_to_warehouses` по `income_id`
   - Если income_id не найден → запись пропускается
   - Если delivery_expr NULL → устанавливается 150

### Логика обработки
- Записи с `income_id = 0` пропускаются (заказ еще не собран WB)
- При следующем обновлении, когда появится реальный income_id, заказ добавится в БД
- UPSERT по `srid` - дубликаты не создаются

## Результаты тестирования
- Получено: 1619 заказов
- Обогащено по product_id: 539
- Обогащено по delivery_expr: 513
- Записано в БД: 765 (включая предыдущие запуски)

## Причина переноса в cancelled
Функция готова и работает, но по запросу пользователя перенесена в cancelled.
