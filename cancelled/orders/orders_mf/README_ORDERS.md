# Orders - Заказы Wildberries

## Описание
Функция для загрузки и хранения заказов Wildberries в базе данных Supabase.

## Структура
- `wb_api/orders/orders.py` - API клиент для получения заказов
- `excel_actions/orders_ea/structure_validator.py` - Валидация структуры данных
- `excel_actions/orders_ea/supabase_writer.py` - Запись в БД с обогащением
- `excel_actions/utils/schemas/orders.schema.json` - Схема валидации
- `main_function/orders_mf/orders_supabase.py` - Главная функция оркестрации

## Запуск
```bash
cd "/Users/makar/Проекты Cursor/Only Wildberries"
source venv/bin/activate
PYTHONPATH="/Users/makar/Проекты Cursor/Only Wildberries:$PYTHONPATH" python main_function/orders_mf/orders_supabase.py
```

## Особенности

### Обогащение данными
1. **product_id** - из таблицы `products` по `nm_id`
   - Если не найден → запись пропускается, логируется nm_id
   
2. **delivery_and_storage_expr** - из таблицы `supplies_to_warehouses` по `income_id`
   - Если income_id не найден → запись пропускается, логируется income_id
   - Если income_id найден, но delivery_expr = NULL → устанавливается дефолт 150

### Ограничения
- По умолчанию загружаются заказы за последние 30 дней
- Лимит API: 1 запрос/минуту
- Максимум 80,000 строк на запрос

### Результат
- Записи сохраняются в таблицу `orders` в Supabase
- UPSERT по полю `srid` (уникальный ID заказа)
- Дубликаты не создаются

## База данных

Таблица: `orders`

Ключевые поля:
- `id` - UUID PK
- `srid` - Уникальный ID заказа (из API)
- `product_id` - FK на `products.id`
- `income_id` - Номер поставки
- `delivery_and_storage_expr` - Коэффициент логистики
- `date`, `last_change_date` - Даты заказа
- `nm_id`, `barcode`, `supplier_article` - Информация о товаре
- Цены: `total_price`, `discount_percent`, `spp`, `finished_price`, `price_with_disc`
