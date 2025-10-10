# База артикулов → Supabase

## Что изменилось

Скрипт `list_of_seller_articles_supabase.py` заменяет старую версию (теперь `list_of_seller_articles_LEGACY.py`), которая записывала данные в Google Sheets.

**Новый флоу:**
1. Получение карточек из WB Content API
2. Валидация структуры (обязательные поля)
3. Нормализация данных
4. Применение исключений (`excluded_nm_ids.py`)
5. **Запись в Supabase** (вместо Google Sheets)

---

## Структура данных

### Таблица `products`:
- `id` — UUID (внутренний, неизменный)
- `serial_id` — порядковый номер (для удобства)
- `nm_id` — артикул WB (уникальный)
- `imt_id` — ID склейки
- `vendor_code` — артикул продавца (уникальный)
- `title` — название товара
- `category_wb` — категория WB

### Таблица `product_sizes`:
- `id` — UUID
- `serial_id` — порядковый номер
- `product_id` — связь с `products` (UUID)
- `barcode` — баркод (уникальный)
- `size` — размер (techSize, может быть пустым)

---

## Запуск

```bash
cd "/Users/makar/Проекты Cursor/Only Wildberries"
source venv/bin/activate
python main_function/list_of_seller_articles_mf/list_of_seller_articles_supabase.py
```

---

## Исключения

Файл: `main_function/list_of_seller_articles_mf/excluded_nm_ids.py`

Добавьте `nmID` товаров, которые не нужно добавлять/обновлять в БД:

```python
EXCLUDED_NM_IDS = {
    558118821,  # Пример
    558118822,
}
```

---

## Логика upsert

### Products:
- Если товар с `nm_id` уже есть → **обновляет** (UUID не меняется)
- Если товара нет → **добавляет новый**

### Product sizes:
- Если баркод уже есть → **обновляет**
- Если баркода нет → **добавляет новый**

**UUID всегда сохраняется!**

---

## Валидация

### Обязательные поля (ОШИБКА → СТОП):
- `nmID` (int)
- `imtID` (int)
- `subjectName` (string)
- `vendorCode` (string)
- `title` (string)
- `sizes[].skus` (list, не пустой)

### Опциональные поля (WARNING):
- `sizes[].techSize` (если нет → пустая строка)

---

## Проверка данных

После запуска откройте **Supabase Studio**:

```
http://localhost:54323
```

Перейдите в **Table Editor** → увидите таблицы `products` и `product_sizes` с данными.

---

## Troubleshooting

### Ошибка: "Библиотека supabase не установлена"
```bash
pip install supabase
```

### Ошибка: "Не найден UUID для nm_id"
Товар не был добавлен в `products` (возможно, не прошёл валидацию или в исключениях).

### Ошибка подключения к Supabase
Проверьте, что:
1. Supabase запущен: `supabase status`
2. Ключи в `api_keys.py` корректны
3. Docker Desktop запущен

---

## Переход на облачный Supabase

Когда будете готовы к продакшену:

1. Создайте проект на https://supabase.com
2. Получите ключи (Dashboard → Settings → API)
3. Обновите `api_keys.py`:
```python
SUPABASE_URL = "https://xxxxx.supabase.co"
SUPABASE_KEY = "ваш_service_role_key"
```
4. Примените миграции:
```bash
supabase link --project-ref xxxxx
supabase db push
```

Готово!

