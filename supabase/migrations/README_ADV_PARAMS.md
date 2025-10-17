# Документация: Агрегация рекламной статистики (adv_params)

## 📊 Структура таблиц

### 1. `adv_campaign_daily_stats` (детальная статистика)
Каждая строка = один артикул в одной рекламной кампании за один день.

**Ключевые поля:**
- `advert_id` - ID рекламной кампании WB
- `nm_id` - Артикул WB
- `vendor_code` - Артикул продавца
- `date` - Дата показателей
- `views`, `clicks`, `sum`, `orders` - Метрики

**Уникальность:** `(advert_id, nm_id, date)`

---

### 2. `adv_params` (агрегированная статистика)
Каждая строка = один артикул за один день (суммируем ВСЕ кампании).

**Ключевые поля:**
- `nm_id` - Артикул WB
- `vendor_code` - Артикул продавца
- `date` - Дата показателей
- `views`, `clicks`, `sum`, `orders` - Агрегированные метрики
- **`updated_at`** - Время последнего обновления данных

**Уникальность:** `(nm_id, date)`

---

## ⚙️ Как работает агрегация

### Функция `aggregate_adv_params(date_from, date_to)`

1. **Фильтрация:** Берет записи из `adv_campaign_daily_stats` за указанный период
2. **Агрегация:** Группирует по `(nm_id, date)` и суммирует метрики
3. **UPSERT:** Вставляет в `adv_params` или обновляет существующие записи

### Пример SQL запроса

```sql
SELECT
  nm_id,
  vendor_code,
  date,
  SUM(views) AS views,
  SUM(clicks) AS clicks,
  SUM(sum) AS sum,
  ...
FROM adv_campaign_daily_stats
WHERE date >= '2025-10-09' AND date <= '2025-10-11'  -- Фильтр по датам
GROUP BY nm_id, vendor_code, date
```

### Результат агрегации

Если артикул участвует в нескольких кампаниях:

```
adv_campaign_daily_stats:
  campaign_1 | nm_id=123 | date=2025-10-09 | views=1000
  campaign_2 | nm_id=123 | date=2025-10-09 | views=500

↓ Агрегация

adv_params:
  nm_id=123 | date=2025-10-09 | views=1500  (сумма)
```

---

## 🔧 Решенная проблема: updated_at

### Проблема (ДО исправления)

При вызове `aggregate_adv_params('2025-10-09', '2025-10-11')`:
- Функция брала **ВСЕ** записи за этот период (от всех ИП: KUSKOV, NOSOV и т.д.)
- Делала UPSERT в `adv_params`
- **Триггер** автоматически обновлял `updated_at = NOW()` для **ВСЕХ** записей

**Результат:** Если KUSKOV запускает функцию, `updated_at` обновляется и для артикулов NOSOV! 😱

### Решение (ПОСЛЕ исправления)

1. **Удален триггер** `update_adv_params_updated_at` для таблицы `adv_params`
2. **Добавлена логика** в `ON CONFLICT`:

```sql
updated_at = CASE 
  WHEN (
    adv_params.views IS DISTINCT FROM EXCLUDED.views OR
    adv_params.clicks IS DISTINCT FROM EXCLUDED.clicks OR
    adv_params.sum IS DISTINCT FROM EXCLUDED.sum OR
    adv_params.orders IS DISTINCT FROM EXCLUDED.orders OR
    adv_params.orders_sum IS DISTINCT FROM EXCLUDED.orders_sum
  )
  THEN NOW()  -- Обновляем ТОЛЬКО если данные изменились
  ELSE adv_params.updated_at  -- Оставляем старое значение
END
```

### Результат

✅ `updated_at` обновляется **ТОЛЬКО** при реальном изменении данных  
✅ Повторный запуск с теми же данными → `updated_at` не меняется  
✅ Артикулы разных ИП не влияют друг на друга по `updated_at`

---

## 🎯 Примеры использования

### Запуск агрегации за период

```python
from supabase import create_client
import api_keys

supabase = create_client(api_keys.SUPABASE_URL, api_keys.SUPABASE_KEY)

# Агрегация за 3 дня
response = supabase.rpc(
    'aggregate_adv_params',
    {
        'p_date_from': '2025-10-09',
        'p_date_to': '2025-10-11'
    }
).execute()

print(f"Обработано записей: {response.data}")
```

### Запуск через main функцию

```bash
cd "/Users/makar/Проекты Cursor/Only Wildberries"
source venv/bin/activate
python3 main_function/adv_params_mf/adv_params_supabase.py --begin 2025-10-09 --end 2025-10-11
```

---

## 📚 Дополнительная информация

### Связь таблиц

```
products (nm_id, vendor_code)
    ↓ FK
adv_params (nm_id, date, updated_at)
    ↑ агрегация
adv_campaign_daily_stats (advert_id, nm_id, date)
```

### Триггеры

- ✅ `update_adv_campaign_daily_stats_updated_at` - активен (для детальной таблицы)
- ❌ `update_adv_params_updated_at` - **отключен** (управляется в RPC функции)

### Индексы

- `idx_adv_params_nm_id` - поиск по артикулу
- `idx_adv_params_date` - поиск по дате
- `ux_adv_params_nm_date` - уникальность (nm_id, date)

---

## 🚨 Важные замечания

1. **Не агрегирует всю таблицу целиком** - только указанный период (date_from → date_to)
2. **Безопасно запускать повторно** - updated_at не изменится, если данные те же
3. **Нет разделения по ИП** - если KUSKOV и NOSOV продают один nm_id, данные будут в одной записи
4. **updated_at показывает реальное изменение** - можно отслеживать, когда данные последний раз менялись

---

## 📝 История изменений

**2025-10-11** - Исправлена проблема с updated_at:
- Удален триггер для adv_params
- Добавлена условная логика в aggregate_adv_params
- updated_at теперь обновляется только при реальных изменениях


