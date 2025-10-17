# 📚 Основы баз данных на примере проекта

Эта справка объясняет основные концепции баз данных на реальных примерах из нашего проекта.

---

## 🏗️ Основные понятия

### Таблица (Table)
**Что это:** Структура для хранения данных, похожая на таблицу Excel.

**Пример из проекта:**
```sql
CREATE TABLE products (
  id UUID PRIMARY KEY,
  nm_id BIGINT UNIQUE NOT NULL,
  vendor_code TEXT UNIQUE NOT NULL,
  title TEXT NOT NULL
);
```

**В нашем проекте:**
- `products` - таблица артикулов (40 записей)
- `adv_campaign_daily_stats` - детальная статистика (тысячи записей)
- `adv_params` - агрегированная статистика (десятки записей)

---

### Сущность (Entity)
**Что это:** Объект реального мира, который мы храним в БД.

**Примеры:**
- 🛍️ **Товар** → таблица `products`
- 📊 **Рекламная кампания** → таблица `adv_campaign_daily_stats`
- 📈 **Статистика артикула** → таблица `adv_params`

---

### Атрибут (Attribute/Column)
**Что это:** Свойство сущности (колонка в таблице).

**Пример - артикул в `products`:**
```
nm_id        = 456770543          (артикул WB)
vendor_code  = "rykzak_black"     (наш артикул)
title        = "Рюкзак черный"    (название)
```

**Типы данных:**
- `BIGINT` - большие целые числа (nm_id)
- `TEXT` - текст любой длины (vendor_code, title)
- `DATE` - дата без времени (2025-10-09)
- `TIMESTAMPTZ` - дата и время с часовым поясом
- `NUMERIC(10,2)` - дробное число (10 цифр, 2 после запятой)
- `UUID` - уникальный идентификатор (gen_random_uuid())

---

## 🔑 Ключи (Keys)

### PRIMARY KEY (Первичный ключ)
**Что это:** Уникальный идентификатор записи. Каждая строка имеет свой уникальный PK.

**Пример:**
```sql
CREATE TABLE products (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),  -- Первичный ключ
  nm_id BIGINT,
  ...
);
```

**Визуализация:**
```
id                                    | nm_id      | vendor_code
--------------------------------------|------------|---------------
f47ac10b-58cc-4372-a567-0e02b2c3d479 | 456770543  | rykzak_black
a3c4e5f7-1234-5678-9abc-def012345678 | 467102886  | korzina_tiger
```

Каждый `id` уникален → можно точно найти любую запись.

---

### UNIQUE (Уникальность)
**Что это:** Гарантирует, что значение не повторяется в таблице.

**Пример:**
```sql
nm_id BIGINT UNIQUE NOT NULL,  -- Один nm_id может быть только раз
vendor_code TEXT UNIQUE NOT NULL,  -- Один vendor_code может быть только раз
```

**Что это значит:**
- ✅ Можно добавить: `nm_id=123456, vendor_code="rykzak_red"`
- ❌ Нельзя добавить еще одну запись с `nm_id=123456` (уже есть!)
- ❌ Нельзя добавить еще одну запись с `vendor_code="rykzak_red"` (уже есть!)

---

### FOREIGN KEY (Внешний ключ)
**Что это:** Связь между таблицами. Показывает, что значение в одной таблице ссылается на другую.

**Пример из проекта:**
```sql
CREATE TABLE adv_params (
  nm_id BIGINT NOT NULL,
  ...
  FOREIGN KEY (nm_id) REFERENCES products(nm_id) ON DELETE CASCADE
);
```

**Визуализация связи:**
```
products:
  nm_id=456770543, vendor_code="rykzak_black"
       ↑
       │ (связь через nm_id)
       │
adv_params:
  nm_id=456770543, date=2025-10-09, views=4710
  nm_id=456770543, date=2025-10-10, views=3190
```

**Правила:**
- ✅ Можно добавить запись в `adv_params` с `nm_id=456770543` (есть в `products`)
- ❌ Нельзя добавить с `nm_id=999999999` (нет в `products`)
- `ON DELETE CASCADE` → при удалении из `products`, удалятся и записи из `adv_params`

---

### UNIQUE INDEX (Составной уникальный ключ)
**Что это:** Гарантирует уникальность комбинации нескольких полей.

**Пример:**
```sql
CREATE UNIQUE INDEX ux_adv_params_nm_date ON adv_params(nm_id, date);
```

**Что это значит:**
- ✅ Можно: `nm_id=123, date=2025-10-09`
- ✅ Можно: `nm_id=123, date=2025-10-10` (другая дата)
- ✅ Можно: `nm_id=456, date=2025-10-09` (другой артикул)
- ❌ Нельзя: еще одну запись `nm_id=123, date=2025-10-09` (уже есть!)

**Визуализация:**
```
adv_params:
  nm_id=456770543, date=2025-10-09  ✅ OK
  nm_id=456770543, date=2025-10-10  ✅ OK (другая дата)
  nm_id=467102886, date=2025-10-09  ✅ OK (другой артикул)
  nm_id=456770543, date=2025-10-09  ❌ ОШИБКА! Дубликат!
```

---

## 🔍 Индексы (Indexes)

**Что это:** Ускоряют поиск данных (как указатель в книге).

**Пример:**
```sql
CREATE INDEX idx_adv_params_date ON adv_params(date);
```

**Без индекса:**
```
Ищем date=2025-10-09 → перебираем ВСЕ 1000 записей (медленно)
```

**С индексом:**
```
Ищем date=2025-10-09 → сразу находим нужные (быстро!)
```

**Индексы в проекте:**
```sql
-- Быстрый поиск по артикулу
CREATE INDEX idx_adv_params_nm_id ON adv_params(nm_id);

-- Быстрый поиск по дате
CREATE INDEX idx_adv_params_date ON adv_params(date);

-- Быстрый поиск по комбинации (составной индекс)
CREATE INDEX idx_adv_daily_nm_date ON adv_campaign_daily_stats(nm_id, date);
```

---

## ⚙️ Триггеры (Triggers)

**Что это:** Автоматические действия при INSERT/UPDATE/DELETE.

**Пример из проекта:**
```sql
CREATE TRIGGER update_adv_campaign_daily_stats_updated_at
    BEFORE UPDATE ON adv_campaign_daily_stats
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();
```

**Как работает:**
1. Кто-то обновляет запись в `adv_campaign_daily_stats`
2. **BEFORE UPDATE** → перед сохранением срабатывает триггер
3. Триггер вызывает функцию `update_updated_at_column()`
4. Функция устанавливает `updated_at = NOW()`
5. Запись сохраняется с новым `updated_at`

**Пример:**
```sql
-- Обновляем views
UPDATE adv_campaign_daily_stats 
SET views = 5000 
WHERE nm_id = 456770543;

-- Триггер автоматически добавляет:
-- updated_at = NOW()
```

---

## 📊 Операции с данными

### INSERT (Вставка)
```sql
INSERT INTO products (nm_id, vendor_code, title, category_wb)
VALUES (456770543, 'rykzak_black', 'Рюкзак черный', 'Рюкзаки');
```

### SELECT (Выборка)
```sql
-- Все записи
SELECT * FROM products;

-- С фильтром
SELECT * FROM products WHERE nm_id = 456770543;

-- С сортировкой
SELECT * FROM products ORDER BY nm_id DESC;

-- Агрегация
SELECT nm_id, SUM(views) AS total_views
FROM adv_campaign_daily_stats
GROUP BY nm_id;
```

### UPDATE (Обновление)
```sql
UPDATE products 
SET title = 'Рюкзак черный обновленный'
WHERE nm_id = 456770543;
```

### DELETE (Удаление)
```sql
DELETE FROM products WHERE nm_id = 456770543;
```

### UPSERT (Вставка или обновление)
```sql
INSERT INTO adv_params (nm_id, date, views)
VALUES (456770543, '2025-10-09', 4710)
ON CONFLICT (nm_id, date) DO UPDATE SET
  views = EXCLUDED.views,
  updated_at = NOW();
```

**Что происходит:**
- Если записи с `(nm_id=456770543, date=2025-10-09)` нет → INSERT
- Если такая запись уже есть → UPDATE

---

## 🎯 Практический пример: Агрегация

Наша функция `aggregate_adv_params` объединяет несколько концепций:

### Входные данные (adv_campaign_daily_stats):
```
advert_id | nm_id      | date       | views
----------|------------|------------|-------
27114105  | 456770543  | 2025-10-09 | 1637
27114115  | 456770543  | 2025-10-09 | 3073
29284679  | 473520914  | 2025-10-09 | 1133
```

### SQL запрос:
```sql
SELECT
  nm_id,
  date,
  SUM(views) AS views  -- Агрегация: суммируем views
FROM adv_campaign_daily_stats
WHERE date >= '2025-10-09' AND date <= '2025-10-09'  -- Фильтр
GROUP BY nm_id, date  -- Группировка
```

### Результат (adv_params):
```
nm_id      | date       | views
-----------|------------|-------
456770543  | 2025-10-09 | 4710  (1637 + 3073)
473520914  | 2025-10-09 | 1133
```

### UPSERT с условием:
```sql
ON CONFLICT (nm_id, date) DO UPDATE SET
  views = EXCLUDED.views,
  updated_at = CASE 
    WHEN adv_params.views IS DISTINCT FROM EXCLUDED.views
    THEN NOW()  -- Обновляем только если изменилось
    ELSE adv_params.updated_at  -- Оставляем старое
  END;
```

---

## 🔗 Связи между таблицами

### Один к одному (One-to-One)
Один продукт → одна запись в products (PRIMARY KEY гарантирует)

### Один ко многим (One-to-Many)
```
products (1)
  └── adv_params (много)
  
Один продукт может иметь много записей статистики (по одной на каждую дату)

products:
  nm_id=456770543, vendor_code="rykzak_black"
       ↓
adv_params:
  nm_id=456770543, date=2025-10-09
  nm_id=456770543, date=2025-10-10
  nm_id=456770543, date=2025-10-11
```

### Многие ко многим (Many-to-Many)
```
campaigns (много) ←→ products (много)
  
Через промежуточную таблицу adv_campaign_daily_stats:
  
campaign_1 → продукт_1
campaign_1 → продукт_2
campaign_2 → продукт_1
campaign_2 → продукт_3
```

---

## 🛡️ Ограничения (Constraints)

### NOT NULL
```sql
nm_id BIGINT NOT NULL  -- Значение обязательно
```
- ✅ `nm_id=456770543`
- ❌ `nm_id=NULL` (ошибка!)

### DEFAULT
```sql
created_at TIMESTAMPTZ DEFAULT NOW()  -- Значение по умолчанию
```
Если не указать `created_at` при INSERT, автоматически установится текущее время.

### CHECK
```sql
views INTEGER CHECK (views >= 0)  -- Проверка условия
```
- ✅ `views=100`
- ❌ `views=-10` (ошибка! views не может быть отрицательным)

---

## 🎓 Полезные SQL паттерны

### 1. Условная агрегация
```sql
SELECT
  nm_id,
  SUM(CASE WHEN date >= '2025-10-09' THEN views ELSE 0 END) AS views_period_1,
  SUM(CASE WHEN date >= '2025-10-10' THEN views ELSE 0 END) AS views_period_2
FROM adv_params
GROUP BY nm_id;
```

### 2. Подзапросы
```sql
SELECT *
FROM products
WHERE nm_id IN (
  SELECT DISTINCT nm_id 
  FROM adv_params 
  WHERE views > 1000
);
```

### 3. JOIN (Соединение таблиц)
```sql
SELECT 
  p.vendor_code,
  a.date,
  a.views
FROM products p
JOIN adv_params a ON p.nm_id = a.nm_id
WHERE a.date = '2025-10-09';
```

### 4. Window Functions (Оконные функции)
```sql
SELECT
  nm_id,
  date,
  views,
  SUM(views) OVER (PARTITION BY nm_id ORDER BY date) AS cumulative_views
FROM adv_params;
```

---

## 📝 Итоговая картина проекта

```
┌─────────────────┐
│    products     │ ← Справочник артикулов (40 записей)
│   nm_id (PK)    │
│   vendor_code   │
└────────┬────────┘
         │ FK (nm_id)
         ↓
┌─────────────────────────────┐
│ adv_campaign_daily_stats    │ ← Детальная статистика (тысячи записей)
│ advert_id, nm_id, date (PK) │   Каждая кампания × артикул × дата
│ views, clicks, sum, orders  │
└──────────────┬──────────────┘
               │ Агрегация (SUM, GROUP BY)
               ↓
         ┌─────────────┐
         │ adv_params  │ ← Агрегированная статистика (десятки записей)
         │ nm_id, date │   Один артикул × дата (сумма всех кампаний)
         │ (PK)        │
         └─────────────┘
```

---

## 🚀 Что дальше?

Теперь ты понимаешь:
- ✅ Как устроены таблицы и связи
- ✅ Что такое первичные и внешние ключи
- ✅ Как работают индексы и триггеры
- ✅ Как агрегируются данные в нашем проекте
- ✅ Почему updated_at обновлялся неправильно и как мы это исправили

**Следующие шаги:**
1. Изучи миграции других таблиц (`cr_daily_stats`, `product_sizes`)
2. Попробуй написать свои SQL запросы для анализа данных
3. Экспериментируй с JOIN и агрегациями в Supabase Studio

Удачи! 🎯


