# 📊 Анализ модуля ADV Params для тестирования

**Дата:** 21 октября 2025  
**Статус:** 🔄 Планирование тестирования  
**Target coverage:** 80%+ для structure_validator.py

---

## 🔍 Структура модуля

```
excel_actions/adv_params_ea/
├── structure_validator.py    🔥 КРИТИЧНО (тестируем сейчас)
├── transform.py               🟡 ВАЖНО (потом)
├── supabase_writer.py         🟡 СРЕДНЕ (потом)
└── data_validator.py          🟢 НИЗКО (потом)
```

---

## 🎯 1. structure_validator.py

### **Что делает:**
Валидация ответов API через **JSON Schema**:
- `/adv/v3/fullstats` - полная статистика по кампаниям
- `/adv/v1/promotion/count` - количество активных кампаний

### **Отличие от CR:**
- CR использует **ручную валидацию** (if/else)
- ADV использует **JSON Schema** (jsonschema библиотека)

### **Функции для тестирования:**

| Функция | Строк | Что делает | Критичность |
|---------|-------|------------|-------------|
| `load_schema()` | 6 | Загружает JSON Schema из файла | 🔥 |
| `validate_promotion_count_response()` | 6 | Валидирует ответ /promotion/count | 🔥 |
| `validate_fullstats_response()` | 6 | Валидирует ответ /fullstats | 🔥 |
| `get_validation_report()` | 7 | Детальный отчёт о валидации | 🟡 |

**Всего:** ~32 строки кода (по данным coverage)

---

## 📋 План тестирования structure_validator.py

### **Категория 1: Валидация Fullstats (приоритет 🔥)**

**Ожидаемые тесты:**

| # | Тест | Что проверяет | Фикстура |
|---|------|---------------|----------|
| 1 | `test_valid_fullstats_passes` | Валидный ответ | `adv_fullstats_valid.json` |
| 2 | `test_empty_fullstats_array` | Пустой массив [] | `adv_fullstats_empty.json` |
| 3 | `test_fullstats_not_array` | Ответ не массив (объект) | `adv_fullstats_not_array.json` |
| 4 | `test_missing_advertId` | Нет advertId в кампании | `adv_fullstats_missing_advertId.json` |
| 5 | `test_wrong_type_advertId` | advertId строка вместо числа | `adv_fullstats_wrong_type_advertId.json` |
| 6 | `test_missing_clicks` | Нет поля clicks | `adv_fullstats_missing_clicks.json` |
| 7 | `test_negative_clicks` | clicks < 0 (нарушение minimum: 0) | `adv_fullstats_negative_clicks.json` |
| 8 | `test_missing_days_array` | Нет поля days | `adv_fullstats_missing_days.json` |
| 9 | `test_days_not_array` | days не массив | `adv_fullstats_days_not_array.json` |
| 10 | `test_missing_date_in_day` | Нет date в элементе days | `adv_fullstats_missing_date.json` |

**Target:** 10 тестов → ~70% coverage

### **Категория 2: Валидация Promotion Count (приоритет 🔥)**

| # | Тест | Что проверяет | Фикстура |
|---|------|---------------|----------|
| 11 | `test_valid_promotion_count_passes` | Валидный ответ | `adv_promotion_count_valid.json` |
| 12 | `test_promotion_count_missing_count` | Нет поля count | `adv_promotion_count_missing.json` |
| 13 | `test_promotion_count_wrong_type` | count строка вместо числа | `adv_promotion_count_wrong_type.json` |

**Target:** 3 теста → +10% coverage

### **Категория 3: Вспомогательные функции (приоритет 🟡)**

| # | Тест | Что проверяет | Фикстура |
|---|------|---------------|----------|
| 14 | `test_load_schema_file_not_found` | Схема не найдена → exception | - |
| 15 | `test_get_validation_report_valid` | Отчёт для валидных данных | `adv_fullstats_valid.json` |
| 16 | `test_get_validation_report_invalid` | Отчёт с ошибками | `adv_fullstats_missing_advertId.json` |

**Target:** 3 теста → +10% coverage

---

## 📊 Итого для structure_validator.py:

```
Всего тестов: 16
Target coverage: 80%+
Критичных тестов: 13
Важных тестов: 3
```

---

## 🔬 2. transform.py (следующий этап)

### **Что делает:**
Агрегация и трансформация данных:
1. Группировка по `nm_id + date`
2. Суммирование метрик (views, clicks, sum)
3. Вычисление производных метрик:
   - **CPM** = (sum / views) * 1000 (cost per 1000 views)
   - **CPC** = sum / clicks (cost per click)
   - **CTR** = (clicks / views) * 100 (click-through rate %)

### **Критичные моменты для тестирования:**

| Что может сломаться | Тест | Приоритет |
|---------------------|------|-----------|
| Деление на ноль (views=0) | `test_cpm_with_zero_views` | 🔥 |
| Деление на ноль (clicks=0) | `test_cpc_with_zero_clicks` | 🔥 |
| NULL значения | `test_null_values_handling` | 🔥 |
| Группировка дубликатов | `test_group_by_nm_id_date` | 🔥 |
| Суммирование метрик | `test_sum_aggregation` | 🔥 |
| Вычисление CPM | `test_calculate_cpm` | 🔥 |
| Вычисление CPC | `test_calculate_cpc` | 🔥 |
| Вычисление CTR | `test_calculate_ctr` | 🔥 |
| Округление до 2 знаков | `test_rounding` | 🟡 |
| Отрицательные значения | `test_negative_values` | 🟡 |

**Ожидаемые тесты:** ~10-12

**Target coverage:** 70%

---

## 💾 3. supabase_writer.py (низкий приоритет)

### **Что делает:**
Запись данных в Supabase:
- Upsert в таблицу `adv_campaign_daily_stats`
- Upsert в таблицу `adv_params`
- Обработка конфликтов (ON CONFLICT)

### **Тесты:**

| Что тестировать | Приоритет |
|----------------|-----------|
| Upsert новой записи | 🟡 |
| Update существующей записи | 🟡 |
| Обработка ошибок БД | 🟡 |

**Ожидаемые тесты:** ~5

**Target coverage:** 60%

---

## ✅ 4. data_validator.py (очень низкий приоритет)

Проверка данных ПОСЛЕ записи в БД.

**Target coverage:** 50% (потом, если будет время)

---

## 🎯 Roadmap тестирования ADV

### **Этап 1: structure_validator.py** 🔄 **СЕЙЧАС**

```
✅ Создать структуру fixtures/adv_params/
✅ Создать 16 фикстур
✅ Написать 16 тестов
✅ Достичь 80%+ coverage
```

**Время:** ~2-3 часа работы

### **Этап 2: transform.py** ⏳ **ПОТОМ**

```
✅ Создать фикстуры для агрегации
✅ Написать 10-12 тестов
✅ Достичь 70% coverage
```

**Время:** ~1-2 часа

### **Этап 3: supabase_writer.py** ⏳ **ОПЦИОНАЛЬНО**

```
✅ Написать 5 тестов
✅ Достичь 60% coverage
```

**Время:** ~1 час

---

## 🆚 Сравнение: CR vs ADV

| Аспект | CR Daily Stats | ADV Params |
|--------|----------------|------------|
| **Валидация** | Ручная (if/else) | JSON Schema |
| **Сложность** | Средняя (вложенные объекты) | Высокая (массивы кампаний + days) |
| **Строк кода** | 104 | 32 (validator) + ~100 (transform) |
| **Тестов создано** | 17 ✅ | 0 ⏳ |
| **Coverage** | 82.69% ✅ | 0% |
| **API** | `/nm-report/detail` | `/adv/v3/fullstats` |
| **Подход к тестам** | Ручная проверка каждого поля | Проверка через JSON Schema |

---

## 💡 Ключевые отличия в подходе к тестированию

### **CR Daily Stats (ручная валидация):**

```python
# Проверяем каждое поле вручную
if "nmID" not in card:
    return False
if not isinstance(card["nmID"], int):
    return False
```

**Тесты:** Проверяем каждое условие отдельно.

### **ADV Params (JSON Schema):**

```python
# JSON Schema описывает всю структуру
schema = {
    "type": "object",
    "required": ["advertId", "clicks"],
    "properties": {
        "advertId": {"type": "integer"},
        "clicks": {"type": "integer", "minimum": 0}
    }
}

# Одна строка валидирует всё
jsonschema.validate(response, schema)
```

**Тесты:** Проверяем что JSON Schema работает + edge cases.

---

## 🚀 Следующие шаги

1. ✅ Создать `fixtures/adv_params/` директорию
2. ✅ Создать первые 3-5 фикстур (valid, empty, missing fields)
3. ✅ Написать первые 3-5 тестов
4. ✅ Запустить → проверить coverage
5. ✅ Добавить остальные тесты до 80%+

**Начинаем с фикстур для fullstats!** 🎯

---

**Обновлено:** 21 октября 2025


