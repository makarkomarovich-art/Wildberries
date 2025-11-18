# 🎯 Резюме: Новые Orchestrator Функции

## ✅ Что было создано

Для каждого типа РНП создана **отдельная функция запуска** (orchestrator.py):

### 1. РНП по артикулам
📍 `front_end/loading_data/rnp_by_article/orchestrator.py`

```bash
python -m front_end.loading_data.rnp_by_article.orchestrator
```

### 2. РНП по категориям
📍 `front_end/loading_data/rnp_by_category/orchestrator.py`

```bash
python -m front_end.loading_data.rnp_by_category.orchestrator
```

### 3. РНП по склейкам
📍 `front_end/loading_data/rnp_by_imt/orchestrator.py`

```bash
python -m front_end.loading_data.rnp_by_imt.orchestrator
```

### 4. РНП по магазину
📍 `front_end/loading_data/rnp_by_shop/orchestrator.py`

```bash
python -m front_end.loading_data.rnp_by_shop.orchestrator
```

---

## 🔧 Найденные настройки атрибутов

Все настройки **сохранены** в каждом `update_sheet.py` файле:

```
┌─────────────────────────────────────────────────────────┐
│                   КОЛИЧЕСТВО АТРИБУТОВ                  │
├─────────────────────────────┬───────────────┬────────────┤
│ Тип РНП                     │ Атрибутов     │ Линия      │
├─────────────────────────────┼───────────────┼────────────┤
│ rnp_by_article              │ 16            │ 51         │
│ rnp_by_category             │ 8             │ 48         │
│ rnp_by_imt                  │ 8             │ 49         │
│ rnp_by_shop                 │ 8             │ 47         │
└─────────────────────────────┴───────────────┴────────────┘
```

### Важные константы

**ATTRIBUTES_PER_ITEM** - Количество метрик на один объект  
- Используется для расчета позиций столбцов при переборе данных
- **Разные значения для разных типов РНП!**

**FIRST_DATE_COLUMN_INDEX** - Индекс первой колонки с датой
- `rnp_by_article`: **F** (6)
- `rnp_by_category`: **D** (4)
- `rnp_by_imt`: **D** (4)  
- `rnp_by_shop`: **D** (4)

**ATTRIBUTE_ORDER** - Порядок атрибутов в строках
- 16 атрибутов для артикулов
- 8 атрибутов для остальных типов

---

## 📋 Главный orchestrator

Файл: `/front_end/loading_data/rnp_orchestrator.py`

Запускает все 5 функций РНП по порядку:
1. РНП BASE
2. РНП по артикулам
3. РНП по склейкам
4. РНП по категориям
5. РНП по магазину

```bash
python /front_end/loading_data/rnp_orchestrator.py
```

---

## 📝 Документация

Полная справка по настройкам находится в:  
📖 `/front_end/loading_data/RNP_SETTINGS.md`

---

## 🎯 Использование

### Запустить одну функцию РНП:
```bash
python -m front_end.loading_data.rnp_by_article.orchestrator
```

### Запустить все РНП:
```bash
python /front_end/loading_data/rnp_orchestrator.py
```

### Запустить только РНП BASE:
```bash
python -m front_end.loading_data.rnp_base.rnp_base_orchestrator
```

