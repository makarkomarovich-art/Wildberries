# 🚀 Быстрый старт

## Есть ли сохранённые настройки про атрибуты?

**✅ ДА, ВСЕ СОХРАНЕНЫ!**

Все настройки находятся в каждом файле `update_sheet.py`:

```
📍 rnp_by_article/update_sheet.py
   ├─ ATTRIBUTES_PER_ITEM = 16        (линия 51)
   ├─ FIRST_DATE_COLUMN_INDEX = 6     (линия 48)
   └─ ATTRIBUTE_ORDER = [...]         (линия 52)

📍 rnp_by_category/update_sheet.py
   ├─ ATTRIBUTES_PER_ITEM = 8         (линия 48)
   ├─ FIRST_DATE_COLUMN_INDEX = 4     (линия 45)
   └─ ATTRIBUTE_ORDER = [...]         (линия 51)

📍 rnp_by_imt/update_sheet.py
   ├─ ATTRIBUTES_PER_ITEM = 8         (линия 49)
   ├─ FIRST_DATE_COLUMN_INDEX = 4     (линия 46)
   └─ ATTRIBUTE_ORDER = [...]         (линия 52)

📍 rnp_by_shop/update_sheet.py
   ├─ ATTRIBUTES_PER_ITEM = 8         (линия 47)
   ├─ FIRST_DATE_COLUMN_INDEX = 4     (линия 44)
   └─ ATTRIBUTE_ORDER = [...]         (линия 50)
```

---

## 🎯 Как работает логика переборки?

**ATTRIBUTES_PER_ITEM** определяет, с каким **шагом** код перемещается по столбцам:

```
Для артикулов (16 атрибутов):
Артикул 1: столбцы 6-21   (FIRST_DATE_COLUMN_INDEX + 0*16 до 0*16+16)
Артикул 2: столбцы 22-37  (FIRST_DATE_COLUMN_INDEX + 1*16 до 1*16+16)
Артикул 3: столбцы 38-53  (FIRST_DATE_COLUMN_INDEX + 2*16 до 2*16+16)
...

Для категорий (8 атрибутов):
Категория 1: столбцы 4-11  (FIRST_DATE_COLUMN_INDEX + 0*8 до 0*8+8)
Категория 2: столбцы 12-19 (FIRST_DATE_COLUMN_INDEX + 1*8 до 1*8+8)
...
```

**Формула:**
```
start_col = FIRST_DATE_COLUMN_INDEX + (index * ATTRIBUTES_PER_ITEM)
end_col = start_col + ATTRIBUTES_PER_ITEM
```

---

## 🎮 Новые Orchestrator функции

Созданы отдельные файлы для запуска каждого типа РНП:

### Запуск по артикулам:
```bash
python -m front_end.loading_data.rnp_by_article.orchestrator
```

### Запуск по категориям:
```bash
python -m front_end.loading_data.rnp_by_category.orchestrator
```

### Запуск по склейкам:
```bash
python -m front_end.loading_data.rnp_by_imt.orchestrator
```

### Запуск по магазину:
```bash
python -m front_end.loading_data.rnp_by_shop.orchestrator
```

### Запуск РНП BASE:
```bash
python -m front_end.loading_data.rnp_base.rnp_base_orchestrator
```

### Запуск всех (главный orchestrator):
```bash
python /front_end/loading_data/rnp_orchestrator.py
```

---

## 📚 Что изменилось?

- ✅ Созданы `orchestrator.py` для каждого типа РНП
- ✅ Добавлена документация (`RNP_SETTINGS.md`)
- ✅ Добавлено резюме (`ORCHESTRATORS_SUMMARY.md`)
- ✅ Все старые настройки сохранены и задокументированы

---

## 📖 Документация

**Полная справка:** `/front_end/loading_data/RNP_SETTINGS.md`  
**Резюме Orchestrators:** `/front_end/loading_data/ORCHESTRATORS_SUMMARY.md`  
**Readme артикулов:** `/front_end/loading_data/rnp_by_article/README.md`

