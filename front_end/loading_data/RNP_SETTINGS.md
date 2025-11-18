# 📋 Настройки РНП (Справка)

## 🎯 Структура проекта РНП

Проект РНП состоит из 5 компонентов:

### 1. **РНП BASE** (`rnp_base/`)
- **Назначение:** Единая база данных с 4 уровнями агрегации (Артикули, Предметы, Склейки, Магазин)
- **Файл для запуска:** `rnp_base_orchestrator.py`
- **Команда запуска:** 
  ```bash
  python -m front_end.loading_data.rnp_base.rnp_base_orchestrator
  ```

### 2. **РНП по артикулам** (`rnp_by_article/`)
- **Назначение:** Детальная аналитика по каждому артикулу с матрицей дат
- **Файл для запуска:** `orchestrator.py`
- **Команда запуска:** 
  ```bash
  python -m front_end.loading_data.rnp_by_article.orchestrator
  ```
- **Настройки:** см. ниже ⬇️

### 3. **РНП по категориям** (`rnp_by_category/`)
- **Назначение:** Агрегированная аналитика по категориям товаров
- **Файл для запуска:** `orchestrator.py`
- **Команда запуска:** 
  ```bash
  python -m front_end.loading_data.rnp_by_category.orchestrator
  ```

### 4. **РНП по склейкам** (`rnp_by_imt/`)
- **Назначение:** Аналитика по склейкам товаров
- **Файл для запуска:** `orchestrator.py`
- **Команда запуска:** 
  ```bash
  python -m front_end.loading_data.rnp_by_imt.orchestrator
  ```

### 5. **РНП по магазину** (`rnp_by_shop/`)
- **Назначение:** Общая аналитика по всему магазину
- **Файл для запуска:** `orchestrator.py`
- **Команда запуска:** 
  ```bash
  python -m front_end.loading_data.rnp_by_shop.orchestrator
  ```

---

## ⚙️ Критические настройки

### **ATTRIBUTES_PER_ITEM** 
📍 Файлы: `rnp_by_*/update_sheet.py` (разные линии в разных файлах)

**Назначение:** Количество метрик/атрибутов на один объект (артикул, категорию, склейку, магазин)

**Текущие значения по типам:**
- `rnp_by_article/update_sheet.py` (линия 51): **16 атрибутов**
- `rnp_by_category/update_sheet.py` (линия 48): **8 атрибутов**
- `rnp_by_imt/update_sheet.py` (линия 49): **8 атрибутов**
- `rnp_by_shop/update_sheet.py` (линия 47): **8 атрибутов**

**Атрибуты в порядке (ATTRIBUTE_ORDER):**
```python
[
    "Заказы",
    "Сумма заказов", 
    "Расход на рекламу",
    "ДРР",
    "Клики общие",
    "В корзину",
    "Конверсия в корзину",
    "Конверсия в заказ",
    "CPM",
    "Рекламные просмотры",
    "Рекламные клики",
    "CTR",
    "CPC",
    "Остатки WB",  ← ВНИМАНИЕ: Была удалена из rnp_base, может быть удалена отсюда
    "Цена одного заказа",
    "Журнал изменений",
]
```

**Как это влияет на код:**
- Алгоритм использует это значение для расчета шага при переборе столбцов
- Если изменить это число, коду нужно пересчитать позиции всех атрибутов
- Формула для нахождения позиции: `column_index = first_date_column + (article_index * ATTRIBUTES_PER_ITEM) + attribute_index`

**Где это используется:**
- `rnp_by_article/update_sheet.py:51`
- `rnp_by_category/update_sheet.py:51`
- `rnp_by_imt/update_sheet.py:51`
- `rnp_by_shop/update_sheet.py:51`

---

### **FIRST_DATE_COLUMN_INDEX**
📍 Файлы: `rnp_by_article/update_sheet.py` (линия 48) и другие

**Назначение:** Индекс первой колонки, содержащей дату

**Текущее значение:** `6` (колонка F в A1 нотации)

**Используется для:**
- Определения, где начинаются столбцы с датами
- Расчета позиции каждого значения для каждого артикула

---

### **ATTRIBUTE_ORDER**
📍 Файлы: `rnp_by_article/update_sheet.py` (линия 52) и другие

**Назначение:** Порядок атрибутов, в котором они расположены в Google Sheets

**Текущий порядок:** 16 атрибутов (см. выше)

**ВАЖНО:** Если изменить количество атрибутов, нужно:
1. Обновить `ATTRIBUTES_PER_ITEM` на новое значение
2. Обновить `ATTRIBUTE_ORDER` с правильным количеством элементов
3. Обновить ВСЕ 4 файла (`rnp_by_article`, `rnp_by_category`, `rnp_by_imt`, `rnp_by_shop`)

---

## 📍 Конфигурация Google Sheets

Для каждого типа РНП используется переменная окружения `SHEET_NAMES` из `api_keys.py`:

```python
SHEET_NAMES = {
    RNP_REPORT_ID: "РНП",              # основной лист по артикулам
    # Другие листы должны быть в той же таблице с ID RNP_REPORT_ID
}
```

Названия листов жёстко закодированы в `update_sheet.py` файлах:
- `rnp_by_article/update_sheet.py`: Читает из `SHEET_NAMES[RNP_REPORT_ID]` → "РНП"
- `rnp_by_category/update_sheet.py`: Читает из `SHEET_NAMES[RNP_REPORT_ID]` → "РНП Предмет"
- `rnp_by_imt/update_sheet.py`: Читает из `SHEET_NAMES[RNP_REPORT_ID]` → "РНП Склейка"
- `rnp_by_shop/update_sheet.py`: Читает из `SHEET_NAMES[RNP_REPORT_ID]` → "РНП Магазин"

---

## 🚀 Главный Orchestrator

📍 Файл: `/front_end/loading_data/rnp_orchestrator.py`

Запускает все 5 функций РНП в последовательном порядке:
1. РНП BASE (база данных)
2. РНП по артикулам
3. РНП по склейкам
4. РНП по предметам
5. РНП по магазину

**Команда запуска:**
```bash
python /front_end/loading_data/rnp_orchestrator.py
```

---

## 🔧 Если нужно изменить количество атрибутов

1. **Обновите Google Sheet структуру** - добавьте/удалите столбцы с атрибутами
2. **Обновите в ВСЕх 4 файлах:**
   - `rnp_by_article/update_sheet.py`
   - `rnp_by_category/update_sheet.py`
   - `rnp_by_imt/update_sheet.py`
   - `rnp_by_shop/update_sheet.py`

   Следующие строки:
   - `ATTRIBUTES_PER_ITEM = X` (новое количество)
   - `ATTRIBUTE_ORDER = [...]` (новый список атрибутов)

3. **Протестируйте** каждую функцию отдельно через её `orchestrator.py`

---

## 📝 Примечания

- **Все настройки хранятся локально** в каждом `update_sheet.py` файле
- **РНП BASE** использует отдельную конфигурацию в `header_config.py` (17 колонок)
- **Артикулы, Категории, Склейки, Магазин** используют одинаковую структуру (16 атрибутов)
- **Остатки WB** были удалены из rnp_base, но могут быть в других РНП (проверьте)

