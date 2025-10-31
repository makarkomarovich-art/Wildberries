# 📋 Unified Test Specification — Only Wildberries

## 🎯 Цель
Единая, воспроизводимая стратегия тестирования и покрытия кода по всему проекту. Тесты и отчёты всегда живут в папке `tests`.

## 📂 Структура
```
tests/
├── fixtures/                 # JSON-фикстуры
│   ├── cr_daily_stats/
│   └── adv_params/
├── test_adv_params.py        # ADV: структура/валидация API
├── test_cr_daily_stats.py    # CR: структура/валидация API
├── test_week_stats.py        # unit-тесты: writers (rows, stats)
├── test_week_stats_integration.py  # интеграция: весь pipeline через main
├── pytest.ini                # конфигурация PyTest + Coverage (HTML -> tests/htmlcov)
├── pytest_with_logs.ini      # конфигурация с логами (опционально)
└── htmlcov/                  # HTML-отчёт coverage (генерируется автоматически)
```

Правило: всё, что связано с тестами (код тестов, фикстуры, конфиги, отчёты) хранится в папке `tests`.

## 🔧 Запуск
- Все тесты с Coverage (HTML-отчёт):
  - `cd tests && pytest`
  - Открыть: `tests/htmlcov/index.html` (или `open "tests/htmlcov/index.html"`)
- С логами: `cd tests && pytest -c pytest_with_logs.ini`
- Частично:
  - `cd tests && pytest test_cr_daily_stats.py`
  - `cd tests && pytest test_week_stats_integration.py::test_end_to_end_week_stats_via_main`

## 🧪 Подход к тестированию
- Разделяем уровни:
  - Unit: чистые функции и writers без внешних сетей/БД.
  - Integration (предпочтительно для бизнес-потоков): через оркестратор `main`, с подменой внешних клиентов/БД (фейки/monkeypatch).
- Минимизируем дублирование: проверяем детали в unit, а целостность — в интеграции. Лишние повторяющиеся проверки в `excel_actions` избегаем, если они уже покрыты интеграционным тестом через `main`.

## 🧩 Week Stats (эталон)
- Интеграционный тест `test_week_stats_integration.py` покрывает единый поток:
  - `wb_api` (официальный + CURL) → валидация → агрегирование → запись в Supabase (фейковый клиент) → `week_stats` → финальная сверка сумм с `week_reports`.
  - Проверки: создаются нужные записи в `week_reports`, `week_rows`, `week_stats`; формат логов (минимальный); сверка сумм OK/Warning.
- Unit-тесты `test_week_stats.py` фокусируются на:
  - `week_rows_writer`: фильтрация `nm_id`, пропуск дублей по `(realizationreport_id, rr_id)`, корректная статистика/логирование, обработка ошибок.
  - `week_stats_writer`: агрегация по `nm_id`, все проценты умножаются на 100, чтение только из `week_rows`, проверка сумм.

## 📈 Coverage
- Конфигурация в `tests/pytest.ini` (унитарный и интеграционный запуск создают HTML-отчёт в `tests/htmlcov`).
- Формат: стандартный coverage.py HTML (индекс: `tests/htmlcov/index.html`).
- Покрытие можно расширять постепенно. Цель по критичным модулям:
  - Week Stats writers: 95%+
  - CR/ADV валидаторы: 80%+
  - Остальные — по приоритету (см. раздел ниже)

## 🧭 Приоритеты (коротко)
- Критично: структурные валидаторы API (CR, ADV), бизнес-орchestrator (week_stats через `main`).
- Важно: writers (запись в БД, дедупликация, фильтрация). 
- Низкий приоритет: вспомогательные утилиты и пост-проверки.

## 📜 Правила и конвенции
- Тесты не обращаются к сети/реальной БД — только фейки (in-memory) и monkeypatch.
- Никаких файлов/временных артефактов вне `tests/`.
- Логирование в тестах — минимальное; оцениваем ключевые сообщения и итоговые статусы, не спамим.
- Валидации: не проверяем все поля «по списку», достаточно репрезентативных кейсов.

## 📌 Итог
- Единый отчёт Coverage по проекту: всегда `tests/htmlcov/index.html`.
- Все тесты и отчёты — строго в `tests/`.
- Week Stats покрыт end-to_end через `main`, детали — через unit в writers.
- Конфигурация запущена и воспроизводима одной командой: `cd tests && pytest`.


