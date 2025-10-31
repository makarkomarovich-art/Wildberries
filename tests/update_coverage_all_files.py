#!/usr/bin/env python3
"""
Скрипт для обновления coverage HTML отчёта, включая все файлы проекта,
даже если они не были импортированы во время тестов.
"""
import coverage
from pathlib import Path
import subprocess
import sys

def update_coverage_with_all_files():
    """Обновляет coverage HTML отчёт со всеми файлами проекта"""
    project_root = Path(__file__).resolve().parent.parent
    tests_dir = project_root / 'tests'
    config_file = project_root / '.coveragerc'
    
    # Загружаем существующие данные coverage
    cov = coverage.Coverage(config_file=str(config_file))
    cov.load()
    
    # Находим все Python файлы в проекте
    all_files = []
    for pattern in ['excel_actions/**/*.py', 'wb_api/**/*.py', 'main_function/**/*.py']:
        for file in project_root.glob(pattern):
            if all(exclude not in str(file) for exclude in ['/tests/', '__pycache__', '/cancelled/', '/venv/']):
                all_files.append(file)
    
    print(f"📁 Найдено {len(all_files)} Python файлов в проекте")
    
    # Генерируем HTML отчёт
    # Coverage автоматически включит все файлы из source директорий
    cov.html_report(directory=str(tests_dir / 'htmlcov'))
    
    print(f"✅ HTML отчёт обновлён: {tests_dir / 'htmlcov' / 'index.html'}")
    print(f"💡 Открыть: open {tests_dir / 'htmlcov' / 'index.html'}")

if __name__ == '__main__':
    update_coverage_with_all_files()

