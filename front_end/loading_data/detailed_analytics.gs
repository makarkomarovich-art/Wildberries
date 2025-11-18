/**
 * Создает лист "Подробная аналитика" на основе данных из "РНП АБЦ" и "База РНП"
 * 
 * Входные данные из листа "РНП АБЦ":
 * - A3:A - имена (до первой пустой)
 * - C1, D1 - даты начала и конца периода
 * - B2:2 - заголовки атрибутов (до первой пустой)
 * 
 * Данные берутся из листа "База РНП"
 */

function createDetailedAnalytics() {
  const spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
  
  // Листы
  const sourceSheet = spreadsheet.getSheetByName("РНП АБЦ");
  const baseSheet = spreadsheet.getSheetByName("База РНП");
  
  if (!sourceSheet) {
    throw new Error("Лист 'РНП АБЦ' не найден");
  }
  if (!baseSheet) {
    throw new Error("Лист 'База РНП' не найден");
  }
  
  // ============================================
  // 1. Читаем входные данные из "РНП АБЦ"
  // ============================================
  
  // Имена из A3:A (до первой пустой)
  const names = [];
  let row = 3;
  while (true) {
    const cellValue = sourceSheet.getRange(row, 1).getValue();
    if (!cellValue || cellValue.toString().trim() === "") {
      break;
    }
    names.push(cellValue.toString().trim());
    row++;
  }
  
  if (names.length === 0) {
    throw new Error("Не найдено ни одного имени в столбце A (начиная с A3)");
  }
  
  // Даты из C1 и D1
  const dateStartValue = sourceSheet.getRange(1, 3).getValue();
  const dateEndValue = sourceSheet.getRange(1, 4).getValue();
  
  if (!dateStartValue || !dateEndValue) {
    throw new Error("Даты в C1 и D1 не найдены");
  }
  
  // Преобразуем даты (могут быть Date объектом или строкой DD.MM.YYYY)
  let dateStart, dateEnd;
  
  if (dateStartValue instanceof Date) {
    dateStart = new Date(dateStartValue);
  } else {
    const dateStr = dateStartValue.toString().trim();
    // Проверяем формат DD.MM.YYYY
    if (/^\d{2}\.\d{2}\.\d{4}$/.test(dateStr)) {
      const [day, month, year] = dateStr.split('.');
      dateStart = new Date(parseInt(year), parseInt(month) - 1, parseInt(day));
    } else {
      dateStart = new Date(dateStartValue);
    }
  }
  
  if (dateEndValue instanceof Date) {
    dateEnd = new Date(dateEndValue);
  } else {
    const dateStr = dateEndValue.toString().trim();
    // Проверяем формат DD.MM.YYYY
    if (/^\d{2}\.\d{2}\.\d{4}$/.test(dateStr)) {
      const [day, month, year] = dateStr.split('.');
      dateEnd = new Date(parseInt(year), parseInt(month) - 1, parseInt(day));
    } else {
      dateEnd = new Date(dateEndValue);
    }
  }
  
  if (isNaN(dateStart.getTime()) || isNaN(dateEnd.getTime())) {
    throw new Error("Неверный формат дат в C1 или D1");
  }
  
  // Генерируем все даты между dateStart и dateEnd включительно
  const dates = [];
  const currentDate = new Date(dateStart);
  while (currentDate <= dateEnd) {
    dates.push(formatDateDDMMYYYY(currentDate));
    currentDate.setDate(currentDate.getDate() + 1);
  }
  
  if (dates.length === 0) {
    throw new Error("Не удалось сгенерировать даты");
  }
  
  // Атрибуты из B2:2 (до первой пустой)
  const attributes = [];
  let col = 2;
  while (true) {
    const cellValue = sourceSheet.getRange(2, col).getValue();
    if (!cellValue || cellValue.toString().trim() === "") {
      break;
    }
    attributes.push(cellValue.toString().trim());
    col++;
  }
  
  if (attributes.length === 0) {
    throw new Error("Не найдено ни одного атрибута в строке 2 (начиная с B2)");
  }
  
  // ============================================
  // 2. Читаем заголовки из "База РНП"
  // ============================================
  
  const baseHeaders = baseSheet.getRange(1, 1, 1, baseSheet.getLastColumn()).getValues()[0];
  const headerToColumn = {};
  for (let i = 0; i < baseHeaders.length; i++) {
    const header = baseHeaders[i];
    if (header && header.toString().trim() !== "") {
      headerToColumn[header.toString().trim()] = i + 1; // 1-based column index
    }
  }
  
  // Проверяем наличие необходимых столбцов
  const requiredColumns = ["Дата", "Имя"];
  for (const reqCol of requiredColumns) {
    if (!headerToColumn[reqCol]) {
      throw new Error(`В листе 'База РНП' не найден столбец '${reqCol}'`);
    }
  }
  
  // ============================================
  // 3. Читаем все данные из "База РНП"
  // ============================================
  
  const lastRow = baseSheet.getLastRow();
  const baseData = baseSheet.getRange(2, 1, lastRow - 1, baseSheet.getLastColumn()).getValues();
  
  // Создаем индекс для быстрого поиска: (дата, имя) -> строка данных
  const dataIndex = {};
  const dateCol = headerToColumn["Дата"] - 1; // 0-based
  const nameCol = headerToColumn["Имя"] - 1; // 0-based
  
  for (let i = 0; i < baseData.length; i++) {
    const row = baseData[i];
    const date = row[dateCol];
    const name = row[nameCol];
    
    if (!date || !name) continue;
    
    // Обрабатываем дату: может быть Date объектом или строкой DD.MM.YYYY
    let dateStr;
    if (date instanceof Date) {
      dateStr = formatDateDDMMYYYY(date);
    } else {
      // Если это строка, пытаемся нормализовать
      const dateStrRaw = date.toString().trim();
      // Проверяем формат DD.MM.YYYY
      if (/^\d{2}\.\d{2}\.\d{4}$/.test(dateStrRaw)) {
        dateStr = dateStrRaw;
      } else {
        // Пытаемся преобразовать через Date
        const dateObj = new Date(date);
        if (!isNaN(dateObj.getTime())) {
          dateStr = formatDateDDMMYYYY(dateObj);
        } else {
          continue; // Пропускаем строку с неверной датой
        }
      }
    }
    
    const nameStr = name.toString().trim();
    const key = `${dateStr}|${nameStr}`;
    
    dataIndex[key] = row;
  }
  
  // ============================================
  // 4. Создаем новый лист
  // ============================================
  
  let targetSheetName = "Подробная аналитика";
  let sheetNumber = 1;
  
  while (spreadsheet.getSheetByName(targetSheetName)) {
    sheetNumber++;
    targetSheetName = `Подробная аналитика (${sheetNumber})`;
  }
  
  const targetSheet = spreadsheet.insertSheet(targetSheetName);
  
  // ============================================
  // 5. Строим структуру и заполняем данные
  // ============================================
  
  // Заголовки дат (C1, D1, E1...)
  targetSheet.getRange(1, 3, 1, dates.length).setValues([dates]);
  
  // Подготавливаем данные для записи
  const rows = [];
  
  for (const attribute of attributes) {
    // Проверяем, есть ли такой атрибут в заголовках "База РНП"
    const attributeCol = headerToColumn[attribute];
    if (!attributeCol) {
      console.log(`⚠️ Атрибут '${attribute}' не найден в листе 'База РНП', пропускаем`);
      continue;
    }
    
    const attributeColIndex = attributeCol - 1; // 0-based
    
    // Для каждого имени
    for (const name of names) {
      const row = [null, name]; // A - будет заполнено позже, B - имя
      
      // Для каждой даты
      for (const date of dates) {
        const key = `${date}|${name}`;
        const dataRow = dataIndex[key];
        
        if (dataRow && dataRow[attributeColIndex] !== undefined && dataRow[attributeColIndex] !== null && dataRow[attributeColIndex] !== "") {
          row.push(dataRow[attributeColIndex]);
        } else {
          row.push(""); // Пусто, если данных нет
        }
      }
      
      rows.push(row);
    }
  }
  
  // Записываем данные
  if (rows.length > 0) {
    targetSheet.getRange(2, 2, rows.length, rows[0].length).setValues(rows);
  }
  
  // ============================================
  // 6. Объединяем ячейки в столбце A
  // ============================================
  
  let rowIndex = 2; // Начинаем с B2
  for (const attribute of attributes) {
    const attributeCol = headerToColumn[attribute];
    if (!attributeCol) continue;
    
    // Количество строк для этого атрибута = количество имен
    const numRows = names.length;
    
    if (numRows > 0) {
      // Объединяем ячейки A{rowIndex}:A{rowIndex + numRows - 1}
      const mergeRange = targetSheet.getRange(rowIndex, 1, numRows, 1);
      mergeRange.merge();
      mergeRange.setValue(attribute);
      mergeRange.setVerticalAlignment("middle");
      mergeRange.setHorizontalAlignment("center");
    }
    
    rowIndex += numRows;
  }
  
  // ============================================
  // 7. Форматирование
  // ============================================
  
  // Форматируем заголовки дат
  if (dates.length > 0) {
    const headerRange = targetSheet.getRange(1, 3, 1, dates.length);
    headerRange.setFontWeight("bold");
    headerRange.setBackground("#e8f0fe");
  }
  
  // Форматируем столбец A (атрибуты)
  if (rows.length > 0) {
    const attrRange = targetSheet.getRange(2, 1, rows.length, 1);
    attrRange.setFontWeight("bold");
    attrRange.setBackground("#f3f4f6");
  }
  
  // Форматируем столбец B (имена)
  if (rows.length > 0) {
    const nameRange = targetSheet.getRange(2, 2, rows.length, 1);
    nameRange.setBackground("#fff9e6");
  }
  
  // Автоподбор ширины столбцов
  targetSheet.autoResizeColumns(1, targetSheet.getLastColumn());
  
  SpreadsheetApp.getUi().alert(`✅ Лист '${targetSheetName}' успешно создан!\n\nИмен: ${names.length}\nДат: ${dates.length}\nАтрибутов: ${attributes.length}\nСтрок данных: ${rows.length}`);
}

/**
 * Форматирует дату в формат DD.MM.YYYY
 */
function formatDateDDMMYYYY(date) {
  const day = String(date.getDate()).padStart(2, '0');
  const month = String(date.getMonth() + 1).padStart(2, '0');
  const year = date.getFullYear();
  return `${day}.${month}.${year}`;
}

