/**
 * Создает лист "Подробная аналитика" на основе данных из "РНП АБЦ" и "База РНП".
 * Взято из прежнего файла "подробная аналитика.js" без изменений логики.
 */
function createDetailedAnalytics() {
  const spreadsheet = SpreadsheetApp.getActiveSpreadsheet();

  SpreadsheetApp.getActive().toast(
    'В случае сортированных имен скрипту понадобится немного больше времени на выполнение',
    'Запуск аналитики',
    5
  );

  const sourceSheet = spreadsheet.getSheetByName('РНП АБЦ');
  const baseSheet   = spreadsheet.getSheetByName('База РНП');

  if (!sourceSheet) throw new Error("Лист 'РНП АБЦ' не найден");
  if (!baseSheet) throw new Error("Лист 'База РНП' не найден");

  const lastRowSource = sourceSheet.getLastRow();
  if (lastRowSource < 3) {
    throw new Error('В листе \"РНП АБЦ\" нет данных ниже A2');
  }

  const height = lastRowSource - 2; // начиная с 3-й строки
  const nameValues = sourceSheet.getRange(3, 1, height, 1).getValues();
  let names = [];

  for (let i = 0; i < height; i++) {
    const rowIndex = 3 + i;
    if (sourceSheet.isRowHiddenByFilter(rowIndex) || sourceSheet.isRowHiddenByUser(rowIndex)) {
      continue;
    }
    const cellValue = nameValues[i][0];
    const text = cellValue ? cellValue.toString().trim() : '';
    if (text === '') break;
    names.push(text);
  }

  if (names.length === 0) {
    throw new Error('Не найдено ни одного видимого имени в столбце A (начиная с A3)');
  }

  const ui = SpreadsheetApp.getUi();
  const resp = ui.prompt(
    'Порядок имен',
    'Введите имена через пробел в желаемом порядке (можно оставить пустым):',
    ui.ButtonSet.OK_CANCEL
  );

  if (resp.getSelectedButton() === ui.Button.OK) {
    const input = resp.getResponseText().trim();
    if (input) {
      const requested = input.split(/\s+/);
      const availableSet = new Set(names);
      const used = new Set();
      const orderedTop = [];

      for (const name of requested) {
        if (availableSet.has(name) && !used.has(name)) {
          orderedTop.push(name);
          used.add(name);
        }
      }

      const remaining = names.filter(n => !used.has(n));
      if (orderedTop.length > 0) names = orderedTop.concat(remaining);
    }
  }

  const dateStartValue = sourceSheet.getRange(1, 3).getValue();
  const dateEndValue   = sourceSheet.getRange(1, 4).getValue();
  if (!dateStartValue || !dateEndValue) {
    throw new Error('Даты в C1 и D1 не найдены');
  }

  let dateStart, dateEnd;

  if (dateStartValue instanceof Date) {
    dateStart = new Date(dateStartValue);
  } else {
    const dateStr = dateStartValue.toString().trim();
    if (/^\d{2}\.\d{2}\.\d{4}$/.test(dateStr)) {
      const [day, month, year] = dateStr.split('.');
      dateStart = new Date(parseInt(year, 10), parseInt(month, 10) - 1, parseInt(day, 10));
    } else {
      dateStart = new Date(dateStartValue);
    }
  }

  if (dateEndValue instanceof Date) {
    dateEnd = new Date(dateEndValue);
  } else {
    const dateStr = dateEndValue.toString().trim();
    if (/^\d{2}\.\d{2}\.\d{4}$/.test(dateStr)) {
      const [day, month, year] = dateStr.split('.');
      dateEnd = new Date(parseInt(year, 10), parseInt(month, 10) - 1, parseInt(day, 10));
    } else {
      dateEnd = new Date(dateEndValue);
    }
  }

  if (isNaN(dateStart.getTime()) || isNaN(dateEnd.getTime())) {
    throw new Error('Неверный формат дат в C1 или D1');
  }

  const dates = [];
  const currentDate = new Date(dateStart);
  while (currentDate <= dateEnd) {
    dates.push(formatDateDDMMYYYY(currentDate));
    currentDate.setDate(currentDate.getDate() + 1);
  }
  if (dates.length === 0) throw new Error('Не удалось сгенерировать даты');

  const attributes = [];
  let col = 2;
  while (true) {
    const cellValue = sourceSheet.getRange(2, col).getValue();
    if (!cellValue || cellValue.toString().trim() === '') break;
    attributes.push(cellValue.toString().trim());
    col++;
  }
  if (attributes.length === 0) {
    throw new Error('Не найдено ни одного атрибута в строке 2 (начиная с B2)');
  }

  const baseHeaders = baseSheet
    .getRange(1, 1, 1, baseSheet.getLastColumn())
    .getValues()[0];

  const headerToColumn = {};
  for (let i = 0; i < baseHeaders.length; i++) {
    const header = baseHeaders[i];
    if (header && header.toString().trim() !== '') {
      headerToColumn[header.toString().trim()] = i + 1;
    }
  }

  const requiredColumns = ['Дата', 'Имя'];
  for (const reqCol of requiredColumns) {
    if (!headerToColumn[reqCol]) {
      throw new Error("В листе 'База РНП' не найден столбец '" + reqCol + "'");
    }
  }

  const lastRowBase = baseSheet.getLastRow();
  if (lastRowBase < 2) {
    throw new Error("Лист 'База РНП' не содержит данных (нет строк ниже заголовков)");
  }

  const baseData = baseSheet.getRange(
    2,
    1,
    lastRowBase - 1,
    baseSheet.getLastColumn()
  ).getValues();

  const dataIndex = {};
  const dateCol = headerToColumn['Дата'] - 1;
  const nameCol = headerToColumn['Имя']  - 1;

  for (let i = 0; i < baseData.length; i++) {
    const rowData = baseData[i];
    const dateVal = rowData[dateCol];
    const nameVal = rowData[nameCol];
    if (!dateVal || !nameVal) continue;

    let dateStr;
    if (dateVal instanceof Date) {
      dateStr = formatDateDDMMYYYY(dateVal);
    } else {
      const dateStrRaw = dateVal.toString().trim();
      if (/^\d{2}\.\d{2}\.\d{4}$/.test(dateStrRaw)) {
        dateStr = dateStrRaw;
      } else {
        const dateObj = new Date(dateVal);
        if (!isNaN(dateObj.getTime())) {
          dateStr = formatDateDDMMYYYY(dateObj);
        } else {
          continue;
        }
      }
    }

    const nameStr = nameVal.toString().trim();
    const key = dateStr + '|' + nameStr;
    dataIndex[key] = rowData;
  }

  let targetSheetName = 'Подробная аналитика';
  let sheetNumber = 1;
  while (spreadsheet.getSheetByName(targetSheetName)) {
    sheetNumber++;
    targetSheetName = 'Подробная аналитика (' + sheetNumber + ')';
  }

  const sheets = spreadsheet.getSheets();
  let sourceIndex = -1;
  for (let i = 0; i < sheets.length; i++) {
    if (sheets[i].getName() === 'РНП АБЦ') {
      sourceIndex = i;
      break;
    }
  }

  let targetSheet;
  if (sourceIndex >= 0) {
    targetSheet = spreadsheet.insertSheet(targetSheetName, sourceIndex + 1);
  } else {
    targetSheet = spreadsheet.insertSheet(targetSheetName);
  }

  const datesHeaderRange = targetSheet.getRange(1, 3, 1, dates.length);
  datesHeaderRange.setValues([dates]);
  datesHeaderRange.setFontWeight('bold');

  const rows = [];
  const blocks = [];

  const PERCENT_ATTRS = new Set([
    'ДРР',
    'Конверсия в корзину',
    'Конверсия в заказ',
    'CTR',
  ]);

  const HIGH_GOOD = new Set([
    'Заказы',
    'Конверсия в корзину',
    'Конверсия в заказ',
    'CTR',
  ]);

  const LOW_GOOD = new Set([
    'ДРР',
    'CPM',
    'CPC',
    'CTC',
  ]);

  let currentRow = 2;

  for (const attribute of attributes) {
    const attributeCol = headerToColumn[attribute];
    if (!attributeCol) {
      console.log("⚠️ Атрибут '" + attribute + "' не найден в листе 'База РНП', пропускаем");
      continue;
    }

    const attributeColIndex = attributeCol - 1;

    const sepRowIndex = currentRow;
    const emptyRow = new Array(1 + dates.length).fill('');
    rows.push(emptyRow);
    currentRow++;

    const startDataRow = currentRow;

    for (const name of names) {
      const rowArr = [name];
      for (const dateStr of dates) {
        const key = dateStr + '|' + name;
        const dataRow = dataIndex[key];

        let value = '';
        if (
          dataRow &&
          dataRow[attributeColIndex] !== undefined &&
          dataRow[attributeColIndex] !== null &&
          dataRow[attributeColIndex] !== ''
        ) {
          value = normalizeMetricValueForAttribute(attribute, dataRow[attributeColIndex], PERCENT_ATTRS);
        }
        rowArr.push(value);
      }
      rows.push(rowArr);
      currentRow++;
    }

    const endDataRow = currentRow - 1;
    blocks.push({
      attribute: attribute,
      sepRow: sepRowIndex,
      startRow: startDataRow,
      endRow: endDataRow,
    });
  }

  if (rows.length > 0) {
    targetSheet
      .getRange(2, 2, rows.length, rows[0].length)
      .setValues(rows);
  }

  const lastRow = targetSheet.getLastRow();
  const lastCol = targetSheet.getLastColumn();

  targetSheet.getBandings().forEach(b => b.remove());

  const GREEN = '#d4edda';
  const YELLOW = '#fff3cd';
  const RED = '#f8d7da';

  const rules = targetSheet.getConditionalFormatRules();

  const stripeColor = '#e3f2fd';
  for (const block of blocks) {
    const stripeRange = targetSheet.getRange(block.sepRow, 1, 1, lastCol);
    stripeRange.setBackground(stripeColor);
    stripeRange.setFontWeight('bold');

    const titleCell = targetSheet.getRange(block.sepRow, 1);
    titleCell.setValue(block.attribute.toString().toUpperCase());
    titleCell.setHorizontalAlignment('left');

    if (block.endRow >= block.startRow) {
      const dataHeight = block.endRow - block.startRow + 1;
      const dataWidth = Math.max(0, lastCol - 2);
      const groupRange = targetSheet.getRange(block.startRow, 1, dataHeight, 1);
      groupRange.shiftRowGroupDepth(1);

      if (dataHeight > 0 && dataWidth > 0) {
        const dataRange = targetSheet.getRange(block.startRow, 3, dataHeight, dataWidth);

        if (HIGH_GOOD.has(block.attribute)) {
          const ruleHigh = SpreadsheetApp.newConditionalFormatRule()
            .setGradientMinpointWithValue(RED, SpreadsheetApp.InterpolationType.PERCENTILE, '0')
            .setGradientMidpointWithValue(YELLOW, SpreadsheetApp.InterpolationType.PERCENTILE, '50')
            .setGradientMaxpointWithValue(GREEN, SpreadsheetApp.InterpolationType.PERCENTILE, '95')
            .setRanges([dataRange])
            .build();
          rules.push(ruleHigh);
        } else if (LOW_GOOD.has(block.attribute)) {
          const ruleLow = SpreadsheetApp.newConditionalFormatRule()
            .setGradientMinpointWithValue(GREEN, SpreadsheetApp.InterpolationType.PERCENTILE, '0')
            .setGradientMidpointWithValue(YELLOW, SpreadsheetApp.InterpolationType.PERCENTILE, '50')
            .setGradientMaxpointWithValue(RED, SpreadsheetApp.InterpolationType.PERCENTILE, '95')
            .setRanges([dataRange])
            .build();
          rules.push(ruleLow);
        }

        if (PERCENT_ATTRS.has(block.attribute)) {
          dataRange.setNumberFormat('0.00%');
        }
      }
    }
  }

  targetSheet.setConditionalFormatRules(rules);
  targetSheet.setFrozenRows(1);
  targetSheet.setFrozenColumns(2);

  if (lastRow > 0 && lastCol > 0) {
    const baseSize = targetSheet.getRange(1, 3).getFontSize() || 10;
    const newSize = baseSize + 1;
    targetSheet.getRange(1, 1, lastRow, lastCol).setFontSize(newSize);
  }

  targetSheet.autoResizeColumns(1, lastCol);

  SpreadsheetApp.getUi().alert(
    "✅ Лист '" + targetSheetName + "' успешно создан!\n\n" +
    'Имен (видимых): ' + names.length + '\n' +
    'Дат: ' + dates.length + '\n' +
    'Атрибутов: ' + attributes.length + '\n' +
    'Строк (вкл. полосы): ' + lastRow
  );
}

function normalizeMetricValueForAttribute(attribute, rawValue, PERCENT_ATTRS) {
  let value = normalizeMetricValue(rawValue);
  if (value === '') return '';

  if (PERCENT_ATTRS.has(attribute)) {
    let num = value;
    if (typeof num === 'string') {
      const s = num.replace(',', '.').trim();
      const parsed = parseFloat(s);
      if (!isNaN(parsed)) {
        num = parsed;
      } else {
        return value;
      }
    }
    if (typeof num === 'number') {
      return num / 100;
    }
  }
  return value;
}

function normalizeMetricValue(value) {
  if (value === null || value === '' || value === undefined) return '';
  if (typeof value === 'number' && value === 0) return '';
  if (typeof value === 'string') {
    const trimmed = value.trim();
    if (/^0([.,]0+)?$/.test(trimmed)) return '';
  }
  return value;
}

function formatDateDDMMYYYY(date) {
  const day   = String(date.getDate()).padStart(2, '0');
  const month = String(date.getMonth() + 1).padStart(2, '0');
  const year  = date.getFullYear();
  return day + '.' + month + '.' + year;
}







