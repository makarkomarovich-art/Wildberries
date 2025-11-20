function openProtectionDialog() {
  const html = HtmlService.createHtmlOutputFromFile('ProtectionDialog')
    .setWidth(400)
    .setHeight(320);
  SpreadsheetApp.getUi().showModalDialog(html, 'Блокировка ячеек');
}

/**
 * Простая защита строк/столбцов с шагом и ограничением по колонкам.
 * config = {
 *   sheetName: string | '',
 *   orientation: 'ROWS' | 'COLS',
 *   startIndex: string | number,
 *   step: string | number,
 *   repeatCount: string | number,
 *   fromCol: string,
 *   toCol: string
 * }
 */
function applyProtection(config) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = config.sheetName
    ? ss.getSheetByName(config.sheetName)
    : ss.getActiveSheet();

  if (!sheet) throw new Error('Лист не найден');

  const start = Number(config.startIndex);
  const step = Number(config.step) || 1;
  const repeatCount = Number(config.repeatCount) || 1;

  if (!start || !step || !repeatCount) {
    throw new Error('startIndex, step и repeatCount должны быть числами > 0');
  }

  const lastRow = sheet.getLastRow();
  const lastCol = sheet.getMaxColumns();

  const fromColIndex = config.fromCol
    ? parseColumnIndex_(config.fromCol, 1)
    : 1;
  const toColIndex = config.toCol
    ? parseColumnIndex_(config.toCol, lastCol)
    : lastCol;

  const colStart = Math.max(1, Math.min(fromColIndex, lastCol));
  const colEnd = Math.max(colStart, Math.min(toColIndex, lastCol));

  for (let i = 0; i < repeatCount; i++) {
    const idx = start + i * step;

    if (config.orientation === 'ROWS') {
      if (idx > lastRow) break;
      const width = colEnd - colStart + 1;
      const range = sheet.getRange(idx, colStart, 1, width);
      const protection = range.protect().setDescription('Редактура: авто-строка');
      protection.setWarningOnly(true);
    } else { // COLS
      if (idx > lastCol) break;
      const range = sheet.getRange(1, idx, lastRow, 1);
      const protection = range.protect().setDescription('Редактура: авто-столбец');
      protection.setWarningOnly(true);
    }
  }
}

/** Удалить все защиты с активного листа */
function removeAllProtectionsFromActiveSheet() {
  const sheet = SpreadsheetApp.getActiveSheet();
  const ui = SpreadsheetApp.getUi();

  const resp = ui.alert(
    'Подтверждение',
    `Удалить все блокировки (защищённые диапазоны и листы) на листе "${sheet.getName()}"?`,
    ui.ButtonSet.YES_NO
  );
  if (resp !== ui.Button.YES) {
    return;
  }

  const rangeProtections = sheet.getProtections(SpreadsheetApp.ProtectionType.RANGE);
  const sheetProtections = sheet.getProtections(SpreadsheetApp.ProtectionType.SHEET);

  rangeProtections.forEach(p => p.remove());
  sheetProtections.forEach(p => p.remove());

  ui.alert(`Удалено защит: диапазонов = ${rangeProtections.length}, листов = ${sheetProtections.length}`);
}

/**
 * Парсит индекс колонки из буквы (A, F, AA) или числа.
 */
function parseColumnIndex_(value, fallback) {
  if (!value) return fallback;
  const trimmed = value.toString().trim();
  if (!trimmed) return fallback;

  // Если число
  const asNum = Number(trimmed);
  if (!isNaN(asNum) && asNum > 0) return Math.floor(asNum);

  // Буквы колонки
  const letters = trimmed.toUpperCase().replace(/[^A-Z]/g, '');
  if (!letters) return fallback;
  let result = 0;
  for (let i = 0; i < letters.length; i++) {
    result = result * 26 + (letters.charCodeAt(i) - 64); // 'A' = 65
  }
  return result || fallback;
}



