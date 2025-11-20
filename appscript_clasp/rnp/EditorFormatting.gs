function openFormattingDialog() {
  const html = HtmlService.createHtmlOutputFromFile('FormattingDialog')
    .setWidth(400)
    .setHeight(320);
  SpreadsheetApp.getUi().showModalDialog(html, 'Условное форматирование');
}

/** Очистить все правила условного форматирования с активного листа */
function clearAllConditionalFormattingFromActiveSheet() {
  const sheet = SpreadsheetApp.getActiveSheet();
  const ui = SpreadsheetApp.getUi();

  const resp = ui.alert(
    'Подтверждение',
    `Удалить все правила условного форматирования на листе "${sheet.getName()}"?`,
    ui.ButtonSet.YES_NO
  );
  if (resp !== ui.Button.YES) {
    return;
  }

  sheet.setConditionalFormatRules([]);
  ui.alert(`Все правила условного форматирования удалены с листа: ${sheet.getName()}`);
}

/** Фиксированный набор цветов для градиентов */
function getPaletteColors_(sheet) {
  return {
    red:    '#ea9999',  // красный
    orange: '#f9cb9c',  // оранжевый (Light Orange 2)
    green:  '#93c47d'   // зелёный (Light Green 1)
  };
}

/**
 * Применяет условное форматирование с градиентом.
 * config = {
 *   sheetName: string | '',
 *   ranges: string[],           // до 5 диапазонов в A1-нотации
 *   gradientType: 'TWO' | 'THREE',
 *   reverse: boolean,
 *   orientation: 'ROWS' | 'COLS',
 *   step: string | number,
 *   repeatCount: string | number
 * }
 */
function applyFormatting(config) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = config.sheetName
    ? ss.getSheetByName(config.sheetName)
    : ss.getActiveSheet();

  if (!sheet) throw new Error('Лист не найден');

  const palette = getPaletteColors_(sheet);

  const step = Number(config.step) || 1;
  const repeatCount = Number(config.repeatCount) || 1;
  const orientation = config.orientation === 'COLS' ? 'COLS' : 'ROWS';

  const baseRangesA1 = (config.ranges || [])
    .map(r => (r && String(r).trim()) || '')
    .filter(Boolean);
  if (!baseRangesA1.length) {
    throw new Error('Не указано ни одного диапазона для форматирования');
  }

  const baseRanges = baseRangesA1.map(r => sheet.getRange(r));

  const maxRow = sheet.getLastRow();
  const maxCol = sheet.getLastColumn();

  const ranges = [];
  baseRanges.forEach(baseRange => {
    const startRow = baseRange.getRow();
    const startCol = baseRange.getColumn();
    const numRows = baseRange.getNumRows();
    const numCols = baseRange.getNumColumns();

    for (let i = 0; i < repeatCount; i++) {
      const offset = i * step;
      let r = startRow;
      let c = startCol;
      if (orientation === 'ROWS') {
        r = startRow + offset;
        if (r > maxRow) break;
      } else {
        c = startCol + offset;
        if (c > maxCol) break;
      }
      ranges.push(sheet.getRange(r, c, numRows, numCols));
    }
  });
  if (!ranges.length) throw new Error('Не найдено ни одного диапазона для применения форматирования');

  // Берём существующие правила и выбрасываем те, что пересекаются с нашими диапазонами
  const existingRules = sheet.getConditionalFormatRules() || [];
  const baseRules = [];
  existingRules.forEach(rule => {
    const ruleRanges = rule.getRanges();
    const intersects = ruleRanges.some(rr =>
      ranges.some(tr => rangesOverlap_(rr, tr))
    );
    if (!intersects) {
      baseRules.push(rule);
    }
  });

  const rules = baseRules;

  // Для каждой строки/колонки — отдельное новое правило
  ranges.forEach(targetRange => {
    let rule;
    const builder = SpreadsheetApp.newConditionalFormatRule()
      .setRanges([targetRange]);

    if (config.gradientType === 'TWO') {
      const minColor = config.reverse ? palette.green : palette.orange;
      const maxColor = config.reverse ? palette.orange  : palette.green;

      rule = builder
        .setGradientMinpointWithValue(
          minColor,
          SpreadsheetApp.InterpolationType.PERCENT,
          '0'
        )
        .setGradientMaxpointWithValue(
          maxColor,
          SpreadsheetApp.InterpolationType.PERCENT,
          '100'
        )
        .build();

    } else {
      const minColor = config.reverse ? palette.green  : palette.red;
      const midColor = palette.orange;
      const maxColor = config.reverse ? palette.red   : palette.green;

      rule = builder
        .setGradientMinpointWithValue(
          minColor,
          SpreadsheetApp.InterpolationType.PERCENT,
          '0'
        )
        .setGradientMidpointWithValue(
          midColor,
          SpreadsheetApp.InterpolationType.PERCENT,
          '50'
        )
        .setGradientMaxpointWithValue(
          maxColor,
          SpreadsheetApp.InterpolationType.PERCENT,
          '100'
        )
        .build();
    }

    rules.push(rule);
  });
  sheet.setConditionalFormatRules(rules);
}

/**
 * Проверяет, пересекаются ли два прямоугольных диапазона.
 */
function rangesOverlap_(r1, r2) {
  const r1Top = r1.getRow();
  const r1Bottom = r1.getLastRow();
  const r1Left = r1.getColumn();
  const r1Right = r1.getLastColumn();

  const r2Top = r2.getRow();
  const r2Bottom = r2.getLastRow();
  const r2Left = r2.getColumn();
  const r2Right = r2.getLastColumn();

  const rowsDisjoint = r1Bottom < r2Top || r2Bottom < r1Top;
  const colsDisjoint = r1Right < r2Left || r2Right < r1Left;

  return !(rowsDisjoint || colsDisjoint);
}


