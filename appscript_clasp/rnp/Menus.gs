function onOpen() {
  const ui = SpreadsheetApp.getUi();

  ui.createMenu('🌍 Меню')
    // Аналитика
    .addItem('📊 Подробная аналитика', 'createDetailedAnalytics')
    .addSeparator()
    // Редактура
    .addItem('🎨 Условное форматирование', 'openFormattingDialog')
    .addItem('🧼 Очистить условное форматирование (лист)', 'clearAllConditionalFormattingFromActiveSheet')
    .addItem('🔒 Блокировка ячеек', 'openProtectionDialog')
    .addItem('🧹 Удалить все блокировки (лист)', 'removeAllProtectionsFromActiveSheet')
    .addToUi();
}


