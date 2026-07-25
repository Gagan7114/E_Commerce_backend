/**
 * JM Inventory  ->  this Google Sheet   (Google Apps Script - runs on Google's
 * servers, no PC / no Python needed).
 *
 * Pulls live JM Inventory for selected warehouses from the Ecom app and writes
 * 5 columns (Item Code, Item Name, Warehouse Code, On Hand, Available) into a
 * tab of the Sheet. Add a time-driven trigger to auto-refresh.
 *
 * ONLY FINISHED GOODS are written: an item is kept when its Item Code starts
 * with "FG" AND its SAP item group (OITB ItmsGrpNam) is "FINISHED". The group
 * is used purely to qualify the row - it is NOT written as a column.
 *
 * Your own columns to the RIGHT of column E are preserved on every refresh,
 * INCLUDING formulas (they are re-written as formulas, not flattened to their
 * last value). See writeSheet_ for the details.
 *
 * -- SETUP (one time) ------------------------------------------------------
 * 1. Open your Google Sheet -> Extensions -> Apps Script. Paste this whole file
 *    into Code.gs (replacing what's there) and Save.  (Opening it from the
 *    Sheet "binds" the script to that Sheet, so it can write to it and travels
 *    with the Sheet when you share/hand it over.)
 * 2. Left sidebar -> Project Settings (gear) -> Script Properties -> Add:
 *       ECOM_EMAIL     = your Ecom login email
 *       ECOM_PASSWORD  = your Ecom password
 *    (Storing them here keeps passwords out of the code.)
 * 3. Back in the editor pick the function "refreshJmInventory" -> Run.
 *    Approve the one-time permission prompt (it needs to call the Ecom API and
 *    edit the Sheet).  Done - the tab fills with data.
 *
 * To refresh later: use the "JM Inventory -> Refresh now" menu that appears on
 * the Sheet, or set a trigger (see scheduleDailyRefresh below).
 */

var CONFIG = {
  // Deployed Ecom app - must be reachable from the internet (Google's servers
  // call it). Do NOT use localhost here; that only exists on your own PC.
  apiBase: 'https://ecom.jivo.in',

  // Warehouses grouped by SAP company DB ("source"). One API call reads ONE
  // DB, so mart and oil are fetched separately and merged.
  warehouses: {
    mart: ['GP-FGM', 'BH-FGM'],
    oil:  ['DL-INT', 'DL-FG'],
  },

  status: '',       // '' = all | 'Y' = active only | 'N' = frozen only
  stockState: '',   // '' = all | 'in' = in-stock only | 'low'
  tabName: 'JM Inventory',

  // Finished-goods filter (both conditions must hold for a row to be written).
  // Set either one to '' to switch that half of the filter off.
  itemCodePrefix: 'FG',      // Item Code must start with this (FG0000348, ...)
  itemGroupName: 'FINISHED', // SAP item group name, compared case-insensitively

  // When a manual column is a formula column (one formula covers at least half
  // of the rows), copy that formula onto rows that are new in this refresh, so
  // the column stays filled without dragging it down by hand.
  fillDownFormulas: true,

  // Only needed if you keep this as a STANDALONE script (not opened from the
  // Sheet). Leave blank when bound via Extensions -> Apps Script.
  sheetId: '',
};

var COLUMNS = [
  ['ItemCode', 'Item Code'],
  ['ItemName', 'Item Name'],
  ['WhsCode', 'Warehouse Code'],
  ['OnHand', 'On Hand'],
  ['Available', 'Available'],
];

/** Main entry point - pull live data and overwrite the tab. */
function refreshJmInventory() {
  var props = PropertiesService.getScriptProperties();
  var email = props.getProperty('ECOM_EMAIL') || '';
  var password = props.getProperty('ECOM_PASSWORD') || '';
  if (!email || !password) {
    throw new Error('Set ECOM_EMAIL and ECOM_PASSWORD in Project Settings -> Script Properties.');
  }
  var apiBase = (props.getProperty('ECOM_API_BASE') || CONFIG.apiBase).replace(/\/+$/, '');

  var token = login_(apiBase, email, password);
  var result = fetchAll_(apiBase, token);
  writeSheet_(result.rows);
  logCounts_(result.perWh, result.rows.length, result.skipped);
}

function login_(apiBase, email, password) {
  var resp = UrlFetchApp.fetch(apiBase + '/api/auth/login', {
    method: 'post',
    contentType: 'application/json',
    payload: JSON.stringify({ email: email, password: password }),
    muteHttpExceptions: true,
  });
  var code = resp.getResponseCode();
  if (code === 401) throw new Error('Login failed: invalid email or password.');
  if (code < 200 || code >= 300) {
    throw new Error('Login failed: HTTP ' + code + ' ' + resp.getContentText().slice(0, 300));
  }
  var token = JSON.parse(resp.getContentText()).token;
  if (!token) throw new Error('Login succeeded but no token was returned.');
  return token;
}

function fetchSource_(apiBase, token, source, codes) {
  var params = [
    'warehouse_code=' + encodeURIComponent(codes.join(',')),
    'source=' + encodeURIComponent(source),
    'page_size=100000',
    'page=0',
  ];
  if (CONFIG.status) params.push('status=' + encodeURIComponent(CONFIG.status));
  if (CONFIG.stockState) params.push('stock_state=' + encodeURIComponent(CONFIG.stockState));

  var resp = UrlFetchApp.fetch(apiBase + '/api/sap/inventory-overview?' + params.join('&'), {
    method: 'get',
    headers: { Authorization: 'Bearer ' + token },
    muteHttpExceptions: true,
  });
  var code = resp.getResponseCode();
  if (code === 403) throw new Error("This account lacks the 'sap.view' permission needed to read JM Inventory.");
  if (code < 200 || code >= 300) {
    throw new Error('Inventory request failed (source=' + source + '): HTTP ' + code + ' ' + resp.getContentText().slice(0, 300));
  }
  return JSON.parse(resp.getContentText()).data || [];
}

// Finished goods only: Item Code starts with CONFIG.itemCodePrefix AND the row's
// SAP item group (GroupName, returned by the API but not written to the sheet)
// equals CONFIG.itemGroupName. The group check is what keeps out FG-looking
// codes that sit in another group.
function isFinishedGood_(row) {
  var prefix = String(CONFIG.itemCodePrefix || '').toUpperCase();
  if (prefix) {
    var code = String(row.ItemCode == null ? '' : row.ItemCode).trim().toUpperCase();
    if (code.slice(0, prefix.length) !== prefix) return false;
  }
  var wanted = String(CONFIG.itemGroupName || '').toUpperCase();
  if (wanted) {
    var group = String(row.GroupName == null ? '' : row.GroupName).trim().toUpperCase();
    if (group !== wanted) return false;
  }
  return true;
}

function fetchAll_(apiBase, token) {
  var rows = [];
  var perWh = {};
  var skipped = 0;
  // Seed every requested code at 0 so a warehouse returning nothing still shows.
  Object.keys(CONFIG.warehouses).forEach(function (source) {
    (CONFIG.warehouses[source] || []).forEach(function (c) { perWh[c] = 0; });
  });

  Object.keys(CONFIG.warehouses).forEach(function (source) {
    var codes = (CONFIG.warehouses[source] || []).filter(function (c) { return c; });
    if (!codes.length) return;
    fetchSource_(apiBase, token, source, codes).forEach(function (r) {
      if (!isFinishedGood_(r)) { skipped++; return; }
      rows.push(r);
      var wh = String(r.WhsCode || '?');
      perWh[wh] = (perWh[wh] || 0) + 1;
    });
  });

  rows.sort(function (a, b) {
    var wa = String(a.WhsCode || '');
    var wb = String(b.WhsCode || '');
    if (wa !== wb) return wa < wb ? -1 : 1;
    var na = String(a.ItemName || '');
    var nb = String(b.ItemName || '');
    return na < nb ? -1 : (na > nb ? 1 : 0);
  });
  return { rows: rows, perWh: perWh, skipped: skipped };
}

function num_(v) {
  if (v === null || v === undefined || v === '') return 0;
  var n = Number(v);
  return isNaN(n) ? v : n;
}

function getSpreadsheet_() {
  var active = SpreadsheetApp.getActiveSpreadsheet();
  if (active) return active;                          // bound script (recommended)
  if (CONFIG.sheetId) return SpreadsheetApp.openById(CONFIG.sheetId);
  throw new Error('No spreadsheet found. Open this from your Sheet (Extensions -> ' +
                  'Apps Script) so it is bound, or set CONFIG.sheetId.');
}

// The script OWNS only the columns in COLUMNS (A..E). Anything to the RIGHT of
// them (F, G, ...) is MANUAL: it survives every refresh and is re-attached to
// the SAME item row (matched by Item Code + Warehouse Code), so a note or a
// calculation stays with its item even when the row order changes.
//
// FORMULAS ARE KEPT AS FORMULAS. Manual cells are snapshotted in R1C1 notation
// (=XLOOKUP(RC[-5], ...) instead of =XLOOKUP(A12, ...)), so a formula landing on
// a different row still points at ITS OWN row - and it is written back with
// setFormulasR1C1, never flattened into the value it happened to show.
function writeSheet_(rows) {
  var ss = getSpreadsheet_();
  var sheet = ss.getSheetByName(CONFIG.tabName) || ss.insertSheet(CONFIG.tabName);

  var scriptCols = COLUMNS.length;
  var header = COLUMNS.map(function (c) { return c[1]; });

  // 1) Snapshot the manual columns (values AND formulas) before touching them.
  var lastRow = sheet.getLastRow();
  var lastCol = sheet.getLastColumn();
  var manualWidth = Math.max(0, lastCol - scriptCols);
  var snap = snapshotManual_(sheet, lastRow, lastCol, scriptCols, manualWidth);

  // 2) Build the value matrix (script columns + manual values). Cells that hold
  //    a formula are left blank here and get their formula back in step 4.
  var totalCols = scriptCols + manualWidth;
  var matrix = [header.concat(snap.headerCells.map(function (c) { return c.v; }))];
  var manualPerRow = [];
  rows.forEach(function (r) {
    var saved = snap.byKey[itemKey_(r.ItemCode, r.WhsCode)];
    var cells = [];
    for (var j = 0; j < manualWidth; j++) {
      var cel = (saved && saved[j]) ? saved[j] : { f: '', v: '' };
      // Row is new (or that cell was empty) in a formula column -> seed it.
      if (!cel.f && cel.v === '' && snap.fillDown[j]) cel = { f: snap.fillDown[j], v: '' };
      cells.push(cel);
    }
    manualPerRow.push(cells);
    matrix.push([
      r.ItemCode || '', r.ItemName || '', r.WhsCode || '',
      num_(r.OnHand), num_(r.Available),
    ].concat(cells.map(function (c) { return c.v; })));
  });

  // 3) Write the values. Grow the grid first if the tab is smaller than needed.
  ensureSize_(sheet, matrix.length, totalCols);
  sheet.getRange(1, 1, matrix.length, totalCols).setValues(matrix);

  // 4) Put the formulas back, in R1C1 so each one re-anchors to its new row.
  if (manualWidth > 0) {
    var grid = [snap.headerCells.map(function (c) { return c.f; })];
    manualPerRow.forEach(function (cells) {
      grid.push(cells.map(function (c) { return c.f; }));
    });
    applyFormulas_(sheet, grid, scriptCols);
  }

  // 5) Clear only rows left BELOW the new data. No sheet.clear(), so manual
  //    columns are never wiped - they were rewritten in place above.
  if (lastRow > matrix.length) {
    var width = Math.max(lastCol, totalCols);
    sheet.getRange(matrix.length + 1, 1, lastRow - matrix.length, width).clearContent();
  }
  sheet.setFrozenRows(1);
}

// Formulas whose result can spill down over the cells beneath them. Only the
// OUTERMOST function matters, which is why this is anchored at the start.
var SPILL_RE = /^=\s*(ARRAYFORMULA|QUERY|FILTER|SORT|SORTN|UNIQUE|SPLIT|FLATTEN|TRANSPOSE|SEQUENCE|IMPORTRANGE|IMPORTHTML|IMPORTDATA|IMPORTXML)\s*\(/i;

// Reads the manual region once and returns, per item row, one {f, v} cell per
// manual column: f = the R1C1 formula ('' when the cell is a plain value),
// v = the plain value ('' when the cell is a formula).
function snapshotManual_(sheet, lastRow, lastCol, scriptCols, manualWidth) {
  var blank = { headerCells: [], byKey: {}, fillDown: [] };
  if (manualWidth <= 0 || lastRow < 1) return blank;

  var range = sheet.getRange(1, 1, lastRow, lastCol);
  var values = range.getValues();
  var formulas = range.getFormulasR1C1();   // '' where a cell has no formula

  var headerCells = [];
  var counts = [];        // per manual column: {r1c1 formula -> how many rows}
  var spills = [];        // per manual column: a spilling formula owns it
  for (var c = 0; c < manualWidth; c++) {
    var head = cell_(formulas[0][scriptCols + c], values[0][scriptCols + c]);
    headerCells.push(head);
    counts.push({});
    spills.push(SPILL_RE.test(head.f));
  }

  var byKey = {};
  var dataRows = 0;
  for (var i = 1; i < values.length; i++) {
    if (!String(values[i][0] == null ? '' : values[i][0]).trim()) continue;  // no Item Code
    dataRows++;
    var cells = [];
    var hasData = false;
    for (var j = 0; j < manualWidth; j++) {
      var cel = cell_(formulas[i][scriptCols + j], values[i][scriptCols + j]);
      // Below a spilling formula (ARRAYFORMULA/QUERY/...) the cells hold its
      // OUTPUT, not typed data. Keeping them would block the spill with a
      // #REF!, so drop them and let the one anchor formula refill the column.
      if (spills[j] && !cel.f) cel = { f: '', v: '' };
      if (cel.f && SPILL_RE.test(cel.f)) spills[j] = true;
      if (cel.f) counts[j][cel.f] = (counts[j][cel.f] || 0) + 1;
      if (cel.f || cel.v !== '') hasData = true;
      cells.push(cel);
    }
    if (hasData) byKey[itemKey_(values[i][0], values[i][2])] = cells;
  }

  // A manual column counts as a "formula column" when its most common formula
  // covers at least half of the old data rows. That formula seeds rows that are
  // new in this refresh (safe because it is stored relative, in R1C1).
  var fillDown = [];
  for (var k = 0; k < manualWidth; k++) {
    var best = '', bestN = 0;
    for (var f in counts[k]) {
      if (counts[k][f] > bestN) { best = f; bestN = counts[k][f]; }
    }
    var isFormulaColumn = CONFIG.fillDownFormulas && !spills[k] &&
                          dataRows > 0 && bestN * 2 >= dataRows;
    fillDown.push(isFormulaColumn ? best : '');
  }
  return { headerCells: headerCells, byKey: byKey, fillDown: fillDown };
}

function cell_(formulaR1C1, value) {
  var f = String(formulaR1C1 == null ? '' : formulaR1C1);
  return { f: f, v: f ? '' : (value == null ? '' : value) };
}

// Writes the manual formulas back, one setFormulasR1C1 call per run of
// consecutive formula cells in a column (normally one call per column), so the
// plain values written by setValues in between are left untouched.
function applyFormulas_(sheet, grid, scriptCols) {
  var width = grid.length ? grid[0].length : 0;
  for (var j = 0; j < width; j++) {
    var run = null;
    for (var i = 0; i <= grid.length; i++) {
      var f = i < grid.length ? (grid[i][j] || '') : '';
      if (f) {
        if (!run) run = { start: i, items: [] };
        run.items.push([f]);
      } else if (run) {
        sheet.getRange(run.start + 1, scriptCols + j + 1, run.items.length, 1)
             .setFormulasR1C1(run.items);
        run = null;
      }
    }
  }
}

// Grow the tab so a setValues of rows x cols always fits.
function ensureSize_(sheet, rows, cols) {
  var maxRows = sheet.getMaxRows();
  if (rows > maxRows) sheet.insertRowsAfter(maxRows, rows - maxRows);
  var maxCols = sheet.getMaxColumns();
  if (cols > maxCols) sheet.insertColumnsAfter(maxCols, cols - maxCols);
}

// Stable identity for a row: Item Code + Warehouse Code (unique per inventory row).
function itemKey_(itemCode, whsCode) {
  return String(itemCode == null ? '' : itemCode).trim() + '||' +
         String(whsCode == null ? '' : whsCode).trim();
}

function logCounts_(perWh, total, skipped) {
  var lines = Object.keys(perWh).sort().map(function (k) { return '  ' + k + ': ' + perWh[k]; });
  Logger.log('Rows per warehouse:\n' + lines.join('\n') +
             '\n  Total written: ' + total +
             '\n  Skipped (not ' + (CONFIG.itemCodePrefix || 'any') + '* / not ' +
             (CONFIG.itemGroupName || 'any group') + '): ' + skipped);
}

/** Adds a "JM Inventory -> Refresh now" menu on the Sheet. */
function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu('JM Inventory')
    .addItem('Refresh now', 'refreshJmInventory')
    .addToUi();
}

/**
 * Run this ONCE to auto-refresh every day at ~7 AM IST. Re-running adds another
 * trigger, so only do it once (manage/remove under the clock icon -> Triggers).
 */
function scheduleDailyRefresh() {
  ScriptApp.newTrigger('refreshJmInventory')
    .timeBased()
    .atHour(7)
    .everyDays(1)
    .inTimezone('Asia/Kolkata')
    .create();
}
