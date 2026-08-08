/**
 * RK WORLD sales  ->  this Google Sheet   (Google Apps Script - runs on
 * Google's servers, no PC / no Python needed).
 *
 * Pulls the JM Primary sales register (SAP HANA REPORT_SALES_ANALYSIS, served
 * by the Ecom app) for ONE customer - R K WORLDINFOCOM PVT LTD, CardCode
 * CUSTA000048 - out of ONE company book - JIVO MART (source=mart, schema
 * JIVO_MART_HANADB) - and writes it into a tab of the Sheet. Nothing else is
 * fetched: every other customer is dropped, and the JIVO OIL book is not read
 * at all.
 *
 * DATE WINDOW
 *   from_date = FIXED at CONFIG.fromDate ('2026-07-01'). It never moves on its
 *               own - edit that one line if you ever want a different start.
 *   to_date   = TODAY + CONFIG.toDateOffsetDays (default 1 = tomorrow), so the
 *               window keeps growing by itself as new days arrive and a
 *               document dated "next day" is already covered.
 *
 * Your own columns to the RIGHT of the script columns are preserved on every
 * refresh, INCLUDING formulas (they are re-written as formulas, not flattened
 * to their last value). See writeSheet_ for the details.
 *
 * -- SETUP (one time) ------------------------------------------------------
 * 1. Open your Google Sheet -> Extensions -> Apps Script. Paste this whole file
 *    into Code.gs (replacing what's there) and Save.  (Opening it from the
 *    Sheet "binds" the script to that Sheet, so it can write to it and travels
 *    with the Sheet when you share/hand it over.)
 * 2. Give it your Ecom login, EITHER of these two ways:
 *    (a) Left sidebar -> Project Settings (gear) -> Script Properties -> Add
 *        two properties, named EXACTLY:
 *              ECOM_EMAIL     = your Ecom login email
 *              ECOM_PASSWORD  = your Ecom password
 *        The NAME is 'ECOM_EMAIL'; your email is the VALUE next to it. This
 *        keeps the password out of the code.
 *    (b) Or just type them into CONFIG.email / CONFIG.password below. Easier,
 *        but anyone who can open this script can read the password.
 * 3. Back in the editor pick the function "refreshRkWorld" -> Run.
 *    Approve the one-time permission prompt (it needs to call the Ecom API and
 *    edit the Sheet).  Done - the tab fills with data.
 *
 * To refresh later: use the "RK World -> Refresh now" menu that appears on the
 * Sheet, or set a trigger (see scheduleDailyRefresh below).
 *
 * Not sure which fields the SAP procedure returns? Run logAvailableColumns
 * once and read the execution log - it prints every column name in the live
 * data, which is exactly what you put in the COLUMNS list below.
 */

var CONFIG = {
  // Deployed Ecom app - must be reachable from the internet (Google's servers
  // call it). Do NOT use localhost here; that only exists on your own PC.
  apiBase: 'https://ecom.jivo.in',

  // ---- Ecom login ---------------------------------------------------------
  // Two ways to supply it, in this order of preference:
  //   1. Project Settings (gear) -> Script Properties -> add ECOM_EMAIL and
  //      ECOM_PASSWORD. Keeps the password out of the code. Leave the two
  //      lines below blank in that case.
  //   2. Just type them here. Simpler, but anyone who can open this script
  //      can read the password.
  email: '',
  password: '',

  // ---- The date window ----------------------------------------------------
  // FIXED start. Change this line (and only this line) to move the start.
  fromDate: '2026-07-01',
  // Rolling end: today + N days, recomputed on every run. 1 = include tomorrow
  // ("the next day"); use 0 if you only ever want up to today.
  toDateOffsetDays: 1,
  // Clock used to decide what "today" is, and to stamp the log.
  timezone: 'Asia/Kolkata',

  // ---- The one customer we want ------------------------------------------
  // cardCode is the authoritative match. cardNameContains is a safety net in
  // case a row comes back without a CardCode - letters/digits only, compared
  // case-insensitively, so spacing and punctuation don't matter.
  customer: {
    cardCode: 'CUSTA000048',
    cardNameContains: 'WORLDINFOCOM',
    label: 'R K WORLDINFOCOM PVT LTD',
  },

  // SAP company book to read. JIVO MART only (schema JIVO_MART_HANADB) - the
  // oil book is deliberately NOT fetched. One API call reads ONE book, so
  // adding 'oil' back here would simply fetch it too and merge the rows.
  sources: ['mart'],

  tabName: 'RK World',

  // Long ranges are fetched one calendar month at a time. Keeps each API call
  // small and quick (Apps Script gives up on very slow responses), and lets the
  // backend cache each finished month.
  chunkByMonth: true,

  // Rows per API page. The backend caps this at 100000.
  pageSize: 100000,

  // Safety: if a run comes back with ZERO rows (VPN to HANA down, wrong card
  // code, ...) keep the sheet as it is instead of blanking it.
  abortOnEmpty: true,

  // When a manual column is a formula column (one formula covers at least half
  // of the rows), copy that formula onto rows that are new in this refresh, so
  // the column stays filled without dragging it down by hand.
  fillDownFormulas: true,

  // Only needed if you keep this as a STANDALONE script (not opened from the
  // Sheet). Leave blank when bound via Extensions -> Apps Script.
  sheetId: '',
};

// [field in the API row, header written to the sheet, type].
// type: 'date' -> real date cell | 'number' -> real number cell | '' -> text.
// '__source' is added by this script (mart / oil); everything else comes
// straight from the SAP procedure. Add or remove lines freely - the script
// columns are simply "the first COLUMNS.length columns of the tab".
var COLUMNS = [
  ['DocDate',       'Date',       'date'],
  ['ItemCode',      'Item Code',  ''],
  ['ItemName',      'Item Name',  ''],
  ['SKU',           'SKU',        ''],
  ['U_TYPE',        'Item Head',  ''],
  ['U_Sub_Group',   'Sub Group',  ''],
  ['Quantity',      'Quantity',   'number'],
  ['Liter',         'Liter',      'number'],
  ['LineTotal',     'Line Total', 'number'],
  ['__source',      'Source',     ''],
];

// Which fields identify a row, so a note you typed in a manual column comes
// back onto the SAME sale after the next refresh. Repeats of the same
// (date, item) are told apart by their position within that group.
var KEY_FIELDS = ['DocDate', 'ItemCode'];

// Where the login comes from: Script Properties first, then the CONFIG lines.
// NOTE getScriptProperties().getProperty() takes the property NAME
// ('ECOM_EMAIL') - never the value. Passing your actual email address there
// just looks up a property that doesn't exist and returns null.
function credentials_() {
  var props = PropertiesService.getScriptProperties();
  var email = props.getProperty('ECOM_EMAIL') || CONFIG.email || '';
  var password = props.getProperty('ECOM_PASSWORD') || CONFIG.password || '';
  if (!email || !password) {
    throw new Error(
      'No Ecom login found. Either (a) fill in CONFIG.email and CONFIG.password ' +
      'at the top of this file, or (b) add properties named exactly ECOM_EMAIL ' +
      'and ECOM_PASSWORD under Project Settings -> Script Properties.'
    );
  }
  return {
    email: email,
    password: password,
    apiBase: (props.getProperty('ECOM_API_BASE') || CONFIG.apiBase).replace(/\/+$/, ''),
  };
}

/** Main entry point - pull live data and rewrite the tab. */
function refreshRkWorld() {
  var creds = credentials_();
  var apiBase = creds.apiBase;

  var fromDate = CONFIG.fromDate;
  var toDate = rollingToDate_();
  var token = login_(apiBase, creds.email, creds.password);
  var result = fetchAll_(apiBase, token, fromDate, toDate);

  if (!result.rows.length && CONFIG.abortOnEmpty) {
    throw new Error('No ' + CONFIG.customer.label + ' rows for ' + fromDate + ' .. ' + toDate +
                    ' (' + result.fetched + ' rows scanned). Sheet left untouched. ' +
                    'Check the VPN/HANA link and CONFIG.customer.cardCode.');
  }
  writeSheet_(result.rows);
  logRun_(result, fromDate, toDate);
}

/** Today + CONFIG.toDateOffsetDays, in CONFIG.timezone, as YYYY-MM-DD. */
function rollingToDate_() {
  var shifted = new Date(Date.now() + (CONFIG.toDateOffsetDays || 0) * 86400000);
  return Utilities.formatDate(shifted, CONFIG.timezone, 'yyyy-MM-dd');
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

// One page of the sales register for one book and one date range. `search` is
// the card code, so the BACKEND already drops every other customer - only RK
// World's rows travel over the wire.
function fetchPage_(apiBase, token, source, fromDate, toDate, page) {
  var needle = CONFIG.customer.cardCode || CONFIG.customer.cardNameContains || '';
  if (!needle) throw new Error('Set CONFIG.customer.cardCode (or cardNameContains).');
  var params = [
    'from_date=' + encodeURIComponent(fromDate),
    'to_date=' + encodeURIComponent(toDate),
    'source=' + encodeURIComponent(source),
    'search=' + encodeURIComponent(needle),
    'page=' + page,
    'page_size=' + CONFIG.pageSize,
    'nocache=1',   // skip the response cache
    'fresh=1',     // skip the 120s procedure cache - read HANA now
  ];
  var resp = UrlFetchApp.fetch(apiBase + '/api/sap/sales-analysis?' + params.join('&'), {
    method: 'get',
    headers: { Authorization: 'Bearer ' + token },
    muteHttpExceptions: true,
  });
  var code = resp.getResponseCode();
  if (code === 403) throw new Error("This account lacks the 'sap.view' permission needed to read JM Primary sales.");
  if (code === 503) throw new Error('SAP HANA is unreachable right now (VPN / network / host down). ' +
                                    resp.getContentText().slice(0, 200));
  if (code < 200 || code >= 300) {
    throw new Error('Sales request failed (source=' + source + ', ' + fromDate + '..' + toDate +
                    '): HTTP ' + code + ' ' + resp.getContentText().slice(0, 300));
  }
  return JSON.parse(resp.getContentText());
}

// All pages for one book and one date range.
function fetchRange_(apiBase, token, source, fromDate, toDate) {
  var out = [];
  for (var page = 0; page < 50; page++) {
    var json = fetchPage_(apiBase, token, source, fromDate, toDate, page);
    var data = json.data || [];
    out = out.concat(data);
    var count = Number(json.count || 0);
    if (!data.length || out.length >= count) break;
  }
  return out;
}

// Split [fromISO, toISO] on calendar-month boundaries: ['2026-07-01'..'2026-07-31',
// '2026-08-01'..'2026-08-09', ...]. Finished months are stable, so only the
// current month's chunk really changes between runs.
function monthChunks_(fromISO, toISO) {
  var f = ymd_(fromISO);
  var t = ymd_(toISO);
  if (f.y > t.y || (f.y === t.y && (f.m > t.m || (f.m === t.m && f.d > t.d)))) {
    throw new Error('CONFIG.fromDate (' + fromISO + ') is after the end date (' + toISO + ').');
  }
  if (!CONFIG.chunkByMonth) return [{ from: fromISO, to: toISO }];

  var chunks = [];
  var y = f.y, m = f.m, d = f.d;
  while (chunks.length < 240) {
    var last = new Date(Date.UTC(y, m, 0)).getUTCDate();   // last day of month m (1-based)
    var isLast = (y === t.y && m === t.m);
    chunks.push({ from: iso_(y, m, d), to: iso_(y, m, isLast ? t.d : last) });
    if (isLast) break;
    m++; if (m > 12) { m = 1; y++; }
    d = 1;
  }
  return chunks;
}

function ymd_(isoDate) {
  var p = String(isoDate).split('-');
  return { y: Number(p[0]), m: Number(p[1]), d: Number(p[2]) };
}

function iso_(y, m, d) {
  return y + '-' + (m < 10 ? '0' + m : m) + '-' + (d < 10 ? '0' + d : d);
}

// Case-insensitive field read: SAP/HANA column casing varies between
// procedures, so 'DocDate', 'DOCDATE' and 'docdate' all resolve.
function val_(row, field) {
  if (row == null) return null;
  if (field in row) return row[field];
  var wanted = String(field).toLowerCase();
  for (var k in row) {
    if (String(k).toLowerCase() === wanted) return row[k];
  }
  return null;
}

// Keep ONLY RK World. The backend already filtered on the card code; this is
// the belt-and-braces check so a loose text match can never leak another
// customer into the sheet.
function isTargetCustomer_(row) {
  var wantCode = String(CONFIG.customer.cardCode || '').trim().toUpperCase();
  if (wantCode) {
    var code = String(val_(row, 'CardCode') == null ? '' : val_(row, 'CardCode')).trim().toUpperCase();
    if (code === wantCode) return true;
  }
  var needle = String(CONFIG.customer.cardNameContains || '').toUpperCase().replace(/[^A-Z0-9]/g, '');
  if (!needle) return false;
  var name = String(val_(row, 'CardName') == null ? '' : val_(row, 'CardName'))
    .toUpperCase().replace(/[^A-Z0-9]/g, '');
  return name.indexOf(needle) >= 0;
}

function fetchAll_(apiBase, token, fromDate, toDate) {
  var chunks = monthChunks_(fromDate, toDate);
  var rows = [];
  var perSource = {};
  var fetched = 0;
  (CONFIG.sources || []).forEach(function (s) { perSource[s] = 0; });

  (CONFIG.sources || []).forEach(function (source) {
    chunks.forEach(function (chunk) {
      fetchRange_(apiBase, token, source, chunk.from, chunk.to).forEach(function (r) {
        fetched++;
        if (!isTargetCustomer_(r)) return;
        r.__source = source;
        rows.push(r);
        perSource[source] = (perSource[source] || 0) + 1;
      });
    });
  });

  // Deterministic order: date, then item name, then everything else - so the
  // same data always lands on the same row and your manual notes stay put.
  rows.sort(function (a, b) {
    return cmp_(sortKey_(a), sortKey_(b));
  });
  return { rows: rows, perSource: perSource, fetched: fetched, chunks: chunks.length };
}

function sortKey_(row) {
  var parts = [
    String(val_(row, 'DocDate') || ''),
    String(val_(row, 'ItemName') || ''),
    String(val_(row, 'ItemCode') || ''),
  ];
  COLUMNS.forEach(function (c) { parts.push(String(val_(row, c[0]) == null ? '' : val_(row, c[0]))); });
  return parts.join('');
}

function cmp_(a, b) { return a < b ? -1 : (a > b ? 1 : 0); }

function num_(v) {
  if (v === null || v === undefined || v === '') return '';
  var n = Number(v);
  return isNaN(n) ? v : n;
}

// 'YYYY-MM-DD' / 'YYYY-MM-DDTHH:MM:SS' / 'YYYYMMDD' -> a real date cell, so the
// column sorts, filters and formats as a date in the Sheet. Anything else is
// written through unchanged.
function date_(v) {
  if (v === null || v === undefined || v === '') return '';
  if (Object.prototype.toString.call(v) === '[object Date]') return v;
  var s = String(v).trim();
  var m = s.match(/^(\d{4})-(\d{2})-(\d{2})/) || s.match(/^(\d{4})(\d{2})(\d{2})$/);
  if (!m) return s;
  return new Date(Number(m[1]), Number(m[2]) - 1, Number(m[3]));
}

function cellValue_(row, field, type) {
  var raw = val_(row, field);
  if (type === 'date') return date_(raw);
  if (type === 'number') return num_(raw);
  return raw == null ? '' : raw;
}

function getSpreadsheet_() {
  var active = SpreadsheetApp.getActiveSpreadsheet();
  if (active) return active;                          // bound script (recommended)
  if (CONFIG.sheetId) return SpreadsheetApp.openById(CONFIG.sheetId);
  throw new Error('No spreadsheet found. Open this from your Sheet (Extensions -> ' +
                  'Apps Script) so it is bound, or set CONFIG.sheetId.');
}

// ---------------------------------------------------------------------------
// Row identity. The script OWNS only the columns in COLUMNS (A..J by default).
// Anything to the RIGHT of them is MANUAL: it survives every refresh and is
// re-attached to the SAME sale, matched on KEY_FIELDS plus how many earlier
// rows share that key.
// ---------------------------------------------------------------------------

function keyPart_(v) {
  if (v === null || v === undefined) return '';
  if (Object.prototype.toString.call(v) === '[object Date]') {
    return Utilities.formatDate(v, CONFIG.timezone, 'yyyy-MM-dd');
  }
  return String(v).trim();
}

// Key of an API row (before it becomes a sheet row).
function rowKey_(row) {
  return KEY_FIELDS.map(function (f) {
    var col = null;
    for (var i = 0; i < COLUMNS.length; i++) if (COLUMNS[i][0] === f) col = COLUMNS[i];
    return keyPart_(col ? cellValue_(row, f, col[2]) : val_(row, f));
  }).join('||');
}

// Key of a row already sitting in the sheet (values as the Sheet returns them).
function sheetRowKey_(values, offset) {
  return KEY_FIELDS.map(function (f) {
    var idx = -1;
    for (var i = 0; i < COLUMNS.length; i++) if (COLUMNS[i][0] === f) idx = i;
    return idx < 0 ? '' : keyPart_(values[offset + idx]);
  }).join('||');
}

// Same key twice in a row set? Number them, so line 2 of a repeat keeps ITS note.
function withOccurrence_(key, seen) {
  var n = (seen[key] || 0) + 1;
  seen[key] = n;
  return key + '#' + n;
}

// FORMULAS ARE KEPT AS FORMULAS. Manual cells are snapshotted in R1C1 notation
// (=XLOOKUP(RC[-9], ...) instead of =XLOOKUP(B12, ...)), so a formula landing on
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
  var seen = {};
  rows.forEach(function (r) {
    var saved = snap.byKey[withOccurrence_(rowKey_(r), seen)];
    var cells = [];
    for (var j = 0; j < manualWidth; j++) {
      var cel = (saved && saved[j]) ? saved[j] : { f: '', v: '' };
      // Row is new (or that cell was empty) in a formula column -> seed it.
      if (!cel.f && cel.v === '' && snap.fillDown[j]) cel = { f: snap.fillDown[j], v: '' };
      cells.push(cel);
    }
    manualPerRow.push(cells);
    matrix.push(
      COLUMNS.map(function (c) { return cellValue_(r, c[0], c[2]); })
             .concat(cells.map(function (c) { return c.v; }))
    );
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
  formatColumns_(sheet, matrix.length);
  sheet.setFrozenRows(1);
}

// Date columns as dd-MMM-yyyy, number columns with thousands separators.
function formatColumns_(sheet, rowCount) {
  if (rowCount < 2) return;
  COLUMNS.forEach(function (c, i) {
    if (c[2] === 'date') {
      sheet.getRange(2, i + 1, rowCount - 1, 1).setNumberFormat('dd-MMM-yyyy');
    } else if (c[2] === 'number') {
      sheet.getRange(2, i + 1, rowCount - 1, 1).setNumberFormat('#,##0.00');
    }
  });
}

// Formulas whose result can spill down over the cells beneath them. Only the
// OUTERMOST function matters, which is why this is anchored at the start.
var SPILL_RE = /^=\s*(ARRAYFORMULA|QUERY|FILTER|SORT|SORTN|UNIQUE|SPLIT|FLATTEN|TRANSPOSE|SEQUENCE|IMPORTRANGE|IMPORTHTML|IMPORTDATA|IMPORTXML)\s*\(/i;

// Reads the manual region once and returns, per data row, one {f, v} cell per
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
  var seen = {};
  for (var i = 1; i < values.length; i++) {
    if (!hasScriptData_(values[i], scriptCols)) continue;   // blank / spacer row
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
    var key = withOccurrence_(sheetRowKey_(values[i], 0), seen);
    if (hasData) byKey[key] = cells;
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

function hasScriptData_(rowValues, scriptCols) {
  for (var i = 0; i < scriptCols; i++) {
    if (String(rowValues[i] == null ? '' : rowValues[i]).trim() !== '') return true;
  }
  return false;
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

function logRun_(result, fromDate, toDate) {
  var qty = 0, ltr = 0, amt = 0;
  result.rows.forEach(function (r) {
    qty += Number(num_(val_(r, 'Quantity')) || 0);
    ltr += Number(num_(val_(r, 'Liter')) || 0);
    amt += Number(num_(val_(r, 'LineTotal')) || 0);
  });
  var lines = Object.keys(result.perSource).sort().map(function (k) {
    return '  ' + k + ': ' + result.perSource[k] + ' rows';
  });
  Logger.log(
    CONFIG.customer.label + '  ' + fromDate + ' .. ' + toDate +
    ' (' + result.chunks + ' month chunk(s))\n' + lines.join('\n') +
    '\n  Rows written : ' + result.rows.length +
    '\n  Rows scanned : ' + result.fetched +
    '\n  Quantity     : ' + qty.toFixed(2) +
    '\n  Liter        : ' + ltr.toFixed(2) +
    '\n  Line Total   : ' + amt.toFixed(2) +
    '\n  Refreshed at : ' + Utilities.formatDate(new Date(), CONFIG.timezone, 'yyyy-MM-dd HH:mm') + ' IST'
  );
}

/**
 * Diagnostic: prints every column name the SAP procedure actually returns for
 * RK World (plus one sample row), so you can put the right field names in
 * COLUMNS. Reads a single day to stay fast.
 */
function logAvailableColumns() {
  var creds = credentials_();
  var apiBase = creds.apiBase;
  var token = login_(apiBase, creds.email, creds.password);
  var chunks = monthChunks_(CONFIG.fromDate, rollingToDate_());
  for (var s = 0; s < (CONFIG.sources || []).length; s++) {
    for (var c = 0; c < chunks.length; c++) {
      var rows = fetchRange_(apiBase, token, CONFIG.sources[s], chunks[c].from, chunks[c].to);
      for (var i = 0; i < rows.length; i++) {
        if (!isTargetCustomer_(rows[i])) continue;
        Logger.log('Columns (' + CONFIG.sources[s] + '):\n  ' + Object.keys(rows[i]).join('\n  '));
        Logger.log('Sample row:\n' + JSON.stringify(rows[i], null, 2));
        return;
      }
    }
  }
  Logger.log('No ' + CONFIG.customer.label + ' rows found in ' + CONFIG.fromDate + ' .. ' + rollingToDate_());
}

/** Adds a "RK World -> Refresh now" menu on the Sheet. */
function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu('RK World')
    .addItem('Refresh now', 'refreshRkWorld')
    .addItem('Show available columns (log)', 'logAvailableColumns')
    .addToUi();
}

/**
 * Run this ONCE to auto-refresh every day at ~7 AM IST. Re-running adds another
 * trigger, so only do it once (manage/remove under the clock icon -> Triggers).
 */
function scheduleDailyRefresh() {
  ScriptApp.newTrigger('refreshRkWorld')
    .timeBased()
    .atHour(7)
    .everyDays(1)
    .inTimezone('Asia/Kolkata')
    .create();
}
