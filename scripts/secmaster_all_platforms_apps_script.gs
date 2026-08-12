/*************************************************************************
 * SecMaster  →  Google Sheet   (via the deployed backend API)
 * ALL-PLATFORM EDITION — SWIGGY / BLINKIT / ZEPTO / BIG BASKET (tick-boxes).
 * The raw DB port is firewalled to Google, so we ask the backend
 * (ecom.jivo.in) for the rows instead. Same view/columns/filters.
 *
 * RUN ORDER: setupCredentials -> testConnection -> setupSheet -> menu.
 *
 * AGGREGATED OUTPUT:
 *   Rows are grouped by GROUP_COLS (format, month, year, item_head,
 *   category, sub_category, item). Per group we show the SUM of Ltr Sold
 *   and the SUM of Unit Sold; Max Date is FORMAT-WIDE (every row of a
 *   format shows that format's latest date). (city / sku_code /
 *   per_ltr_unit are not pulled.)
 *   To collapse purely to category + sub_category, delete the columns
 *   you don't want to group on from COLUMNS below — GROUP_COLS follows
 *   COLUMNS automatically.
 *************************************************************************/

/* ─── 1. CONFIG ──────────────────────────────────────────────────── */
/* NOTE: the units column on the SecMaster view is `quantity`, NOT
   `qty_sold` / `units_sold`. `qty_sold` only exists inside the view's inner
   per-platform subqueries and is aliased to `quantity` in the outer SELECT —
   asking for it makes Postgres throw and the API replies {"error": ...}. */
const COLUMNS = [
  ['format',       'FORMAT'],       ['date',         'Max Date'],
  ['month',        'Month'],        ['year',         'Year'],
  ['item_head',    'Item Head'],    ['category',     'Category'],
  ['sub_category', 'Sub Category'], ['item',         'Item'],
  ['ltr_sold',     'Ltr Sold (Sum)'],
  ['quantity',     'Unit Sold (Sum)'],
];
const NUMERIC_COLS = ['ltr_sold', 'quantity'];

/* Aggregation config — derived from COLUMNS so it stays in sync. */
const DATE_COL   = 'date';                     // aggregated as MAX
const SUM_COLS   = ['ltr_sold', 'quantity'];   // aggregated as SUM
const GROUP_COLS = COLUMNS.map(c => c[0])
                          .filter(k => k !== DATE_COL && SUM_COLS.indexOf(k) === -1);

const PLATFORMS    = ['SWIGGY', 'BLINKIT', 'ZEPTO', 'BIG BASKET'];
const MONTHS_UPPER = ['JANUARY','FEBRUARY','MARCH','APRIL','MAY','JUNE',
                      'JULY','AUGUST','SEPTEMBER','OCTOBER','NOVEMBER','DECEMBER'];

/* ─── 2. CREDENTIALS (your ECMS login, not the DB password) ──────── */
function setupCredentials() {
  PropertiesService.getScriptProperties().setProperties({
    API_BASE:       'https://ecom.jivo.in',
    LOGIN_EMAIL:    'ecom@jivo.in',   // <-- TODO your ECMS login email
    LOGIN_PASSWORD: '12345'           // <-- TODO your ECMS password
  });
  Logger.log('✅ Saved. Fill LOGIN_EMAIL / LOGIN_PASSWORD, then run testConnection.');
}

/* ─── 3. API HELPERS ─────────────────────────────────────────────── */
function apiBase_() {
  const b = PropertiesService.getScriptProperties().getProperty('API_BASE') || '';
  return b.replace(/\/+$/, '');
}
function getToken_() {
  const p = PropertiesService.getScriptProperties();
  const email = p.getProperty('LOGIN_EMAIL'), pass = p.getProperty('LOGIN_PASSWORD');
  if (!apiBase_() || !email || !pass) throw new Error('Missing API creds — run setupCredentials()');
  const res = UrlFetchApp.fetch(apiBase_() + '/api/auth/login', {
    method: 'post', contentType: 'application/json',
    payload: JSON.stringify({ email: email, password: pass }), muteHttpExceptions: true
  });
  const code = res.getResponseCode(), body = res.getContentText();
  if (code !== 200) throw new Error('Login failed (HTTP ' + code + '): ' + body);
  const token = JSON.parse(body).token;
  if (!token) throw new Error('Login returned no token: ' + body);
  return token;
}
function fetchReportPage_(token, formatsCsv, dateFrom, dateTo, page, pageSize) {
  const cols = COLUMNS.map(c => c[0]).join(',');
  const url = apiBase_() + '/api/reports/raw?view=SecMaster'
    + '&columns='   + encodeURIComponent(cols)
    + '&platform='  + encodeURIComponent(formatsCsv)
    + '&date_from=' + encodeURIComponent(dateFrom || '')
    + '&date_to='   + encodeURIComponent(dateTo || '')
    + '&page=' + page + '&page_size=' + pageSize;
  const res = UrlFetchApp.fetch(url, {
    method: 'get', headers: { Authorization: 'Bearer ' + token }, muteHttpExceptions: true
  });
  const code = res.getResponseCode(), body = res.getContentText();
  if (code !== 200) throw new Error('Reports HTTP ' + code + ': ' + body);
  const data = JSON.parse(body);
  if (data.error) throw new Error('API error: ' + data.error);
  return data;
}

/* ─── 3b. AGGREGATION HELPERS ────────────────────────────────────── */
function aggKey_(obj) {
  return GROUP_COLS.map(function (k) { return String(obj[k] == null ? '' : obj[k]); }).join('');
}
function parseDate_(v) {
  if (v == null || v === '') return null;
  const d = new Date(v);
  return isNaN(d.getTime()) ? null : d.getTime();
}
// Fold a page of raw rows into the running aggregate map (mutates map).
// e.s is an ARRAY of running totals, one slot per SUM_COLS entry
// (0 = ltr_sold, 1 = quantity/Unit Sold).
function accumulate_(map, rows) {
  for (var i = 0; i < rows.length; i++) {
    var o = rows[i], k = aggKey_(o), e = map[k];
    if (!e) {
      e = { g: GROUP_COLS.map(function (c) { return o[c] == null ? '' : o[c]; }),
            d: '', t: -Infinity,
            s: SUM_COLS.map(function () { return 0; }) };
      map[k] = e;
    }
    for (var j = 0; j < SUM_COLS.length; j++) {
      var n = Number(o[SUM_COLS[j]]); if (!isNaN(n)) e.s[j] += n;
    }
    var dt = parseDate_(o[DATE_COL]);
    if (dt !== null && dt > e.t) { e.t = dt; e.d = o[DATE_COL]; }
    else if (dt === null && e.d === '') { e.d = o[DATE_COL] == null ? '' : o[DATE_COL]; }
  }
}
// Turn the aggregate map into sheet rows (COLUMNS order), sorted by category, sub_category, item.
// Max Date is format-wide: every row of a format shows that format's latest date,
// not the latest date within its own category/sub-category group.
function aggToRows_(map) {
  var keys = Object.keys(map);
  var fmtIdx = GROUP_COLS.indexOf('format');
  var fmtMax = {};
  keys.forEach(function (k) {
    var e = map[k], f = String(e.g[fmtIdx]);
    if (!(f in fmtMax) || e.t > fmtMax[f].t) fmtMax[f] = { t: e.t, d: e.d };
  });
  var rows = keys.map(function (k) {
    var e = map[k], byKey = {};
    for (var i = 0; i < GROUP_COLS.length; i++) byKey[GROUP_COLS[i]] = e.g[i];
    var fm = fmtMax[String(e.g[fmtIdx])];
    byKey[DATE_COL] = fm ? fm.d : e.d;
    for (var j = 0; j < SUM_COLS.length; j++) byKey[SUM_COLS[j]] = e.s[j];
    return COLUMNS.map(function (c) {
      var v = byKey[c[0]];
      if (NUMERIC_COLS.indexOf(c[0]) !== -1) { var n = Number(v); return isNaN(n) ? 0 : n; }
      return v == null ? '' : v;
    });
  });
  var colKeys = COLUMNS.map(function (c) { return c[0]; });
  var catIdx  = colKeys.indexOf('category');
  var subIdx  = colKeys.indexOf('sub_category');
  var itemIdx = colKeys.indexOf('item');
  rows.sort(function (a, b) {
    var x = String(a[catIdx]).localeCompare(String(b[catIdx]));
    if (x !== 0) return x;
    var y = String(a[subIdx]).localeCompare(String(b[subIdx]));
    if (y !== 0) return y;
    return itemIdx === -1 ? 0 : String(a[itemIdx]).localeCompare(String(b[itemIdx]));
  });
  return rows;
}

/* ─── 4. DIAGNOSTIC ──────────────────────────────────────────────── */
function testConnection() {
  let msg;
  try {
    const token = getToken_();
    const data = fetchReportPage_(token, PLATFORMS.join(','), '', '', 0, 1);
    msg = '✅ CONNECTED. SecMaster reachable — ' + data.count + ' total rows for all formats.';
  } catch (e) { msg = '❌ FAILED:\n' + e.message; }
  Logger.log(msg); alert_(msg);
}

/* ─── 5. MENU ────────────────────────────────────────────────────── */
function onOpen2() {
  SpreadsheetApp.getUi().createMenu('⚡ SecMaster')
    .addItem('🔄 Refresh Data', 'fetchData')
    .addItem('⚙️ Setup Sheet', 'setupSheet')
    .addItem('🔌 Test Connection', 'testConnection')
    .addItem('🛑 Cancel Job', 'cancelJob')
    .addToUi();
}

/* ─── 6. CONTROL SHEET ───────────────────────────────────────────── */
function DB_() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  return ss.getSheetByName('SEC DATA') || ss.insertSheet('SEC DATA');
}
// Grow a sheet so a setValues() of this size cannot fall off the grid. A new
// sheet is 1000x26; the aggregate can be wider/taller than that.
function ensureSize_(sheet, rows, cols) {
  const r = sheet.getMaxRows(), c = sheet.getMaxColumns();
  if (rows > r) sheet.insertRowsAfter(r, rows - r);
  if (cols > c) sheet.insertColumnsAfter(c, cols - c);
}
// Non-blocking notice. NEVER use SpreadsheetApp.getUi().alert() here — a modal
// alert() blocks (waiting for an "OK" click) when run from the Apps Script
// editor instead of the sheet, hanging until the 6-min timeout. toast() is
// fire-and-forget, so it's safe from both the editor and the menu.
function alert_(m) {
  Logger.log(m);
  try { SpreadsheetApp.getActiveSpreadsheet().toast(String(m).slice(0, 250), 'SecMaster', 5); } catch (x) {}
}

function setupSheet() {
  const sheet = DB_(); sheet.clear();
  ensureSize_(sheet, 20, 2 + COLUMNS.length);
  const monthNames = ['January','February','March','April','May','June',
                      'July','August','September','October','November','December'];
  const now = new Date(), yr = now.getFullYear();
  const years = [yr - 2, yr - 1, yr, yr + 1].map(String);
  ['From Month','From Year','To Month','To Year'].forEach((l, i) =>
    sheet.getRange(i * 2 + 1, 1).setValue(l).setFontWeight('bold').setBackground('#f3f3f3'));
  sheet.getRange('A2').setValue(monthNames[now.getMonth()]).setBackground('#e8f5e9');
  sheet.getRange('A4').setValue(yr).setBackground('#e8f5e9');
  sheet.getRange('A6').setValue(monthNames[now.getMonth()]).setBackground('#e8f5e9');
  sheet.getRange('A8').setValue(yr).setBackground('#e8f5e9');
  const mRule = SpreadsheetApp.newDataValidation().requireValueInList(monthNames, true).setAllowInvalid(false).build();
  const yRule = SpreadsheetApp.newDataValidation().requireValueInList(years, true).setAllowInvalid(false).build();
  sheet.getRange('A2').setDataValidation(mRule); sheet.getRange('A6').setDataValidation(mRule);
  sheet.getRange('A4').setDataValidation(yRule); sheet.getRange('A8').setDataValidation(yRule);
  sheet.getRange('A9').setValue('Formats').setFontWeight('bold').setBackground('#f3f3f3');
  PLATFORMS.forEach((label, i) => {
    sheet.getRange(10 + i, 1).insertCheckboxes().setValue(true);
    sheet.getRange(10 + i, 2).setValue(label).setFontWeight('bold');
  });
  sheet.getRange('A14').setValue('Status').setFontWeight('bold').setBackground('#f3f3f3');
  sheet.getRange('A15').setValue('Ready').setFontColor('#4caf50');
  writeHeaders_(sheet);
  sheet.setFrozenRows(1); sheet.setFrozenColumns(2);
  sheet.setColumnWidth(1, 40); sheet.setColumnWidth(2, 110);
  alert_('✅ Sheet ready. Set months/years, tick formats, then ⚡ SecMaster → Refresh Data.');
}
// Header row is rewritten on every fetch too, so adding a column to COLUMNS
// shows up without having to re-run Setup Sheet (which wipes your filters).
function writeHeaders_(sheet) {
  const headers = COLUMNS.map(c => c[1]);
  ensureSize_(sheet, 20, 2 + headers.length);
  sheet.getRange(1, 3, 1, sheet.getMaxColumns() - 2).clearContent();
  sheet.getRange(1, 3, 1, headers.length).setValues([headers])
       .setFontWeight('bold').setBackground('#f9a825').setHorizontalAlignment('center');
}
function getSelectedPlatforms_(sheet) {
  const vals = sheet.getRange(10, 1, PLATFORMS.length, 1).getValues();
  return PLATFORMS.filter((_, i) => vals[i][0] === true);
}

/* ─── 7. MONTH/YEAR RANGE → date_from / date_to ──────────────────── */
function pad2_(n) { return (n < 10 ? '0' : '') + n; }
function monthRangeToDates_(fm, fy, tm, ty) {
  let fi = MONTHS_UPPER.indexOf(String(fm).toUpperCase());
  let ti = MONTHS_UPPER.indexOf(String(tm).toUpperCase());
  let fyN = parseInt(fy, 10), tyN = parseInt(ty, 10);
  if (fi === -1 || ti === -1) throw new Error('Invalid month name in filter');
  if (fyN * 12 + fi > tyN * 12 + ti) { const a = fi; fi = ti; ti = a; const b = fyN; fyN = tyN; tyN = b; }
  const lastDay = new Date(tyN, ti + 1, 0).getDate();
  return {
    dateFrom: fyN + '-' + pad2_(fi + 1) + '-01',
    dateTo:   tyN + '-' + pad2_(ti + 1) + '-' + pad2_(lastDay),
    months: (tyN * 12 + ti) - (fyN * 12 + fi) + 1
  };
}

/* ─── 8. JOB STATE + AUTO-RESUME ─────────────────────────────────── */
const JOB_KEY   = 'SECMASTER_JOB';
const STASH_NAME = 'SEC_AGG_STASH';   // hidden sheet holding the partial aggregate between resumes
function saveJob_(s) { PropertiesService.getScriptProperties().setProperty(JOB_KEY, JSON.stringify(s)); }
function loadJob_() { const r = PropertiesService.getScriptProperties().getProperty(JOB_KEY); return r ? JSON.parse(r) : null; }
function clearJob_() { PropertiesService.getScriptProperties().deleteProperty(JOB_KEY); }
function deleteResumeTriggers_() {
  ScriptApp.getProjectTriggers().forEach(t => { if (t.getHandlerFunction() === 'resumeJob') ScriptApp.deleteTrigger(t); });
}
function scheduleResume_() { deleteResumeTriggers_(); ScriptApp.newTrigger('resumeJob').timeBased().after(10000).create(); }
function setStatus_(sheet, msg, color) { sheet.getRange('A15').setValue(msg).setFontColor(color); SpreadsheetApp.flush(); }
function cancelJob() { deleteResumeTriggers_(); clearJob_(); clearStash_(); setStatus_(DB_(), '🛑 Cancelled', '#f44336'); alert_('🛑 Job cancelled.'); }

/* Partial-aggregate stash (survives the ~6-min execution cap between resumes). */
function stashSheet_(create) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let sh = ss.getSheetByName(STASH_NAME);
  if (!sh && create) { sh = ss.insertSheet(STASH_NAME); sh.hideSheet(); }
  return sh;
}
function saveStash_(map) {
  const sh = stashSheet_(true); sh.clearContents();
  const keys = Object.keys(map);
  if (!keys.length) return;
  // Row layout: [maxDateRaw, maxDateTime, ...sums (SUM_COLS order), ...groupVals]
  const out = keys.map(function (k) {
    const e = map[k];
    return [e.d, (e.t === -Infinity ? '' : e.t)].concat(e.s).concat(e.g);
  });
  const width = 2 + SUM_COLS.length + GROUP_COLS.length;
  ensureSize_(sh, out.length, width);
  sh.getRange(1, 1, out.length, width).setValues(out);
}
function loadStash_() {
  const sh = stashSheet_(false), map = {};
  if (!sh) return map;
  const last = sh.getLastRow();
  if (last < 1) return map;
  const gStart = 2 + SUM_COLS.length;
  const vals = sh.getRange(1, 1, last, gStart + GROUP_COLS.length).getValues();
  for (let i = 0; i < vals.length; i++) {
    const r = vals[i], g = r.slice(gStart, gStart + GROUP_COLS.length), o = {};
    for (let j = 0; j < GROUP_COLS.length; j++) o[GROUP_COLS[j]] = g[j];
    const s = SUM_COLS.map(function (_, j) { return Number(r[2 + j]) || 0; });
    map[aggKey_(o)] = { g: g, d: r[0], t: (r[1] === '' ? -Infinity : Number(r[1])), s: s };
  }
  return map;
}
function clearStash_() {
  const sh = stashSheet_(false);
  if (sh) SpreadsheetApp.getActiveSpreadsheet().deleteSheet(sh);
}

/* ─── 9. FETCH ───────────────────────────────────────────────────── */
function fetchData() {
  const sheet = DB_();
  deleteResumeTriggers_(); clearJob_(); clearStash_();
  const f = sheet.getRange('A2:A8').getValues();
  const fm = f[0][0], fy = String(f[2][0]), tm = f[4][0], ty = String(f[6][0]);
  const platforms = getSelectedPlatforms_(sheet);
  if (!fm || !fy || !tm || !ty) { alert_('⚠️ Fill all 4 date cells'); return; }
  if (!platforms.length) { alert_('⚠️ Tick at least one format'); return; }
  const r = monthRangeToDates_(fm, fy, tm, ty);
  writeHeaders_(sheet);
  // Clear the whole data region (wide) so stale columns from older runs are removed.
  const maxRows = sheet.getMaxRows(), maxCols = sheet.getMaxColumns();
  if (maxRows > 1 && maxCols > 2) sheet.getRange(2, 3, maxRows - 1, maxCols - 2).clearContent();
  setStatus_(sheet, '⏳ Starting...', '#ff9800');
  saveJob_({ formatsCsv: platforms.join(','), dateFrom: r.dateFrom, dateTo: r.dateTo,
             page: 0, rowsRead: 0, total: 0, platforms: platforms.length, months: r.months });
  runJob();
}
function resumeJob() { deleteResumeTriggers_(); if (loadJob_()) runJob(); }

/* ─── 10. JOB RUNNER (paged via API; aggregates, auto-pauses near 6-min cap) ─── */
function runJob() {
  const sheet = DB_(), start = Date.now();
  const TIME_LIMIT = 5 * 60 * 1000, PAGE_SIZE = 50000;
  const state = loadJob_(); if (!state) return;
  let token;
  try { token = getToken_(); }
  catch (e) { setStatus_(sheet, '❌ ' + e.message, '#f44336'); clearJob_(); clearStash_(); throw e; }

  const map = loadStash_();            // resume the running aggregate (empty on first run)
  let page = state.page, seen = state.rowsRead || 0, total = state.total;
  try {
    while (true) {
      const data = fetchReportPage_(token, state.formatsCsv, state.dateFrom, state.dateTo, page, PAGE_SIZE);
      const rows = data.rows || [];
      accumulate_(map, rows);
      seen += rows.length;
      if (data.count != null) total = data.count;
      page += 1;
      setStatus_(sheet, '⏳ read ' + seen + (total ? ('/' + total) : '') + ' rows, ' + Object.keys(map).length + ' groups...', '#ff9800');
      if (rows.length < PAGE_SIZE) break;
      if (Date.now() - start > TIME_LIMIT) {
        saveStash_(map);
        saveJob_({ formatsCsv: state.formatsCsv, dateFrom: state.dateFrom, dateTo: state.dateTo, page: page,
                   rowsRead: seen, total: total, platforms: state.platforms, months: state.months });
        scheduleResume_();
        setStatus_(sheet, '⏸️ read ' + seen + ' rows — resuming in ~10s...', '#ff9800');
        return;
      }
    }
  } catch (e) { setStatus_(sheet, '❌ ' + e.message, '#f44336'); clearJob_(); clearStash_(); deleteResumeTriggers_(); throw e; }

  // Done reading everything — write the aggregated result.
  const out = aggToRows_(map);
  const maxRows = sheet.getMaxRows(), maxCols = sheet.getMaxColumns();
  if (maxRows > 1 && maxCols > 2) sheet.getRange(2, 3, maxRows - 1, maxCols - 2).clearContent();
  if (out.length) {
    ensureSize_(sheet, out.length + 1, 2 + COLUMNS.length);
    sheet.getRange(2, 3, out.length, COLUMNS.length).setValues(out);
  }
  clearJob_(); clearStash_(); deleteResumeTriggers_();
  setStatus_(sheet, '✅ ' + out.length + ' groups | ' + seen + ' rows read | ' + state.months + ' month(s) | ' + state.platforms + ' format(s)', '#4caf50');
}
