function onOpen(){
  // Each menu is added independently. Previously a single missing/renamed
  // function (e.g. onOpen1) threw and killed EVERY menu after it, so the
  // "⚡ SecMaster" menu silently never appeared. Now one broken menu cannot
  // take the others down with it — the failure just lands in the log.
  ['onOpen1', 'onOpen2', 'invOnOpen', 'BBADS_addMenu'].forEach(function (name) {
    try {
      var f = globalThis[name];
      if (typeof f === 'function') f();
      else Logger.log('menu skipped: ' + name + ' is not defined');
    } catch (e) {
      Logger.log('menu failed in ' + name + ': ' + (e && e.message ? e.message : e));
    }
  });
}
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
 *
 * COLUMN OWNERSHIP (see section 0):
 *   A..B  control panel   |   C..L  script report block   |   M..  YOURS.
 *   Nothing in this file clears or writes past column L, so formulas you
 *   keep in M onward survive both Refresh Data and Setup Sheet.
 *
 * ─── FIXES IN THIS VERSION (2026-08-21) ────────────────────────────
 *   1. onOpen no longer dies on one missing menu function (see above).
 *   2. aggKey_ now joins group values with a separator. With join('') two
 *      different groups could glue into the SAME key and have their sums
 *      merged — e.g. ('OLIVE','OIL 1L') and ('OLIVE OIL',' 1L') both became
 *      "OLIVEOIL 1L". Silently wrong totals, no error. Fixed.
 *   3. Every UrlFetchApp call goes through fetchWithRetry_, which retries
 *      network blips and, on giving up, explains that "Address unavailable"
 *      means GOOGLE could not open a connection (stale DNS on Google's side
 *      or a firewall drop) — not that this script or the site is broken.
 *   4. The 6-minute cap is now checked BEFORE starting another page, and the
 *      budget dropped 5min -> 3.5min. Before, the check ran after the fetch,
 *      so a page starting at 4:55 could blow past 6:00 and be killed with the
 *      partial aggregate never stashed and no resume trigger created — the
 *      job died silently with Status frozen on "read N rows...".
 *   5. The paging loop now stops on the row COUNT, not on "got fewer rows
 *      than I asked for". The backend clamps page_size to the view's max_rows
 *      (platforms/reports.py); if that cap is ever lowered, the old test
 *      broke after page 0 and reported success with a fraction of the data.
 *   6. A failed page no longer throws away a long read: the aggregate is
 *      stashed and the job retries up to 3 times before giving up.
 *   7. New diagnoseHost() (also on the menu) tells you whether Google can
 *      reach the server at all, and by which route.
 *************************************************************************/

/* ─── 0. COLUMN BOUNDARY — protects your formulas in M+ ──────────── */
const SCRIPT_FIRST_COL = 3;    // C — first report column
const SCRIPT_LAST_COL  = 12;   // L — the wall. Column M onward is never touched.

// Width of the report block, clamped so it can never spill past column L.
function scriptCols_() {
  return Math.min(COLUMNS.length, SCRIPT_LAST_COL - SCRIPT_FIRST_COL + 1);
}
// Refuse to run (rather than overwrite M) if COLUMNS outgrew C..L.
// NOTE: COLUMNS has 10 entries and C:L is exactly 10 wide — there is ZERO
// spare room. Adding an 11th column trips this on purpose. To make room:
// insert a spreadsheet column before M, then raise SCRIPT_LAST_COL to 13.
function assertFits_() {
  const room = SCRIPT_LAST_COL - SCRIPT_FIRST_COL + 1;
  if (COLUMNS.length > room) {
    const m = '❌ COLUMNS has ' + COLUMNS.length + ' entries but only ' + room +
              ' columns (C:L) are reserved. Insert spreadsheet column(s) before M, ' +
              'raise SCRIPT_LAST_COL, then retry. Nothing was written.';
    alert_(m);
    throw new Error(m);
  }
}
// Clear the report block from `firstRow` downward. Stops dead at column L.
function clearScriptBlock_(sheet, firstRow) {
  const rows = sheet.getMaxRows() - firstRow + 1;
  const cols = scriptCols_();
  if (rows > 0 && cols > 0) {
    sheet.getRange(firstRow, SCRIPT_FIRST_COL, rows, cols).clearContent();
  }
}

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

/* Networking */
const FETCH_TRIES     = 3;       // attempts per HTTP call before giving up
const FETCH_BACKOFF   = 2000;    // ms before the 2nd try, doubled each time
const MAX_JOB_RETRIES = 3;       // resumes allowed after a failed page

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

// Errors that mean "Google could not open a connection", not "the server said
// no". Worth retrying; if they persist the cause is DNS or a firewall, outside
// this script.
const _NET_ERRORS = ['Address unavailable', 'DNS error', 'Timeout', 'timed out',
                     'Unexpected error', 'Connection refused'];
function isNetworkError_(msg) {
  for (var i = 0; i < _NET_ERRORS.length; i++) {
    if (String(msg).indexOf(_NET_ERRORS[i]) !== -1) return true;
  }
  return false;
}
// Single place every HTTP call goes through, so one retry policy covers all.
function fetchWithRetry_(url, params) {
  var lastErr = null, wait = FETCH_BACKOFF;
  for (var i = 0; i < FETCH_TRIES; i++) {
    try { return UrlFetchApp.fetch(url, params); }
    catch (e) {
      lastErr = e;
      var msg = (e && e.message) ? e.message : String(e);
      if (!isNetworkError_(msg)) throw e;          // a real error — do not mask it
      Logger.log('network attempt ' + (i + 1) + '/' + FETCH_TRIES + ' failed: ' + msg);
      if (i < FETCH_TRIES - 1) { Utilities.sleep(wait); wait *= 2; }
    }
  }
  var last = (lastErr && lastErr.message) ? lastErr.message : String(lastErr);
  throw new Error('Cannot reach ' + apiBase_() + ' from Google — "' + last +
    '". The site itself may be perfectly healthy: this error means GOOGLE could ' +
    'not open the connection, usually a stale DNS entry on Google\'s side after ' +
    'the server moved, or a firewall dropping Google\'s IPs. Run diagnoseHost() ' +
    'and read the Executions log.');
}

function getToken_() {
  const p = PropertiesService.getScriptProperties();
  const email = p.getProperty('LOGIN_EMAIL'), pass = p.getProperty('LOGIN_PASSWORD');
  if (!apiBase_() || !email || !pass) throw new Error('Missing API creds — run setupCredentials()');
  const res = fetchWithRetry_(apiBase_() + '/api/auth/login', {
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
  const res = fetchWithRetry_(url, {
    method: 'get', headers: { Authorization: 'Bearer ' + token }, muteHttpExceptions: true
  });
  const code = res.getResponseCode(), body = res.getContentText();
  if (code !== 200) throw new Error('Reports HTTP ' + code + ': ' + body);
  const data = JSON.parse(body);
  if (data.error) throw new Error('API error: ' + data.error);
  return data;
}

/* ─── 3b. AGGREGATION HELPERS ────────────────────────────────────── */
// U+0001 — a control character that can never appear in a category / item
// name, so two different groups can never glue into the same key. Built with
// fromCharCode rather than typed literally so it survives copy-paste. See
// fix #2 in the header.
const KEY_SEP = String.fromCharCode(1);
function aggKey_(obj) {
  return GROUP_COLS.map(function (k) {
    return String(obj[k] == null ? '' : obj[k]);
  }).join(KEY_SEP);
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

/* ─── 4. DIAGNOSTICS ─────────────────────────────────────────────── */
function testConnection() {
  let msg;
  try {
    const token = getToken_();
    const data = fetchReportPage_(token, PLATFORMS.join(','), '', '', 0, 1);
    msg = '✅ CONNECTED. SecMaster reachable — ' + data.count + ' total rows for all formats.';
  } catch (e) { msg = '❌ FAILED:\n' + e.message; }
  Logger.log(msg); alert_(msg);
}

/* Answers ONE question: can Google reach the server, and by which route?
   Run it, then read View > Executions (or the log below).

   HOW TO READ THE RESULT
   - step 3 gives a NUMBER (404 or 200) but step 2 fails
       -> Google CAN reach the box; only the hostname route is stale. Nothing
          to change in this script — Google's DNS cache is behind. Wait for the
          TTL, or get the DNS TTL on the `ecom` record lowered.
   - step 3 ALSO fails with "Address unavailable"
       -> Google's IPs are being dropped at the network edge. No script change
          can fix that; the network team must allow them, or the data must be
          PUSHED from the Django server instead of pulled from here.
   A 404 in step 3 is a SUCCESS: it means the TCP+TLS connection worked and
   nginx simply picked its default site because we asked by IP, not by name. */
function diagnoseHost() {
  var out = [];
  function log(s) { out.push(s); Logger.log(s); }

  log('--- diagnoseHost ' + new Date() + ' ---');
  log('API_BASE stored in Script Properties: ' + (apiBase_() || '(empty!)'));

  // 1. What does Google's public resolver say the address is today?
  try {
    var dns = UrlFetchApp.fetch('https://dns.google/resolve?name=ecom.jivo.in&type=A',
                                { muteHttpExceptions: true });
    log('1. Google DNS says: ' + dns.getContentText());
  } catch (e) { log('1. DNS lookup FAILED: ' + e.message); }

  // 2. By hostname — this is the call the script actually makes.
  try {
    var a = UrlFetchApp.fetch('https://ecom.jivo.in/', { muteHttpExceptions: true });
    log('2. by hostname  -> HTTP ' + a.getResponseCode() + ' (reachable)');
  } catch (e) { log('2. by hostname  -> FAILED: ' + e.message); }

  // 3. Straight to the current IP, skipping DNS entirely. The certificate is
  //    issued for the NAME, not the IP, so cert checking must be off here.
  try {
    var b = UrlFetchApp.fetch('https://138.252.101.117/', {
      muteHttpExceptions: true, validateHttpsCertificates: false });
    log('3. by IP .117   -> HTTP ' + b.getResponseCode() +
        ' (connection OK — 404 is expected and fine)');
  } catch (e) { log('3. by IP .117   -> FAILED: ' + e.message); }

  alert_('diagnoseHost done — open View > Executions for the full log.');
  return out.join('\n');
}

/* ─── 5. MENU ────────────────────────────────────────────────────── */
function onOpen2() {
  SpreadsheetApp.getUi().createMenu('⚡ SecMaster')
    .addItem('🔄 Refresh Data', 'fetchData')
    .addItem('⚙️ Setup Sheet', 'setupSheet')
    .addItem('🔌 Test Connection', 'testConnection')
    .addItem('🩺 Diagnose Host', 'diagnoseHost')
    .addItem('🛑 Cancel Job', 'cancelJob')
    .addToUi();
}

/* ─── 6. CONTROL SHEET ───────────────────────────────────────────── */
function DB_() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  return ss.getSheetByName('SEC DATA') || ss.insertSheet('SEC DATA');
}
// Grow a sheet so a setValues() of this size cannot fall off the grid. A new
// sheet is 1000x26; the aggregate can be taller than that.
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
  assertFits_();
  const sheet = DB_();
  ensureSize_(sheet, 20, SCRIPT_LAST_COL);
  // Reset A:L only (content + formatting + validations). Columns M onward,
  // where your formulas live, are left completely alone.
  // WAS: sheet.clear()  <-- wiped the entire sheet, including M+.
  sheet.getRange(1, 1, sheet.getMaxRows(), SCRIPT_LAST_COL)
       .clear().clearDataValidations().clearNote();

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
  alert_('✅ Sheet ready (columns M+ preserved). Set months/years, tick formats, then ⚡ SecMaster → Refresh Data.');
}
// Header row is rewritten on every fetch too, so adding a column to COLUMNS
// shows up without having to re-run Setup Sheet (which wipes your filters).
function writeHeaders_(sheet) {
  assertFits_();
  const headers = COLUMNS.map(c => c[1]);
  ensureSize_(sheet, 20, SCRIPT_LAST_COL);
  // WAS: getRange(1, 3, 1, sheet.getMaxColumns() - 2)  <-- reached into M+.
  clearScriptBlock_(sheet, 1);                       // row 1, C:L only
  sheet.getRange(1, SCRIPT_FIRST_COL, 1, headers.length).setValues([headers])
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

/* Partial-aggregate stash (survives the ~6-min execution cap between resumes).
   This lives on its own hidden sheet, so the C:L boundary does not apply here. */
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
  assertFits_();
  const sheet = DB_();
  deleteResumeTriggers_(); clearJob_(); clearStash_();
  const f = sheet.getRange('A2:A8').getValues();
  const fm = f[0][0], fy = String(f[2][0]), tm = f[4][0], ty = String(f[6][0]);
  const platforms = getSelectedPlatforms_(sheet);
  if (!fm || !fy || !tm || !ty) { alert_('⚠️ Fill all 4 date cells'); return; }
  if (!platforms.length) { alert_('⚠️ Tick at least one format'); return; }
  const r = monthRangeToDates_(fm, fy, tm, ty);
  writeHeaders_(sheet);
  // Clear the script's data block only (C:L). Columns M+ keep your formulas.
  // WAS: getRange(2, 3, maxRows - 1, maxCols - 2)  <-- wiped M+ on every run.
  clearScriptBlock_(sheet, 2);
  setStatus_(sheet, '⏳ Starting...', '#ff9800');
  saveJob_({ formatsCsv: platforms.join(','), dateFrom: r.dateFrom, dateTo: r.dateTo,
             page: 0, rowsRead: 0, total: 0, platforms: platforms.length,
             months: r.months, attempts: 0 });
  runJob();
}
function resumeJob() { deleteResumeTriggers_(); if (loadJob_()) runJob(); }

/* ─── 10. JOB RUNNER (paged via API; aggregates, auto-pauses near 6-min cap) ─── */
function runJob() {
  const sheet = DB_(), start = Date.now();
  // 3.5 min, not 5. Apps Script hard-kills at 6 min; one 50k-row page can take
  // 1-2 min, so a 5-min budget could start a page that never finishes and the
  // execution died before anything was stashed. See fix #4 in the header.
  const TIME_LIMIT = 3.5 * 60 * 1000, PAGE_SIZE = 50000;
  const MAX_PAGES  = 500;                 // runaway guard: 500 x 50k = 25M rows
  const state = loadJob_(); if (!state) return;

  const map = loadStash_();            // resume the running aggregate (empty on first run)
  let page = state.page, seen = state.rowsRead || 0, total = state.total;

  // Pause cleanly: stash the aggregate, remember where we are, come back in ~10s.
  function pause_(nextPage, attempts, statusMsg) {
    saveStash_(map);
    saveJob_({ formatsCsv: state.formatsCsv, dateFrom: state.dateFrom, dateTo: state.dateTo,
               page: nextPage, rowsRead: seen, total: total,
               platforms: state.platforms, months: state.months, attempts: attempts });
    scheduleResume_();
    setStatus_(sheet, statusMsg, '#ff9800');
  }

  let token;
  try {
    token = getToken_();
    while (true) {
      // Check the clock BEFORE spending another 1-2 minutes on a page, so a
      // hard kill can never lose work that was never stashed.
      if (Date.now() - start > TIME_LIMIT) {
        pause_(page, 0, '⏸️ read ' + seen + ' rows — resuming in ~10s...');
        return;
      }
      if (page >= MAX_PAGES) throw new Error('Aborting: passed ' + MAX_PAGES +
        ' pages without finishing. Narrow the month range.');

      const data = fetchReportPage_(token, state.formatsCsv, state.dateFrom, state.dateTo, page, PAGE_SIZE);
      const rows = data.rows || [];
      accumulate_(map, rows);
      seen += rows.length;
      if (data.count != null) total = data.count;
      page += 1;
      setStatus_(sheet, '⏳ read ' + seen + (total ? ('/' + total) : '') + ' rows, ' +
                        Object.keys(map).length + ' groups...', '#ff9800');

      // Stop on the row COUNT, not on "fewer rows than I asked for". The backend
      // clamps page_size to the view's max_rows; if that cap is ever lowered the
      // old test broke after page 0 and silently reported success on partial
      // data. Paging by count stays correct because the server computes its
      // offset from its OWN clamped page size. See fix #5 in the header.
      if (rows.length === 0) break;
      if (total && seen >= total) {
        if (seen > total) Logger.log('WARNING: read ' + seen + ' rows but count was ' +
          total + ' — pages overlapped, totals may be inflated.');
        break;
      }
    }
  } catch (e) {
    // A network blip must not throw away a long read. Keep the partial
    // aggregate and try again, up to MAX_JOB_RETRIES times. See fix #6.
    const attempts = (state.attempts || 0) + 1;
    if (attempts <= MAX_JOB_RETRIES) {
      pause_(page, attempts, '⚠️ ' + String(e.message).slice(0, 120) +
             ' — retry ' + attempts + '/' + MAX_JOB_RETRIES + ' in ~10s');
      Logger.log('attempt ' + attempts + '/' + MAX_JOB_RETRIES + ' failed: ' + e.message);
      return;
    }
    setStatus_(sheet, '❌ ' + e.message, '#f44336');
    clearJob_(); clearStash_(); deleteResumeTriggers_();
    throw e;
  }

  // Done reading everything — write the aggregated result into C:L only.
  const out = aggToRows_(map);
  // WAS: getRange(2, 3, maxRows - 1, maxCols - 2)  <-- wiped M+ before writing.
  clearScriptBlock_(sheet, 2);
  if (out.length) {
    ensureSize_(sheet, out.length + 1, SCRIPT_LAST_COL);
    sheet.getRange(2, SCRIPT_FIRST_COL, out.length, scriptCols_()).setValues(out);
  }
  clearJob_(); clearStash_(); deleteResumeTriggers_();
  setStatus_(sheet, '✅ ' + out.length + ' groups | ' + seen + ' rows read | ' +
                    state.months + ' month(s) | ' + state.platforms + ' format(s)', '#4caf50');
}
