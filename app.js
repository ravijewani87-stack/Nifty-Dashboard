/* ══════════════════════════════════════════════════════════════
   NIFTY OPTIONS DASHBOARD — App Logic
   Connects to Supabase for live data + realtime subscriptions
   ══════════════════════════════════════════════════════════════ */
 
// ── CONFIG: Replace with your actual Supabase values ──────────
// These are public-safe (anon key only).
// Set them via environment variables injected at build time,
// OR just replace the placeholder strings below.
const SUPABASE_URL = window.__SUPABASE_URL__ || 'https://YOUR_PROJECT.supabase.co';
const SUPABASE_ANON_KEY = window.__SUPABASE_ANON_KEY__ || 'YOUR_ANON_KEY';
 
// ── Init Supabase ─────────────────────────────────────────────
const sb = supabase.createClient(SUPABASE_URL, SUPABASE_ANON_KEY);
 
// ── App State ─────────────────────────────────────────────────
const state = {
  snapshot:  null,
  signals:   [],
  chain:     [],
  positions: [],
  charts:    {},
};
 
// ── Helpers ───────────────────────────────────────────────────
const $ = id => document.getElementById(id);
const fmt  = (n, d=2) => n != null ? Number(n).toFixed(d) : '—';
const fmtN = n => n != null ? Number(n).toLocaleString('en-IN') : '—';
const pct  = n => n != null ? `${Number(n) >= 0 ? '+' : ''}${fmt(n,1)}%` : '—';
 
function showToast(msg, type='') {
  const t = $('toast');
  t.textContent = msg;
  t.className = `toast show ${type}`;
  setTimeout(() => t.className = 'toast hidden', 3500);
}
 
function timeAgo(ts) {
  if (!ts) return '—';
  const d = new Date(ts);
  const s = Math.round((Date.now() - d) / 1000);
  if (s < 60)   return `${s}s ago`;
  if (s < 3600) return `${Math.round(s/60)}m ago`;
  return d.toLocaleTimeString('en-IN', {hour:'2-digit', minute:'2-digit'});
}
 
function setStatus(kind) {
  const dot  = $('statusDot');
  const text = $('statusText');
  dot.className = `status-dot ${kind}`;
  text.textContent = {live:'Live', stale:'Stale', error:'Error', '':'Connecting...'}[kind] || kind;
}
 
// ══════════════════════════════════════════════════════════════
//  DATA LOADING
// ══════════════════════════════════════════════════════════════
 
async function loadAll() {
  await Promise.all([loadSnapshot(), loadSignals(), loadChain(), loadPositions()]);
  renderAll();
}
 
async function loadSnapshot() {
  const { data, error } = await sb.from('snapshot').select('*').eq('id', 1).single();
  if (data) { state.snapshot = data; }
  if (error) console.warn('snapshot load:', error.message);
}
 
async function loadSignals() {
  const { data, error } = await sb.from('signals').select('*').order('score', {ascending: false});
  if (data) { state.signals = data; }
  if (error) console.warn('signals load:', error.message);
}
 
async function loadChain() {
  const { data, error } = await sb.from('option_chain').select('*').order('strike');
  if (data) { state.chain = data; }
  if (error) console.warn('chain load:', error.message);
}
 
async function loadPositions() {
  const { data, error } = await sb.from('positions').select('*').order('created_at', {ascending: false});
  if (data) { state.positions = data; }
  if (error) console.warn('positions load:', error.message);
}
 
// ══════════════════════════════════════════════════════════════
//  RENDER
// ══════════════════════════════════════════════════════════════
 
function renderAll() {
  renderKPIs();
  renderSignals();
  renderChain();
  renderCharts();
  renderPositions();
  updateExpiries();
}
 
// ── KPIs ──────────────────────────────────────────────────────
function renderKPIs() {
  const s = state.snapshot;
  if (!s) return;
 
  $('kpiSpot').textContent = s.spot ? Number(s.spot).toLocaleString('en-IN', {minimumFractionDigits:2}) : '—';
  $('kpiAtm').textContent  = `ATM: ${s.atm || '—'}`;
 
  const pcr = Number(s.pcr);
  $('kpiPcr').textContent  = fmt(pcr, 3);
  $('kpiPcr').className    = `kpi-value ${pcr < 0.70 ? 'green' : pcr > 1.30 ? 'red' : 'yellow'}`;
  $('kpiPcrLabel').textContent = pcr < 0.70 ? '🐂 BULLISH' : pcr > 1.30 ? '🐻 BEARISH' : '⚖️ NEUTRAL';
 
  const ivr = Number(s.iv_rank);
  $('kpiIvRank').textContent = ivr >= 0 ? `${fmt(ivr,0)}%` : '—';
  $('kpiIvRank').className   = `kpi-value ${ivr < 0 ? '' : ivr <= 30 ? 'green' : ivr <= 60 ? 'yellow' : 'red'}`;
  $('kpiIvLabel').textContent = ivr < 0 ? 'Building history...' :
    ivr <= 30 ? 'CHEAP ✅' : ivr <= 60 ? 'MODERATE' : 'EXPENSIVE ⚠️';
 
  $('kpiAtmIv').textContent    = `${fmt(s.atm_iv, 2)}%`;
  $('kpiMaxPain').textContent  = fmtN(s.max_pain);
 
  const gap = Number(s.spot) - Number(s.max_pain);
  $('kpiMpGap').textContent = s.max_pain
    ? `${gap >= 0 ? '↑' : '↓'} ${Math.abs(gap).toFixed(0)} pts from spot`
    : '—';
 
  const entryCount = state.signals.filter(s => s.kind === 'BUY_CALL' || s.kind === 'BUY_PUT').length;
  const exitCount  = state.signals.filter(s => s.kind === 'EXIT').length;
  $('kpiSignals').textContent = entryCount;
  $('kpiSignals').className   = `kpi-value ${entryCount > 0 ? 'green' : ''}`;
  $('kpiExits').textContent   = `${exitCount} exit alert${exitCount !== 1 ? 's' : ''}`;
 
  // Update timestamp
  const ts = s.updated_at;
  $('lastUpdated').textContent = ts ? timeAgo(ts) : '—';
 
  // Status dot
  if (ts) {
    const age = (Date.now() - new Date(ts)) / 1000;
    setStatus(age < 400 ? 'live' : 'stale');
  }
}
 
// ── Signals ───────────────────────────────────────────────────
function renderSignals() {
  const calls  = state.signals.filter(s => s.kind === 'BUY_CALL');
  const puts   = state.signals.filter(s => s.kind === 'BUY_PUT');
  const exits  = state.signals.filter(s => s.kind === 'EXIT');
  const watches = state.signals.filter(s => s.kind === 'WATCH');
 
  // Exit bar
  const ec = $('exitContainer');
  if (exits.length) {
    ec.innerHTML = `<div class="exit-bar">
      <div class="exit-bar-title">🟡 EXIT ALERTS — Act Now (${exits.length})</div>
      ${exits.map(s => sigCardHTML(s)).join('')}
    </div>`;
  } else {
    ec.innerHTML = '';
  }
 
  $('callSignals').innerHTML = calls.length
    ? calls.slice(0,6).map(s => sigCardHTML(s)).join('')
    : '<div class="empty-state">No call signals — PCR or IV not at thresholds yet</div>';
 
  $('putSignals').innerHTML = puts.length
    ? puts.slice(0,6).map(s => sigCardHTML(s)).join('')
    : '<div class="empty-state">No put signals — PCR or IV not at thresholds yet</div>';
 
  $('watchSignals').innerHTML = watches.length
    ? `<div class="signals-grid">${
        watches.map(s => `<div>${sigCardHTML(s)}</div>`).join('')
      }</div>`
    : '<div class="empty-state">Nothing approaching thresholds right now</div>';
}
 
function sigCardHTML(sig) {
  const isCall  = sig.kind === 'BUY_CALL';
  const isExit  = sig.kind === 'EXIT';
  const isWatch = sig.kind === 'WATCH';
  const cls     = isCall ? 'call' : isExit ? 'exit' : isWatch ? 'watch' : 'put';
  const badgeTxt = isCall ? 'BUY CALL' : isExit ? 'EXIT' : isWatch ? 'WATCH' : 'BUY PUT';
  const barColor = isCall ? '#00c853' : isExit ? '#ffd600' : isWatch ? '#2196f3' : '#ff1744';
  const barW     = Math.round((sig.score / 10) * 100);
  const reasons  = Array.isArray(sig.reasons) ? sig.reasons : JSON.parse(sig.reasons || '[]');
  const reasonsCls = isExit ? 'exit-reasons' : '';
 
  return `<div class="sig-card ${cls}">
    <div class="sig-header">
      <span class="badge ${cls}">${badgeTxt}</span>
      <span class="sig-title">${sig.tradingsymbol}</span>
      <span class="sig-score">Score ${sig.score}/10</span>
    </div>
    <div class="score-bar-wrap">
      <div class="score-bar-bg">
        <div class="score-bar-fill" style="width:${barW}%;background:${barColor}"></div>
      </div>
    </div>
    <div class="sig-meta">
      <span>LTP <b>₹${fmt(sig.ltp,2)}</b></span>
      <span>Strike <b>${sig.strike}</b></span>
      <span>Expiry <b>${sig.expiry}</b></span>
      <span>Spot <b>${fmt(sig.spot,0)}</b></span>
      <span>OI <b>${fmtN(sig.oi)}</b>
        <span class="oi-badge ${sig.oi_flag}">${sig.oi_flag} ${pct(sig.oi_change_pct)}</span>
      </span>
    </div>
    <div class="sig-greeks">
      <span>Δ <b>${fmt(sig.delta,4)}</b></span>
      <span>Γ <b>${fmt(sig.gamma,5)}</b></span>
      <span>Θ <b>${fmt(sig.theta,2)}/d</b></span>
      <span>ν <b>${fmt(sig.vega,4)}</b></span>
      <span>IV <b>${fmt(sig.iv,1)}%</b></span>
      <span>PCR <b>${fmt(sig.pcr,3)}</b></span>
      <span>IV Rank <b>${sig.iv_rank >= 0 ? fmt(sig.iv_rank,0)+'%' : '—'}</b></span>
    </div>
    <div class="sig-reasons ${reasonsCls}">
      ${reasons.map(r => `<div class="reason">${r}</div>`).join('')}
    </div>
  </div>`;
}
 
// ── Option Chain ──────────────────────────────────────────────
function getSelectedExpiry(selectId) {
  const sel = $(selectId);
  return sel ? sel.value : null;
}
 
function renderChain() {
  const expiry = getSelectedExpiry('chainExpiry');
  const filter = $('chainFilter')?.value || 'all';
  const spot   = Number(state.snapshot?.spot || 0);
  const atm    = Math.round(spot / 50) * 50;
 
  let rows = state.chain.filter(r => !expiry || r.expiry === expiry);
  if (filter !== 'all') rows = rows.filter(r => r.oi_flag === filter);
 
  // Group by strike
  const strikes = [...new Set(rows.map(r => r.strike))].sort((a,b)=>a-b);
  const calls   = Object.fromEntries(rows.filter(r=>r.type==='CE').map(r=>[r.strike,r]));
  const puts    = Object.fromEntries(rows.filter(r=>r.type==='PE').map(r=>[r.strike,r]));
 
  const tbody = $('chainBody');
  if (!strikes.length) {
    tbody.innerHTML = '<tr><td colspan="14" class="empty-state">No data for this expiry</td></tr>';
    return;
  }
 
  tbody.innerHTML = strikes.map(s => {
    const c   = calls[s] || {};
    const p   = puts[s]  || {};
    const isAtm = s === atm;
    const flag  = c.oi_flag || p.oi_flag || 'neutral';
    const rowCls = isAtm ? 'atm-row' :
                   flag === 'buildup'   ? 'buildup-row' :
                   flag === 'unwinding' ? 'unwinding-row' : '';
 
    const cv = (obj, key, d=2, prefix='') =>
      obj[key] != null ? `${prefix}${fmt(obj[key],d)}` : '<span class="muted">—</span>';
    const cpct = (obj, key) => {
      if (obj[key] == null) return '<span class="muted">—</span>';
      const v = Number(obj[key]);
      return `<span class="${v>0?'positive':v<0?'negative':''}">${v>=0?'+':''}${fmt(v,1)}%</span>`;
    };
 
    return `<tr class="${rowCls}">
      <td>${c.oi ? fmtN(c.oi) : '—'}</td>
      <td>${cpct(c,'oi_change_pct')}</td>
      <td>${c.volume ? fmtN(c.volume) : '—'}</td>
      <td>${c.iv ? fmt(c.iv,1) : '—'}</td>
      <td>${cv(c,'delta',3)}</td>
      <td><b>₹${cv(c,'ltp',2)}</b></td>
      <td class="strike-col">${isAtm?'<span class="atm-star">★</span> ':''}${s}</td>
      <td><b>₹${cv(p,'ltp',2)}</b></td>
      <td>${cv(p,'delta',3)}</td>
      <td>${p.iv ? fmt(p.iv,1) : '—'}</td>
      <td>${p.volume ? fmtN(p.volume) : '—'}</td>
      <td>${cpct(p,'oi_change_pct')}</td>
      <td>${p.oi ? fmtN(p.oi) : '—'}</td>
      <td><span class="oi-badge ${flag}">${flag}</span></td>
    </tr>`;
  }).join('');
}
 
// ── Charts ────────────────────────────────────────────────────
function destroyChart(id) {
  if (state.charts[id]) { state.charts[id].destroy(); delete state.charts[id]; }
}
 
function renderCharts() {
  const expiry = getSelectedExpiry('chartExpiry');
  let rows = state.chain.filter(r => !expiry || r.expiry === expiry);
  const spot     = Number(state.snapshot?.spot || 0);
  const maxPain  = Number(state.snapshot?.max_pain || 0);
 
  const calls  = rows.filter(r=>r.type==='CE').sort((a,b)=>a.strike-b.strike);
  const puts   = rows.filter(r=>r.type==='PE').sort((a,b)=>a.strike-b.strike);
  const strikes = [...new Set(rows.map(r=>r.strike))].sort((a,b)=>a-b);
 
  const darkOptions = {
    plugins: {legend:{labels:{color:'#6b7280'}}, tooltip:{backgroundColor:'#1a1d2e'}},
    scales: {
      x: {ticks:{color:'#6b7280'},grid:{color:'#2a2d42'}},
      y: {ticks:{color:'#6b7280'},grid:{color:'#2a2d42'}},
    },
    animation: {duration: 400},
    responsive: true,
    maintainAspectRatio: true,
  };
 
  // Spot & MaxPain lines
  const spotAnnotation = idx => ({
    type:'line', scaleID:'x', value: strikes.indexOf(spot), borderColor:'rgba(255,255,255,0.5)',
    borderWidth:1, borderDash:[4,4],
  });
 
  // ── OI Bar Chart ──────────────────────────────────────────
  destroyChart('oiChart');
  state.charts['oiChart'] = new Chart($('oiChart'), {
    type: 'bar',
    data: {
      labels: strikes,
      datasets: [
        { label:'Call OI', data: strikes.map(s=>calls.find(r=>r.strike===s)?.oi||0),
          backgroundColor:'rgba(0,200,83,0.6)' },
        { label:'Put OI',  data: strikes.map(s=>puts.find(r=>r.strike===s)?.oi||0),
          backgroundColor:'rgba(255,23,68,0.6)' },
      ],
    },
    options: { ...darkOptions, plugins: { ...darkOptions.plugins,
      annotation: { annotations: {
        spot:    { type:'line', xMin:spot,    xMax:spot,    borderColor:'rgba(255,255,255,0.7)', borderWidth:1, borderDash:[4,4], label:{display:true,content:`Spot ${spot}`,color:'#fff',backgroundColor:'rgba(0,0,0,0.5)'} },
        maxPain: { type:'line', xMin:maxPain, xMax:maxPain, borderColor:'rgba(255,179,0,0.7)',   borderWidth:1, borderDash:[4,4], label:{display:true,content:`MP ${maxPain}`,color:'#ffb300',backgroundColor:'rgba(0,0,0,0.5)'} },
      }},
    }},
  });
 
  // ── OI Change Chart ───────────────────────────────────────
  destroyChart('oiChangeChart');
  const cOiChg = strikes.map(s=>calls.find(r=>r.strike===s)?.oi_change||0);
  const pOiChg = strikes.map(s=>puts.find(r=>r.strike===s)?.oi_change||0);
  state.charts['oiChangeChart'] = new Chart($('oiChangeChart'), {
    type: 'bar',
    data: {
      labels: strikes,
      datasets: [
        { label:'Call OI Δ', data: cOiChg,
          backgroundColor: cOiChg.map(v=>v>=0?'rgba(0,200,83,0.7)':'rgba(255,23,68,0.7)') },
        { label:'Put OI Δ',  data: pOiChg,
          backgroundColor: pOiChg.map(v=>v>=0?'rgba(255,96,144,0.7)':'rgba(0,230,118,0.7)') },
      ],
    },
    options: darkOptions,
  });
 
  // ── IV Skew ───────────────────────────────────────────────
  destroyChart('ivChart');
  state.charts['ivChart'] = new Chart($('ivChart'), {
    type: 'line',
    data: {
      labels: strikes,
      datasets: [
        { label:'Call IV', data: strikes.map(s=>calls.find(r=>r.strike===s)?.iv||null),
          borderColor:'#00c853', pointRadius:3, tension:0.3, spanGaps:true },
        { label:'Put IV',  data: strikes.map(s=>puts.find(r=>r.strike===s)?.iv||null),
          borderColor:'#ff1744', pointRadius:3, tension:0.3, spanGaps:true },
      ],
    },
    options: darkOptions,
  });
 
  // ── PCR Doughnut ──────────────────────────────────────────
  destroyChart('pcrChart');
  const pcr = Number(state.snapshot?.pcr || 1);
  const pcrClamp = Math.min(Math.max(pcr, 0), 2.5);
  const pcrColor = pcr < 0.70 ? '#00c853' : pcr > 1.30 ? '#ff1744' : '#ffd600';
  state.charts['pcrChart'] = new Chart($('pcrChart'), {
    type: 'doughnut',
    data: {
      datasets: [{
        data: [pcrClamp, 2.5 - pcrClamp],
        backgroundColor: [pcrColor, '#2a2d42'],
        borderWidth: 0,
        circumference: 180,
        rotation: 270,
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: true,
      cutout: '72%',
      plugins: {
        legend: { display: false },
        tooltip: { enabled: false },
        // Custom center label
        afterDraw: chart => {
          const {ctx, width, height} = chart;
          ctx.restore();
          ctx.font = `bold 1.8rem sans-serif`;
          ctx.fillStyle = pcrColor;
          ctx.textAlign = 'center';
          ctx.textBaseline = 'middle';
          ctx.fillText(pcr.toFixed(3), width / 2, height * 0.65);
          ctx.restore();
        },
      },
    },
    plugins: [{
      afterDraw(chart) {
        const {ctx, chartArea: {left,right,top,bottom}, width, height} = chart;
        ctx.save();
        ctx.font = `bold ${Math.round(height*0.14)}px sans-serif`;
        ctx.fillStyle = pcrColor;
        ctx.textAlign = 'center';
        ctx.textBaseline = 'middle';
        ctx.fillText(pcr.toFixed(3), width/2, height*0.62);
        ctx.restore();
      },
    }],
  });
}
 
// ── Positions ─────────────────────────────────────────────────
function renderPositions() {
  const list = $('positionsList');
  if (!state.positions.length) {
    list.innerHTML = '<div class="empty-state">No positions yet. Add one to track exit signals.</div>';
    return;
  }
  list.innerHTML = state.positions.map(p => `
    <div class="pos-row">
      <div class="pos-info">
        <div class="pos-sym">${p.tradingsymbol || 'Unknown'}</div>
        <div class="pos-details">
          Buy ₹${fmt(p.buy_price,2)} · ${p.type} · Strike ${p.strike} · Expiry ${p.expiry || '—'}
          ${p.notes ? ` · <i>${p.notes}</i>` : ''}
        </div>
      </div>
      <button class="pos-delete" data-id="${p.id}">Remove</button>
    </div>
  `).join('');
 
  // Attach delete handlers
  list.querySelectorAll('.pos-delete').forEach(btn => {
    btn.addEventListener('click', () => deletePosition(btn.dataset.id));
  });
}
 
async function deletePosition(id) {
  const { error } = await sb.from('positions').delete().eq('id', id);
  if (!error) {
    state.positions = state.positions.filter(p => String(p.id) !== String(id));
    renderPositions();
    showToast('Position removed', 'red');
  }
}
 
// ── Expiry selects ─────────────────────────────────────────────
function updateExpiries() {
  const expiries = [...new Set(state.chain.map(r=>r.expiry))].sort();
  ['chainExpiry','chartExpiry'].forEach(id => {
    const sel = $(id);
    if (!sel) return;
    const current = sel.value;
    sel.innerHTML = expiries.map(e => `<option value="${e}">${e}</option>`).join('');
    if (current && expiries.includes(current)) sel.value = current;
  });
}
 
// ══════════════════════════════════════════════════════════════
//  REALTIME SUBSCRIPTIONS
// ══════════════════════════════════════════════════════════════
 
function setupRealtime() {
  // Signals table: re-render signals + KPIs instantly when GitHub Actions writes new data
  sb.channel('signals-changes')
    .on('postgres_changes', { event: '*', schema: 'public', table: 'signals' },
      async () => {
        await loadSignals();
        renderSignals();
        renderKPIs();
        showToast('🚨 Signals updated!', 'green');
      }
    )
    .subscribe();
 
  // Snapshot table: update KPIs
  sb.channel('snapshot-changes')
    .on('postgres_changes', { event: 'UPDATE', schema: 'public', table: 'snapshot' },
      async () => {
        await Promise.all([loadSnapshot(), loadChain()]);
        renderKPIs();
        renderChain();
        renderCharts();
        $('lastUpdated').textContent = 'just now';
        setStatus('live');
      }
    )
    .subscribe();
}
 
// ══════════════════════════════════════════════════════════════
//  EVENT LISTENERS
// ══════════════════════════════════════════════════════════════
 
// Tabs
document.querySelectorAll('.tab-btn').forEach(btn => {
  btn.addEventListener('click', () => {
    document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
    document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
    btn.classList.add('active');
    $(`tab-${btn.dataset.tab}`).classList.add('active');
    // Re-render charts when charts tab becomes active (canvas sizing)
    if (btn.dataset.tab === 'charts') renderCharts();
  });
});
 
// Chain expiry filter
$('chainExpiry')?.addEventListener('change', renderChain);
$('chainFilter')?.addEventListener('change', renderChain);
$('chartExpiry')?.addEventListener('change', renderCharts);
 
// Add position form
$('posForm')?.addEventListener('submit', async e => {
  e.preventDefault();
  const payload = {
    tradingsymbol: $('posSymbol').value.trim().toUpperCase(),
    buy_price:     parseFloat($('posBuyPrice').value),
    type:          $('posType').value,
    strike:        parseInt($('posStrike').value),
    expiry:        $('posExpiry').value,
    notes:         $('posNotes').value.trim() || null,
  };
  const { data, error } = await sb.from('positions').insert([payload]).select();
  if (data) {
    state.positions.unshift(data[0]);
    renderPositions();
    e.target.reset();
    showToast(`Position ${payload.tradingsymbol} added`, 'green');
  } else {
    showToast(`Error: ${error?.message}`, 'red');
  }
});
 
// ══════════════════════════════════════════════════════════════
//  INIT
// ══════════════════════════════════════════════════════════════
(async () => {
  setStatus('');
  await loadAll();
  setupRealtime();
 
  // Fallback poll every 3 minutes in case realtime misses something
  setInterval(async () => {
    await loadAll();
  }, 3 * 60 * 1000);
})();
