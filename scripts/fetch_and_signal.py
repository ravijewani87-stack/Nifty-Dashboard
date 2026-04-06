"""
╔══════════════════════════════════════════════════════════════╗
║  NIFTY OPTIONS — Data Fetcher & Signal Engine                ║
║  Runs via GitHub Actions every 5 min during market hours     ║
║  Writes results to Supabase for the live dashboard           ║
╚══════════════════════════════════════════════════════════════╝
"""

import os, sys, math, time, datetime, json, logging, requests
from dataclasses import dataclass, field, asdict
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

# ── Env vars (set as GitHub Secrets) ──────────────────────────
SUPABASE_URL        = os.environ["SUPABASE_URL"]          # https://xxx.supabase.co
SUPABASE_KEY        = os.environ["SUPABASE_KEY"]          # anon or service_role key
TELEGRAM_BOT_TOKEN  = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID    = os.environ.get("TELEGRAM_CHAT_ID",   "")

# ── Config (mirrors config.py) ────────────────────────────────
SYMBOL              = "NIFTY"
NIFTY_STEP          = 50
NUM_EXPIRIES        = 2
STRIKES_AROUND_ATM  = 12
RISK_FREE_RATE      = 0.068

PCR_CALL_BUY        = 0.70
PCR_PUT_BUY         = 1.30
IV_RANK_BUY_MAX     = 40
IV_RANK_EXIT_MIN    = 70
DELTA_MIN           = 0.25
DELTA_MAX           = 0.60
OI_BUILDUP_PCT      = 15.0
OI_UNWIND_PCT       = -10.0
TARGET_PROFIT_PCT   = 50
STOP_LOSS_PCT       = 30
MIN_SIGNAL_SCORE    = 6

IST_OPEN    = datetime.time(9, 14)   # slightly before 9:15
IST_CLOSE   = datetime.time(15, 31)  # slightly after 15:30

# ══════════════════════════════════════════════════════════════
#  MARKET HOURS CHECK
# ══════════════════════════════════════════════════════════════

def is_market_open() -> bool:
    """Return True only during NSE trading hours (IST)."""
    now_ist = datetime.datetime.utcnow() + datetime.timedelta(hours=5, minutes=30)
    if now_ist.weekday() >= 5:          # Saturday=5, Sunday=6
        log.info("Market closed (weekend)")
        return False
    t = now_ist.time()
    if not (IST_OPEN <= t <= IST_CLOSE):
        log.info(f"Market closed at {t} IST")
        return False
    return True


# ══════════════════════════════════════════════════════════════
#  NSE DATA FETCH
# ══════════════════════════════════════════════════════════════

NSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer":         "https://www.nseindia.com/",
}


def fetch_nse_option_chain(symbol: str = SYMBOL, retries: int = 3) -> dict:
    """
    Fetch option chain from NSE. Manages cookie session and retries.
    NSE requires a browser-like session: hit homepage first to get cookies.
    """
    sess = requests.Session()
    sess.headers.update(NSE_HEADERS)

    # Warm up cookies (NSE blocks cookie-less API calls)
    warmup_urls = [
        "https://www.nseindia.com",
        "https://www.nseindia.com/market-data/live-equity-market?symbol=NIFTY",
    ]
    for url in warmup_urls:
        try:
            resp = sess.get(url, timeout=12)
            log.info(f"Warmed up: {url} → {resp.status_code}")
            time.sleep(1.5)
        except Exception as e:
            log.warning(f"Warmup failed {url}: {e}")

    api_url = f"https://www.nseindia.com/api/option-chain-indices?symbol={symbol}"

    for attempt in range(1, retries + 1):
        try:
            r = sess.get(api_url, timeout=15)
            if r.status_code == 200:
                data = r.json()
                if data.get("records", {}).get("data"):
                    log.info(f"NSE fetch OK ({len(data['records']['data'])} rows)")
                    return data
                else:
                    log.warning("Empty data in NSE response")
            else:
                log.warning(f"NSE returned {r.status_code} (attempt {attempt})")
        except Exception as e:
            log.warning(f"NSE fetch error attempt {attempt}: {e}")

        if attempt < retries:
            time.sleep(5 * attempt)   # backoff: 5s, 10s

    raise RuntimeError("NSE fetch failed after all retries")


# ══════════════════════════════════════════════════════════════
#  PARSE & GREEKS
# ══════════════════════════════════════════════════════════════

def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2)))

def _norm_pdf(x: float) -> float:
    return math.exp(-0.5 * x * x) / math.sqrt(2 * math.pi)

def bs_greeks(S, K, dte, iv_pct, opt_type) -> dict:
    empty = dict(delta=0.0, gamma=0.0, theta=0.0, vega=0.0)
    if iv_pct <= 0 or S <= 0 or K <= 0 or dte <= 0:
        return empty
    T, r, sig = dte / 365.0, RISK_FREE_RATE, iv_pct / 100.0
    try:
        d1 = (math.log(S / K) + (r + 0.5 * sig**2) * T) / (sig * math.sqrt(T))
        d2 = d1 - sig * math.sqrt(T)
        gamma = round(_norm_pdf(d1) / (S * sig * math.sqrt(T)), 6)
        vega  = round(S * _norm_pdf(d1) * math.sqrt(T) / 100, 4)
        if opt_type == "CE":
            delta = round(_norm_cdf(d1), 4)
            theta = round((-(S * _norm_pdf(d1) * sig) / (2 * math.sqrt(T))
                           - r * K * math.exp(-r * T) * _norm_cdf(d2)) / 365, 2)
        else:
            delta = round(_norm_cdf(d1) - 1, 4)
            theta = round((-(S * _norm_pdf(d1) * sig) / (2 * math.sqrt(T))
                           + r * K * math.exp(-r * T) * _norm_cdf(-d2)) / 365, 2)
        return dict(delta=delta, gamma=gamma, theta=theta, vega=vega)
    except (ValueError, ZeroDivisionError):
        return empty


def parse_chain(raw: dict, num_expiries: int = NUM_EXPIRIES) -> tuple:
    """Returns (rows: list[dict], spot: float, expiries: list[str])"""
    records     = raw.get("records", {})
    all_data    = records.get("data", [])
    spot        = float(records.get("underlyingValue", 0))
    all_expiries = records.get("expiryDates", [])
    target_exps = set(all_expiries[:num_expiries])
    atm         = int(round(spot / NIFTY_STEP) * NIFTY_STEP)

    rows = []
    for item in all_data:
        exp_str = item.get("expiryDate", "")
        if exp_str not in target_exps:
            continue
        try:
            exp_date = datetime.datetime.strptime(exp_str, "%d-%b-%Y").date()
        except ValueError:
            continue
        dte = max((exp_date - datetime.date.today()).days, 1)

        for ot in ("CE", "PE"):
            opt = item.get(ot)
            if not opt:
                continue
            strike  = float(item.get("strikePrice", opt.get("strikePrice", 0)))
            # Only keep strikes near ATM
            if abs(strike - atm) > STRIKES_AROUND_ATM * NIFTY_STEP:
                continue
            ltp     = float(opt.get("lastPrice", 0))
            oi      = int(opt.get("openInterest", 0))
            oi_chg  = int(opt.get("changeinOpenInterest", 0))
            vol     = int(opt.get("totalTradedVolume", 0))
            iv      = float(opt.get("impliedVolatility", 0))
            oi_base = max(oi - oi_chg, 1)
            oi_pct  = round((oi_chg / oi_base) * 100, 1)
            oi_flag = ("buildup" if oi_pct >= OI_BUILDUP_PCT else
                       "unwinding" if oi_pct <= OI_UNWIND_PCT else "neutral")
            g = bs_greeks(spot, strike, dte, iv, ot)
            rows.append({
                "strike": int(strike), "type": ot,
                "expiry": exp_date.isoformat(),
                "ltp": ltp, "oi": oi, "oi_change": oi_chg,
                "oi_change_pct": oi_pct, "volume": vol, "iv": iv,
                "delta": g["delta"], "gamma": g["gamma"],
                "theta": g["theta"], "vega": g["vega"],
                "oi_flag": oi_flag,
                "tradingsymbol": _sym(SYMBOL, exp_date, int(strike), ot),
                "updated_at": datetime.datetime.utcnow().isoformat() + "Z",
            })

    return rows, spot, all_expiries[:num_expiries]


def _sym(sym, exp, strike, ot):
    return f"{sym}{exp.strftime('%y%b').upper()}{strike}{ot}"


# ══════════════════════════════════════════════════════════════
#  DERIVED METRICS
# ══════════════════════════════════════════════════════════════

def compute_pcr(rows: list) -> float:
    put_oi  = sum(r["oi"] for r in rows if r["type"] == "PE")
    call_oi = sum(r["oi"] for r in rows if r["type"] == "CE")
    return round(put_oi / call_oi, 3) if call_oi > 0 else 1.0


def compute_max_pain(rows: list) -> int:
    strikes = sorted(set(r["strike"] for r in rows))
    calls   = {r["strike"]: r["oi"] for r in rows if r["type"] == "CE"}
    puts    = {r["strike"]: r["oi"] for r in rows if r["type"] == "PE"}
    best_s, best_pain = strikes[0], float("inf")
    for s in strikes:
        pain = (sum(max(0, s - k) * calls.get(k, 0) for k in strikes) +
                sum(max(0, k - s) * puts.get(k, 0)  for k in strikes))
        if pain < best_pain:
            best_pain, best_s = pain, s
    return int(best_s)


def compute_iv_rank(current_iv: float, history: list) -> float:
    if len(history) < 5:
        return -1.0
    lo, hi = min(history), max(history)
    return round((current_iv - lo) / (hi - lo) * 100, 1) if hi != lo else 50.0


def get_atm_iv(rows: list, atm: int) -> float:
    vals = [r["iv"] for r in rows if r["strike"] == atm and r["iv"] > 0]
    return round(sum(vals) / len(vals), 2) if vals else 0.0


# ══════════════════════════════════════════════════════════════
#  SIGNAL ENGINE
# ══════════════════════════════════════════════════════════════

def score_row(row: dict, side: str, pcr: float, iv_rank: float, max_pain: int, spot: float):
    score, reasons = 0, []
    delta  = abs(row["delta"])
    oi_pct = row["oi_change_pct"]
    oi_flag = row["oi_flag"]
    vol    = row["volume"]
    gamma  = row["gamma"]
    is_call = (side == "BUY_CALL")

    # PCR (2 pts)
    if is_call and pcr <= PCR_CALL_BUY:
        score += 2; reasons.append(f"PCR {pcr:.2f} → bullish")
    elif is_call and pcr <= 0.88:
        score += 1; reasons.append(f"PCR {pcr:.2f} → mildly bullish")
    elif not is_call and pcr >= PCR_PUT_BUY:
        score += 2; reasons.append(f"PCR {pcr:.2f} → bearish")
    elif not is_call and pcr >= 1.12:
        score += 1; reasons.append(f"PCR {pcr:.2f} → mildly bearish")

    # IV Rank (2 pts)
    if 0 <= iv_rank <= 20:
        score += 2; reasons.append(f"IV Rank {iv_rank:.0f}% — very cheap ✅")
    elif 20 < iv_rank <= IV_RANK_BUY_MAX:
        score += 1; reasons.append(f"IV Rank {iv_rank:.0f}% — cheap")
    elif iv_rank < 0:
        pass   # not enough history yet

    # OI buildup (2 pts)
    if oi_flag == "buildup":
        score += 2; reasons.append(f"OI buildup +{oi_pct:.0f}%")
    elif oi_pct >= 7:
        score += 1; reasons.append(f"OI growing +{oi_pct:.0f}%")

    # Delta sweet spot (1 pt)
    if DELTA_MIN <= delta <= DELTA_MAX:
        score += 1; reasons.append(f"Δ {delta:.2f} in sweet zone")
    elif delta < DELTA_MIN:
        score -= 1

    # Volume (1 pt)
    if vol >= 1000:
        score += 1; reasons.append(f"Vol {vol:,}")

    # Max pain alignment (1 pt)
    if max_pain > 0:
        if is_call and spot > max_pain:
            score += 1; reasons.append(f"Spot {spot:.0f} > MaxPain {max_pain}")
        elif not is_call and spot < max_pain:
            score += 1; reasons.append(f"Spot {spot:.0f} < MaxPain {max_pain}")

    # Gamma (1 pt)
    if gamma >= 0.0005:
        score += 1; reasons.append(f"Γ {gamma:.5f} near ATM")

    return max(score, 0), reasons


def generate_signals(rows: list, pcr: float, iv_rank: float, max_pain: int, spot: float) -> list:
    signals = []
    for row in rows:
        otype = row["type"]
        side  = "BUY_CALL" if otype == "CE" else "BUY_PUT"
        sc, reasons = score_row(row, side, pcr, iv_rank, max_pain, spot)
        kind = side if sc >= MIN_SIGNAL_SCORE else ("WATCH" if sc >= 4 else None)
        if kind:
            signals.append({
                "kind": kind, "tradingsymbol": row["tradingsymbol"],
                "strike": row["strike"], "expiry": row["expiry"],
                "ltp": row["ltp"], "score": sc, "reasons": reasons,
                "delta": row["delta"], "gamma": row["gamma"],
                "theta": row["theta"], "vega": row["vega"],
                "iv": row["iv"], "oi": row["oi"], "oi_flag": row["oi_flag"],
                "oi_change_pct": row["oi_change_pct"],
                "pcr": pcr, "iv_rank": iv_rank, "max_pain": max_pain, "spot": spot,
                "created_at": datetime.datetime.utcnow().isoformat() + "Z",
            })
    signals.sort(key=lambda s: -s["score"])
    return signals


def generate_exit_signals(positions: list, rows: list, pcr: float, iv_rank: float) -> list:
    exits = []
    row_map = {(r["tradingsymbol"],): r for r in rows}
    strike_map = {(r["strike"], r["type"]): r for r in rows}

    for pos in positions:
        sym    = pos.get("tradingsymbol", "")
        bp     = float(pos.get("buy_price", 0))
        ptype  = pos.get("type", "CE")
        strike = pos.get("strike", 0)
        expiry = pos.get("expiry", "")

        row = (row_map.get((sym,)) or
               strike_map.get((strike, ptype)))
        if not row:
            continue

        ltp     = row["ltp"]
        iv      = row["iv"]
        delta   = abs(row["delta"])
        oi_flag = row["oi_flag"]
        reasons, sc = [], 0

        if bp > 0:
            pnl = (ltp - bp) / bp * 100
            if pnl >= TARGET_PROFIT_PCT:
                reasons.append(f"🎯 Profit target +{pnl:.0f}%"); sc += 5
            elif pnl <= -STOP_LOSS_PCT:
                reasons.append(f"🛑 Stop-loss {pnl:.0f}%"); sc += 5

        if iv_rank >= IV_RANK_EXIT_MIN:
            reasons.append(f"IV Rank {iv_rank:.0f}% — expensive, book"); sc += 3
        if oi_flag == "unwinding":
            reasons.append("OI unwinding — exit"); sc += 3
        if delta > 0.75:
            reasons.append(f"|Δ| {delta:.2f} — deep ITM"); sc += 2
        if ptype == "CE" and pcr >= PCR_PUT_BUY:
            reasons.append(f"PCR {pcr:.2f} flipped bearish"); sc += 2
        elif ptype == "PE" and pcr <= PCR_CALL_BUY:
            reasons.append(f"PCR {pcr:.2f} flipped bullish"); sc += 2

        try:
            exp_d = datetime.date.fromisoformat(expiry)
            if (exp_d - datetime.date.today()).days <= 1:
                reasons.append("⏰ Expiry tomorrow — exit!"); sc += 4
        except Exception:
            pass

        if sc >= 4:
            exits.append({
                "kind": "EXIT", "tradingsymbol": sym,
                "strike": strike, "expiry": expiry, "ltp": ltp,
                "score": sc, "reasons": reasons,
                "delta": row["delta"], "gamma": row["gamma"],
                "theta": row["theta"], "vega": row["vega"],
                "iv": iv, "oi": row["oi"], "oi_flag": oi_flag,
                "oi_change_pct": row["oi_change_pct"],
                "pcr": pcr, "iv_rank": iv_rank, "max_pain": 0, "spot": row.get("spot", 0),
                "created_at": datetime.datetime.utcnow().isoformat() + "Z",
            })
    return exits


# ══════════════════════════════════════════════════════════════
#  SUPABASE WRITER
# ══════════════════════════════════════════════════════════════

def sb_headers():
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",
    }

def sb_upsert(table: str, data: list | dict, on_conflict: str = ""):
    if not data:
        return
    if isinstance(data, dict):
        data = [data]
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    params = {}
    if on_conflict:
        params["on_conflict"] = on_conflict
    r = requests.post(url, headers=sb_headers(), params=params, json=data, timeout=15)
    if not r.ok:
        log.error(f"Supabase upsert {table}: {r.status_code} {r.text[:200]}")
    else:
        log.info(f"Supabase upsert {table}: {len(data)} rows OK")

def sb_delete(table: str):
    """Delete all rows from a table (used to refresh signals each run)."""
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    r = requests.delete(url, headers={**sb_headers(), "Prefer": "return=minimal"},
                        params={"id": "gte.0"}, timeout=10)
    if not r.ok:
        log.warning(f"Supabase delete {table}: {r.status_code}")

def sb_select(table: str, cols: str = "*", limit: int = 500) -> list:
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    r = requests.get(url, headers=sb_headers(),
                     params={"select": cols, "limit": limit}, timeout=10)
    if r.ok:
        return r.json()
    log.warning(f"Supabase select {table}: {r.status_code}")
    return []


# ══════════════════════════════════════════════════════════════
#  TELEGRAM
# ══════════════════════════════════════════════════════════════

_SENT_TG: set = set()

def tg(text: str) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        r = requests.post(url, json={
            "chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"
        }, timeout=8)
        return r.ok
    except Exception as e:
        log.warning(f"Telegram error: {e}")
        return False


def tg_buy_alert(sig: dict) -> bool:
    key = (sig["tradingsymbol"], datetime.datetime.utcnow().strftime("%Y%m%d%H"))
    if key in _SENT_TG: return False
    _SENT_TG.add(key)
    kind   = sig["kind"]
    emoji  = "🟢🚀" if kind == "BUY_CALL" else "🔴📉"
    side   = "CALL" if kind == "BUY_CALL" else "PUT"
    ivr    = f"{sig['iv_rank']:.0f}%" if sig["iv_rank"] >= 0 else "building..."
    reasons = "\n".join(f"  ✅ {r}" for r in sig["reasons"])
    msg = (
        f"{emoji} <b>NIFTY {side} SIGNAL  [Score {sig['score']}/10]</b>\n\n"
        f"📌 <code>{sig['tradingsymbol']}</code>\n"
        f"💰 LTP ₹{sig['ltp']:.2f}  |  Strike {sig['strike']}  |  Expiry {sig['expiry']}\n"
        f"📊 Spot {sig['spot']:.0f}\n\n"
        f"<b>Greeks</b>  Δ{sig['delta']:.4f}  Γ{sig['gamma']:.5f}  Θ{sig['theta']:.2f}  ν{sig['vega']:.4f}\n"
        f"IV {sig['iv']:.1f}%  |  IV Rank {ivr}  |  OI {sig['oi']:,} ({sig['oi_flag']})\n"
        f"PCR {sig['pcr']:.3f}  |  MaxPain {sig['max_pain']}\n\n"
        f"<b>Why</b>\n{reasons}\n\n"
        f"⏰ {datetime.datetime.utcnow().strftime('%d %b %Y %H:%M')} UTC"
    )
    return tg(msg)


def tg_exit_alert(sig: dict, buy_price: float = 0) -> bool:
    key = ("EXIT", sig["tradingsymbol"], datetime.datetime.utcnow().strftime("%Y%m%d%H"))
    if key in _SENT_TG: return False
    _SENT_TG.add(key)
    pnl = ((sig["ltp"] - buy_price) / buy_price * 100) if buy_price > 0 else 0
    reasons = "\n".join(f"  ⚠️ {r}" for r in sig["reasons"])
    msg = (
        f"🟡 <b>EXIT — {sig['tradingsymbol']}</b>\n\n"
        f"₹{sig['ltp']:.2f}  |  Bought ₹{buy_price:.2f}  |  P&L {pnl:+.1f}%\n\n"
        f"<b>Reasons</b>\n{reasons}\n\n"
        f"⏰ {datetime.datetime.utcnow().strftime('%d %b %Y %H:%M')} UTC"
    )
    return tg(msg)


def tg_heartbeat(spot, pcr, iv_rank, max_pain):
    bias = "🐂 BULLISH" if pcr < 0.85 else ("🐻 BEARISH" if pcr > 1.2 else "⚖️ NEUTRAL")
    ivs  = (f"{iv_rank:.0f}% CHEAP ✅" if 0 <= iv_rank <= 40 else
            f"{iv_rank:.0f}% EXPENSIVE ⚠️" if iv_rank > 60 else
            f"{iv_rank:.0f}% Moderate") if iv_rank >= 0 else "building..."
    tg(f"📊 <b>NIFTY Hourly</b>\n"
       f"  Spot {spot:,.0f}  |  PCR {pcr:.3f} {bias}\n"
       f"  IV Rank {ivs}  |  MaxPain {max_pain:,}\n"
       f"  ⏰ {datetime.datetime.utcnow().strftime('%H:%M')} UTC")


# ══════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════

def main():
    log.info("=" * 60)
    log.info("Nifty Options Signal Engine — starting")

    if not is_market_open():
        log.info("Outside market hours — exiting")
        sys.exit(0)

    # ── Fetch NSE data ────────────────────────────────────────
    raw = fetch_nse_option_chain()
    rows, spot, expiries = parse_chain(raw)
    if not rows:
        log.error("No option chain rows parsed — exiting")
        sys.exit(1)
    log.info(f"Spot {spot}  |  Expiries {expiries}  |  Rows {len(rows)}")

    atm      = int(round(spot / NIFTY_STEP) * NIFTY_STEP)
    pcr      = compute_pcr(rows)
    max_pain = compute_max_pain(rows)
    atm_iv   = get_atm_iv(rows, atm)

    # ── IV Rank from Supabase history ────────────────────────
    hist_rows = sb_select("iv_history", "atm_iv", limit=252)
    iv_history = [float(r["atm_iv"]) for r in hist_rows if r.get("atm_iv")]
    iv_rank  = compute_iv_rank(atm_iv, iv_history)
    log.info(f"PCR {pcr}  MaxPain {max_pain}  ATM_IV {atm_iv}  IV_Rank {iv_rank}")

    # ── Get open positions ────────────────────────────────────
    positions = sb_select("positions")

    # ── Generate signals ──────────────────────────────────────
    entry_signals = generate_signals(rows, pcr, iv_rank, max_pain, spot)
    exit_signals  = generate_exit_signals(positions, rows, pcr, iv_rank)
    all_signals   = entry_signals + exit_signals
    log.info(f"Signals: {len(entry_signals)} entry, {len(exit_signals)} exit")

    # ── Write to Supabase ─────────────────────────────────────
    # 1. Snapshot (single row upsert)
    sb_upsert("snapshot", [{
        "id": 1, "spot": spot, "atm": atm, "pcr": pcr,
        "max_pain": max_pain, "atm_iv": atm_iv, "iv_rank": iv_rank,
        "updated_at": datetime.datetime.utcnow().isoformat() + "Z",
    }], on_conflict="id")

    # 2. Option chain (upsert by strike+type+expiry)
    sb_upsert("option_chain", rows, on_conflict="strike,type,expiry")

    # 3. Signals (clear + insert fresh)
    sb_delete("signals")
    if all_signals:
        # Supabase doesn't store Python lists natively — convert reasons to JSON string array
        for s in all_signals:
            s["reasons"] = s["reasons"]  # already list, Supabase handles as JSONB array
        sb_upsert("signals", all_signals)

    # 4. IV history
    if atm_iv > 0:
        sb_upsert("iv_history", [{"atm_iv": atm_iv}])

    # ── Telegram alerts ──────────────────────────────────────
    alert_log_entries = []
    for sig in entry_signals[:3]:    # top 3 entry signals
        if sig["score"] >= MIN_SIGNAL_SCORE:
            if tg_buy_alert(sig):
                alert_log_entries.append({
                    "kind": sig["kind"], "tradingsymbol": sig["tradingsymbol"],
                    "ltp": sig["ltp"], "score": sig["score"],
                })

    for sig in exit_signals:
        pos = next((p for p in positions
                    if p.get("tradingsymbol") == sig["tradingsymbol"]), {})
        if tg_exit_alert(sig, float(pos.get("buy_price", 0))):
            alert_log_entries.append({
                "kind": "EXIT", "tradingsymbol": sig["tradingsymbol"],
                "ltp": sig["ltp"], "score": sig["score"],
            })

    if alert_log_entries:
        sb_upsert("alert_log", alert_log_entries)

    # Hourly heartbeat
    now_utc = datetime.datetime.utcnow()
    if now_utc.minute < 6:   # roughly once per hour (within first 5 min)
        tg_heartbeat(spot, pcr, iv_rank, max_pain)

    log.info("Done ✓")


if __name__ == "__main__":
    main()
