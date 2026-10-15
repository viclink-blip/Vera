#!/usr/bin/env python3
"""
Vera Bot - MA-crossover trading bot for Deriv.

Strategy : MA7 / MA14 crossover on 1-minute candles, filtered by RSI(14).
           MA7 crosses above MA14 (and RSI not overbought) -> Rise / Up
           MA7 crosses below MA14 (and RSI not oversold)   -> Fall / Down
Symbols  : any Deriv symbol (R_75, 1HZ100V, BOOM1000, CRASH500, frxEURUSD,
           frxXAUUSD, ...). Uses Rise/Fall where the symbol offers it, else
           Multipliers (e.g. Boom/Crash).
API      : https://api.derivws.com (REST login) -> one-time authenticated WebSocket
Auth     : DERIV_TOKEN (pat_... personal access token) + DERIV_APP_ID in .env.
           The bot gets a one-time WebSocket URL from https://api.derivws.com
           for the account chosen with ACCOUNT_TYPE (demo / real).

Run      : python3 vera_bot.py              (uses SYMBOL below)
           python3 vera_bot.py BOOM1000     (override the symbol)
           python3 vera_bot.py --symbols    (list active symbols and exit)
"""

import asyncio
import csv
import json
import os
import signal
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import websockets
import websockets.exceptions
from dotenv import load_dotenv

load_dotenv(Path(__file__).with_name(".env"))

# ============================================================================
#  SETTINGS - edit these
# ============================================================================
ACCOUNT_TYPE = "demo"        # "demo" or "real" - which of your Deriv accounts to use

SYMBOL = "R_75"              # any Deriv symbol, e.g. R_75, R_100, 1HZ100V, BOOM1000,
                             # CRASH500, JD25, frxEURUSD, frxXAUUSD, cryBTCUSD ...
                             # run `python3 vera_bot.py --symbols` to see them all
SHOW_ACTIVE_SYMBOLS = True   # print every active Deriv symbol at startup

CONTRACT_MODE = "auto"       # "auto": Rise/Fall when the symbol offers it, else Multipliers
                             # "risefall" or "multiplier" to force one
STAKE = 1                    # USD per trade (Deriv minimum is 0.35)
CURRENCY = "USD"

DURATION = 5                 # contract length...
DURATION_UNIT = "t"          # ..."t" ticks, "s" seconds, "m" minutes, "h", "d"
AUTO_ADJUST_DURATION = True  # if DURATION isn't offered for SYMBOL (e.g. ticks on
                             # forex), switch to the shortest duration Deriv allows

MULTIPLIER = 100             # Multiplier contracts: x100 (nearest allowed value is used)
MULT_HOLD_SECONDS = 60       # close a Multiplier position after this many seconds
MULT_TAKE_PROFIT = 0         # USD, Deriv closes the position at this profit (0 = off)
MULT_STOP_LOSS = 0           # USD, Deriv closes the position at this loss (0 = off;
                             # the loss can never exceed the stake anyway)

MA_FAST = 7
MA_SLOW = 14
CANDLE_GRANULARITY = 60      # seconds -> 1-minute candles

RSI_PERIOD = 14
RSI_FILTER = True            # skip Rise when RSI >= RSI_OVERBOUGHT, Fall when RSI <= RSI_OVERSOLD
RSI_OVERBOUGHT = 70
RSI_OVERSOLD = 30

MARTINGALE = False           # after a loss, next stake = previous stake x MULTIPLIER
MARTINGALE_MULTIPLIER = 2.1
MARTINGALE_MAX_STEPS = 5     # reset to STAKE after this many losses in a row

DAILY_STOP_LOSS = 10         # stop the bot when today's P/L reaches -10 USD
DAILY_TAKE_PROFIT = 0        # stop when today's P/L reaches +X USD (0 = disabled)
BANKROLL = 50                # trade as if the account held only this much, e.g. 50
                             # (0 = use the whole account balance). It carries over
                             # between runs; rename vera_trades.csv to start it over.

DRY_RUN = True               # True = simulate trades on live prices, no real buys
DRY_RUN_START_BALANCE = 1000.0  # simulated balance when no token is set

KEEPALIVE_SECONDS = 30      # send {"ping": 1} this often - Deriv drops idle connections
APP_ID = os.getenv("DERIV_APP_ID", "").strip()   # your app ID from developers.deriv.com
REST_URL = os.getenv("DERIV_REST_URL") or "https://api.derivws.com"
LEGACY_WS_URL = "wss://ws.derivws.com/websockets/v3?app_id=1089"   # old-style tokens only
API_URL = os.getenv("DERIV_API_URL")   # optional override of the WebSocket URL (testing)
CSV_FILE = Path(__file__).with_name("vera_trades.csv")
# ============================================================================

CSV_COLUMNS = [
    "time", "mode", "account", "symbol", "direction", "contract_type",
    "duration", "stake", "ma_fast", "ma_slow", "rsi", "entry_spot", "exit_spot",
    "payout", "result", "profit", "daily_pnl", "balance", "contract_id",
]


def now_utc():
    return datetime.now(timezone.utc)


def log(msg):
    print(f"[{now_utc():%Y-%m-%d %H:%M:%S}] {msg}", flush=True)


class DerivAPIError(Exception):
    def __init__(self, error):
        self.code = error.get("code", "")
        super().__init__(f"{self.code}: {error.get('message', error)}")


class DerivAPI:
    """Single websocket connection with req_id based request/response routing."""

    def __init__(self, url):
        self.url = url
        self.ws = None
        self._req_id = 0
        self._futures = {}      # req_id -> Future   (one-shot requests)
        self._queues = {}       # req_id -> Queue    (subscriptions)
        self._reader = None
        self.close_info = ""

    async def connect(self):
        self.ws = await websockets.connect(
            self.url, ping_interval=30, ping_timeout=60, max_size=2**22
        )
        self._reader = asyncio.create_task(self._read_loop())

    async def close(self):
        if self._reader:
            self._reader.cancel()
        if self.ws:
            await self.ws.close()

    async def _read_loop(self):
        exc = ConnectionError("websocket closed")
        try:
            async for raw in self.ws:
                msg = json.loads(raw)
                rid = msg.get("req_id")
                fut = self._futures.pop(rid, None)
                if fut is not None and not fut.done():
                    fut.set_result(msg)
                elif rid in self._queues:
                    self._queues[rid].put_nowait(msg)
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001 - surfaced to waiters below
            exc = e
        finally:
            code = getattr(self.ws, "close_code", None)
            reason = getattr(self.ws, "close_reason", "") or ""
            self.close_info = f"code {code} {reason}".strip() if code else repr(exc)
            # wake everybody waiting so they can fail / reconnect
            for fut in self._futures.values():
                if not fut.done():
                    fut.set_exception(ConnectionError(str(exc)))
            self._futures.clear()
            for q in self._queues.values():
                q.put_nowait(None)

    def _next_id(self):
        self._req_id += 1
        return self._req_id

    async def request(self, payload, timeout=30):
        rid = self._next_id()
        payload = {**payload, "req_id": rid}
        fut = asyncio.get_running_loop().create_future()
        self._futures[rid] = fut
        await self.ws.send(json.dumps(payload))
        try:
            msg = await asyncio.wait_for(fut, timeout)
        finally:
            self._futures.pop(rid, None)
        if "error" in msg:
            raise DerivAPIError(msg["error"])
        return msg

    async def subscribe(self, payload):
        """Returns (req_id, queue). Every message for the subscription lands in
        the queue; None means the connection died."""
        rid = self._next_id()
        q = asyncio.Queue()
        self._queues[rid] = q
        await self.ws.send(json.dumps({**payload, "subscribe": 1, "req_id": rid}))
        return rid, q

    def unsubscribe_queue(self, rid):
        self._queues.pop(rid, None)

    async def forget(self, subscription_id):
        try:
            await self.request({"forget": subscription_id}, timeout=10)
        except Exception:  # noqa: BLE001 - best effort
            pass


def duration_to_seconds(value, unit):
    return value * {"s": 1, "m": 60, "h": 3600, "d": 86400}.get(unit, 0)


def parse_duration(text):
    """'5t' -> (5, 't'), '15m' -> (15, 'm')"""
    return int(text[:-1]), text[-1]


def rest_call(method, path, token):
    """Call Deriv's REST API with a PAT token. Returns the parsed JSON."""
    req = urllib.request.Request(REST_URL + path, method=method, data=b"" if method == "POST" else None)
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Deriv-App-ID", APP_ID)
    req.add_header("Accept", "application/json")
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read() or b"{}")
    except urllib.error.HTTPError as e:
        body = e.read().decode(errors="replace")[:300]
        if e.code in (401, 403):
            raise SystemExit(f"Deriv rejected the token or App ID (HTTP {e.code}): {body}\n"
                             "Check DERIV_TOKEN and DERIV_APP_ID in .env - the token must "
                             "belong to that app and have the trade scope.")
        raise ConnectionError(f"{method} {path} failed: HTTP {e.code} {body}")


def is_demo(account):
    kind = str(account.get("account_type") or account.get("type") or "").lower()
    return kind in ("demo", "virtual") or bool(account.get("is_virtual"))


def check_env():
    """Catch the common .env mistakes before connecting."""
    env = Path(__file__).with_name(".env")
    if not os.getenv("DERIV_TOKEN", "").strip():
        lines = env.read_text().split() if env.exists() else []
        if any(line.startswith("pat_") for line in lines):
            raise SystemExit(".env has the token on its own - the line must read: DERIV_TOKEN=pat_...")
        raise SystemExit(f"DERIV_TOKEN missing - put DERIV_TOKEN=... in {env}")


def explain_http_520(e):
    status = getattr(getattr(e, "response", None), "status_code", None)
    if status == 520:
        raise SystemExit("Deriv's old API server rejected the connection (HTTP 520). "
                         "Use a new pat_ token plus DERIV_APP_ID in .env (see README).")


async def get_session(token):
    """Return (websocket_url, account) for this run.
    PAT tokens: list accounts, pick ACCOUNT_TYPE, get a one-time authenticated URL.
    Old-style tokens: legacy URL, authorized with an `authorize` message (account=None)."""
    if API_URL:
        return API_URL, None
    if not token:
        raise SystemExit("DERIV_TOKEN missing in .env")
    if not token.startswith("pat_"):
        return LEGACY_WS_URL, None
    if not APP_ID:
        raise SystemExit("DERIV_APP_ID missing in .env - copy the App ID of the app you "
                         "created the token under at developers.deriv.com")
    data = (await asyncio.to_thread(rest_call, "GET", "/trading/v1/options/accounts", token)).get("data", [])
    accounts = [data] if isinstance(data, dict) else list(data)
    if not accounts:
        raise SystemExit("No Options accounts found for this token")
    want_demo = ACCOUNT_TYPE.lower() == "demo"
    matches = [a for a in accounts if is_demo(a) == want_demo]
    if not matches:
        found = ", ".join(f"{a.get('account_id')} ({'demo' if is_demo(a) else 'real'})" for a in accounts)
        raise SystemExit(f"No {ACCOUNT_TYPE} account for this token. Found: {found}")
    account = matches[0]
    otp = await asyncio.to_thread(
        rest_call, "POST", f"/trading/v1/options/accounts/{account['account_id']}/otp", token)
    url = (otp.get("data") or {}).get("url")
    if not url:
        raise ConnectionError(f"OTP response had no WebSocket URL: {str(otp)[:200]}")
    return url, account


class TradingBot:
    def __init__(self, token):
        self.token = token
        self.api = None
        self.account = "none"
        self.is_virtual = None
        self.balance = DRY_RUN_START_BALANCE
        self.currency = CURRENCY
        self.duration = DURATION
        self.duration_unit = DURATION_UNIT
        self.kind = "risefall"                  # or "multiplier" - set by resolve_contract()
        self.multiplier = MULTIPLIER
        self.symbols_shown = False
        self.sim_balance_set = False
        self.symbol_key = "underlying_symbol"   # new API name; falls back to "symbol"

        self.closes = pd.Series(dtype=float)   # closed candles, index = open_time
        self.current_open_time = None
        self.current_close = None
        self.last_tick_epoch = None

        self.stake = float(STAKE)
        self.loss_streak = 0
        self.trading_day = now_utc().date()
        self._migrate_csv()
        self.daily_pnl = self._load_today_pnl()

        self.trade_task = None
        self.pending_contract = None           # live contract to resume after reconnect
        self.tick_listeners = []               # dry-run simulators waiting on ticks
        self.stopped = False
        self.stop_reason = ""
        self.stop_event = asyncio.Event()

    # ------------------------------------------------------------------ setup
    @property
    def mode(self):
        return "DRY_RUN" if DRY_RUN else "LIVE"

    @staticmethod
    def _migrate_csv():
        """Add new columns (e.g. rsi) to a CSV written by an older version."""
        old_csv = CSV_FILE.with_name("deriv_trades.csv")   # name used by earlier versions
        if old_csv.exists() and not CSV_FILE.exists():
            old_csv.rename(CSV_FILE)
        if not CSV_FILE.exists():
            return
        with CSV_FILE.open() as f:
            header = f.readline().strip().split(",")
        if header != CSV_COLUMNS:
            pd.read_csv(CSV_FILE).reindex(columns=CSV_COLUMNS).to_csv(CSV_FILE, index=False)

    def _bankroll_balance(self):
        """BANKROLL plus the P/L of every trade logged for this account and mode."""
        total = 0.0
        if CSV_FILE.exists():
            try:
                df = pd.read_csv(CSV_FILE)
                mine = (df["mode"] == self.mode) & (df["account"].astype(str) == str(self.account))
                total = float(pd.to_numeric(df.loc[mine, "profit"], errors="coerce").sum())
            except Exception as e:  # noqa: BLE001
                log(f"Could not read {CSV_FILE.name} for the bankroll ({e})")
        return round(BANKROLL + total, 2)

    def set_balance(self, account_balance):
        """Set the balance the bot trades with, once per run for dry run / bankroll."""
        if self.sim_balance_set and (DRY_RUN or BANKROLL):
            return
        self.sim_balance_set = True
        if BANKROLL:
            self.balance = self._bankroll_balance()
            log(f"Bankroll mode: trading with {self.balance:.2f} of {BANKROLL:.2f} {self.currency} "
                f"(account holds {account_balance:.2f})")
        else:
            self.balance = account_balance

    def _load_today_pnl(self):
        """Resume today's P/L from the CSV so a restart doesn't reset the stop loss."""
        if not CSV_FILE.exists():
            return 0.0
        try:
            df = pd.read_csv(CSV_FILE)
            if df.empty:
                return 0.0
            ts = pd.to_datetime(df["time"], utc=True, errors="coerce")
            today = (ts.dt.date == now_utc().date()) & (df["mode"] == self.mode)
            return round(float(df.loc[today, "profit"].sum()), 2)
        except Exception as e:  # noqa: BLE001
            log(f"Could not read {CSV_FILE.name} ({e}); daily P/L starts at 0")
            return 0.0

    async def authorize(self, account):
        if account is not None:   # PAT flow: the socket is already logged in
            self.account = account["account_id"]
            self.is_virtual = is_demo(account)
            self.currency = account.get("currency") or CURRENCY
            try:
                bal = (await self.api.request({"balance": 1}))["balance"]
                balance = float(bal["balance"])
                self.currency = bal.get("currency") or self.currency
            except (DerivAPIError, KeyError, TypeError, ValueError):
                balance = float(account.get("balance") or 0)
            kind = "DEMO" if self.is_virtual else "REAL"
            log(f"Logged in {self.account} ({kind}) balance {balance:.2f} {self.currency}")
            self.set_balance(balance)
            return
        if not self.token:
            raise SystemExit("DERIV_TOKEN missing in .env")
        auth = (await self.api.request({"authorize": self.token}))["authorize"]
        self.account = auth["loginid"]
        self.is_virtual = bool(auth.get("is_virtual"))
        self.currency = auth.get("currency") or CURRENCY
        kind = "DEMO" if self.is_virtual else "REAL"
        log(f"Authorized {self.account} ({kind}) balance {float(auth['balance']):.2f} {self.currency}")
        self.set_balance(float(auth["balance"]))

    async def load_active_symbols(self, show):
        """Fetch every active Deriv symbol, optionally print them, and check SYMBOL."""
        resp = await self.api.request({"active_symbols": "brief"})
        symbols = resp["active_symbols"]
        for sym in symbols:
            normalize_symbol(sym)
        if show:
            print_symbols(symbols)
        by_code = {sym["symbol"]: sym for sym in symbols}
        info = by_code.get(SYMBOL)
        if info is None:
            hits = [c for c, sym in by_code.items()
                    if SYMBOL.lower() in (c.lower(), sym.get("display_name", "").lower())]
            hint = f" Did you mean {hits[0]}?" if hits else " Run with --symbols to list them."
            raise SystemExit(f"Symbol {SYMBOL!r} is not an active Deriv symbol.{hint}")
        status = "OPEN" if info.get("exchange_is_open") else "CLOSED"
        if info.get("is_trading_suspended"):
            status = "SUSPENDED"
        log(f"Symbol {SYMBOL} = {info.get('display_name')} "
            f"({info.get('market_display_name')} / {info.get('submarket_display_name')}) - market {status}")
        if status != "OPEN":
            log("Market not open now - trades will be rejected until it opens")

    async def resolve_contract(self):
        """Pick Rise/Fall or Multipliers for SYMBOL and check duration / multiplier."""
        try:
            try:
                resp = await self.api.request({"contracts_for": SYMBOL, "currency": self.currency})
            except DerivAPIError as e:
                if e.code != "InputValidationFailed":
                    raise
                resp = await self.api.request({"contracts_for": SYMBOL})   # new API: no currency
        except DerivAPIError as e:
            log(f"contracts_for failed ({e}); using Rise/Fall {DURATION}{DURATION_UNIT}")
            return
        available = resp["contracts_for"]["available"]
        calls = [c for c in available if c.get("contract_type") == "CALL"]
        # Rise/Fall = CALL without barriers. Field formats differ between API
        # versions (barriers 0 / "0" / missing), so only rule out clear mismatches.
        offers = [
            c for c in calls
            if str(c.get("barriers", 0)) in ("0", "", "None")
            and c.get("expiry_type") != "no_expiry"
        ] or calls
        mult_offers = [c for c in available if c["contract_type"] == "MULTUP"]
        types = sorted({c["contract_type"] for c in available})
        log(f"Contract types for {SYMBOL}: {', '.join(types)}")

        if CONTRACT_MODE == "multiplier" or (CONTRACT_MODE == "auto" and not offers):
            if not mult_offers:
                raise SystemExit(f"Neither Rise/Fall nor Multipliers are offered for {SYMBOL}")
            self.kind = "multiplier"
            allowed_mults = sorted({int(m) for c in mult_offers for m in c.get("multiplier_range", [])})
            if allowed_mults and MULTIPLIER not in allowed_mults:
                self.multiplier = min(allowed_mults, key=lambda m: (abs(m - MULTIPLIER), m))
                log(f"Multiplier x{MULTIPLIER} not offered for {SYMBOL}; using x{self.multiplier} "
                    f"(allowed {allowed_mults})")
            log(f"Contract: Multipliers Up/Down x{self.multiplier}, close after {MULT_HOLD_SECONDS}s")
            return
        if not offers:
            raise SystemExit(f"Rise/Fall is not offered for {SYMBOL} (types: {', '.join(types)})")
        self.kind = "risefall"

        ranges = []
        for c in offers:
            try:
                ranges.append((parse_duration(c["min_contract_duration"]),
                               parse_duration(c["max_contract_duration"])))
            except (KeyError, ValueError, TypeError, IndexError):
                pass
        if not ranges:   # duration format unknown - let the proposal validate it
            log(f"Contract: Rise/Fall {DURATION}{DURATION_UNIT} (durations not listed: "
                f"{json.dumps(offers[0])[:300]})")
            return

        def allowed(value, unit):
            for (lo_v, lo_u), (hi_v, hi_u) in ranges:
                if unit == "t" and lo_u == "t" and lo_v <= value <= hi_v:
                    return True
                if unit != "t" and lo_u != "t":
                    s = duration_to_seconds(value, unit)
                    if duration_to_seconds(lo_v, lo_u) <= s <= duration_to_seconds(hi_v, hi_u):
                        return True
            return False

        if allowed(DURATION, DURATION_UNIT):
            log(f"Contract: Rise/Fall {DURATION}{DURATION_UNIT}")
            return
        mins = [lo for lo, _ in ranges]
        if not AUTO_ADJUST_DURATION:
            raise SystemExit(f"{DURATION}{DURATION_UNIT} not allowed for {SYMBOL}. "
                             f"Minimums offered: {[f'{v}{u}' for v, u in mins]}")
        # prefer ticks, else shortest time based duration
        ticks = [m for m in mins if m[1] == "t"]
        best = min(ticks) if ticks else min(mins, key=lambda m: duration_to_seconds(*m))
        self.duration, self.duration_unit = best
        log(f"{DURATION}{DURATION_UNIT} not offered for {SYMBOL}; using {best[0]}{best[1]}")

    # ------------------------------------------------------------- strategy
    def _ma(self):
        fast = self.closes.rolling(MA_FAST).mean()
        slow = self.closes.rolling(MA_SLOW).mean()
        return fast, slow

    def _rsi(self):
        """Wilder's RSI on candle closes."""
        delta = self.closes.diff()
        gain = delta.clip(lower=0).ewm(alpha=1 / RSI_PERIOD, adjust=False).mean()
        loss = (-delta.clip(upper=0)).ewm(alpha=1 / RSI_PERIOD, adjust=False).mean()
        rs = gain / loss
        return (100 - 100 / (1 + rs)).fillna(100.0).iloc[-1]   # no losses -> RSI 100

    def on_candle_closed(self, open_time, close):
        self.closes.loc[open_time] = close
        self.closes = self.closes.iloc[-300:]
        need = max(MA_SLOW, RSI_PERIOD) + 1
        if len(self.closes) < need:
            log(f"Collecting candles {len(self.closes)}/{need}")
            return
        fast, slow = self._ma()
        f_prev, s_prev, f_now, s_now = fast.iloc[-2], slow.iloc[-2], fast.iloc[-1], slow.iloc[-1]
        rsi = self._rsi()
        log(f"{SYMBOL} candle close {close} | MA{MA_FAST}={f_now:.5f} MA{MA_SLOW}={s_now:.5f} "
            f"RSI{RSI_PERIOD}={rsi:.1f}")

        direction = None
        if f_prev <= s_prev and f_now > s_now:
            direction = "RISE"
        elif f_prev >= s_prev and f_now < s_now:
            direction = "FALL"
        if not direction:
            return
        log(f">>> Signal {direction}: MA{MA_FAST} crossed {'above' if direction == 'RISE' else 'below'} "
            f"MA{MA_SLOW} | RSI {rsi:.1f}")
        if RSI_FILTER:
            if direction == "RISE" and rsi >= RSI_OVERBOUGHT:
                log(f"RSI {rsi:.1f} >= {RSI_OVERBOUGHT} (overbought) - Rise skipped")
                return
            if direction == "FALL" and rsi <= RSI_OVERSOLD:
                log(f"RSI {rsi:.1f} <= {RSI_OVERSOLD} (oversold) - Fall skipped")
                return
        if self.trade_task and not self.trade_task.done():
            log("Trade still open - signal skipped")
            return
        self.trade_task = asyncio.create_task(self.trade(direction, f_now, s_now, rsi))

    def on_tick(self, epoch, quote):
        self.last_tick_epoch = epoch
        for listener in list(self.tick_listeners):
            listener(epoch, quote)

    # --------------------------------------------------------------- risk
    def check_day_rollover(self):
        today = now_utc().date()
        if today != self.trading_day:
            log(f"New day {today} - daily P/L reset (was {self.daily_pnl:+.2f})")
            self.trading_day = today
            self.daily_pnl = 0.0

    def risk_allows(self, stake):
        self.check_day_rollover()
        if self.daily_pnl <= -DAILY_STOP_LOSS:
            self.stop(f"Daily stop loss hit ({self.daily_pnl:+.2f} {self.currency})")
            return False
        if DAILY_TAKE_PROFIT and self.daily_pnl >= DAILY_TAKE_PROFIT:
            self.stop(f"Daily take profit hit ({self.daily_pnl:+.2f} {self.currency})")
            return False
        if self.daily_pnl - stake < -DAILY_STOP_LOSS:
            self.stop(f"Next stake {stake:.2f} could exceed daily stop loss "
                      f"(P/L {self.daily_pnl:+.2f}, limit -{DAILY_STOP_LOSS})")
            return False
        if stake > self.balance:
            what = "Bankroll" if BANKROLL else "Balance"
            self.stop(f"{what} {self.balance:.2f} too low for stake {stake:.2f}")
            return False
        return True

    def stop(self, reason):
        if not self.stopped:
            self.stopped = True
            self.stop_reason = reason
            self.stop_event.set()
            log(f"STOPPING BOT: {reason}")

    def update_stake(self, won):
        if not MARTINGALE or won:
            self.loss_streak = 0
            self.stake = float(STAKE)
            return
        self.loss_streak += 1
        if self.loss_streak >= MARTINGALE_MAX_STEPS:
            log(f"Martingale: {self.loss_streak} losses in a row - reset stake")
            self.loss_streak = 0
            self.stake = float(STAKE)
        else:
            self.stake = round(self.stake * MARTINGALE_MULTIPLIER, 2)
            log(f"Martingale: next stake {self.stake:.2f}")

    # -------------------------------------------------------------- trading
    def proposal_request(self, direction, stake):
        """Build the proposal for the contract kind SYMBOL supports."""
        req = {"proposal": 1, "amount": stake, "basis": "stake",
               "currency": self.currency, self.symbol_key: SYMBOL}
        if self.kind == "multiplier":
            req["contract_type"] = "MULTUP" if direction == "RISE" else "MULTDOWN"
            req["multiplier"] = self.multiplier
            limit = {}
            if MULT_TAKE_PROFIT:
                limit["take_profit"] = MULT_TAKE_PROFIT
            if MULT_STOP_LOSS:
                limit["stop_loss"] = MULT_STOP_LOSS
            if limit:
                req["limit_order"] = limit
        else:
            req["contract_type"] = "CALL" if direction == "RISE" else "PUT"
            req["duration"] = self.duration
            req["duration_unit"] = self.duration_unit
        return req

    @property
    def duration_label(self):
        if self.kind == "multiplier":
            return f"x{self.multiplier}/{MULT_HOLD_SECONDS}s"
        return f"{self.duration}{self.duration_unit}"

    async def trade(self, direction, ma_fast, ma_slow, rsi):
        stake = round(self.stake, 2)
        if self.stopped or not self.risk_allows(stake):
            return
        req = self.proposal_request(direction, stake)
        contract_type = req["contract_type"]
        try:
            try:
                proposal = (await self.api.request(req))["proposal"]
            except DerivAPIError as e:
                if e.code != "InputValidationFailed":
                    raise
                # API version uses the other field name for the symbol - switch and retry
                other = "symbol" if self.symbol_key == "underlying_symbol" else "underlying_symbol"
                req[other] = req.pop(self.symbol_key)
                proposal = (await self.api.request(req))["proposal"]
                self.symbol_key = other
        except DerivAPIError as e:
            log(f"Proposal rejected: {e}")
            return
        payout = float(proposal.get("payout") or 0)
        extra = (f"x{self.multiplier} commission {float(proposal.get('commission') or 0):.2f}"
                 if self.kind == "multiplier" else f"payout {payout:.2f}")
        log(f"Proposal {contract_type} {SYMBOL} stake {stake:.2f} {extra} spot {proposal.get('spot')}")

        if DRY_RUN:
            if self.kind == "multiplier":
                entry, exit_, profit = await self.simulate_multiplier(
                    contract_type, stake, float(proposal.get("commission") or 0))
                won = profit > 0
            else:
                entry, exit_, won = await self.simulate(contract_type)
                profit = round(payout - stake, 2) if won else -stake
            self.balance = round(self.balance + profit, 2)
            self.finish_trade(direction, contract_type, stake, ma_fast, ma_slow, rsi,
                              entry, exit_, payout, won, profit, "dry-run")
            return

        try:
            buy = (await self.api.request({"buy": proposal["id"], "price": proposal["ask_price"]}))["buy"]
        except DerivAPIError as e:
            log(f"Buy failed: {e}")
            return
        contract_id = buy["contract_id"]
        log(f"BOUGHT {contract_type} #{contract_id} for {float(buy['buy_price']):.2f} "
            f"(balance {float(buy['balance_after']):.2f})")
        self.pending_contract = dict(direction=direction, contract_type=contract_type, stake=stake,
                                     ma_fast=ma_fast, ma_slow=ma_slow, rsi=rsi,
                                     contract_id=contract_id, kind=self.kind,
                                     sell_at=time.time() + MULT_HOLD_SECONDS)
        await self.watch_contract()

    async def watch_contract(self):
        """Follow a live contract until it is sold/expired, then record it."""
        info = self.pending_contract
        rid, q = await self.api.subscribe({"proposal_open_contract": 1,
                                           "contract_id": info["contract_id"]})
        sub_id = None
        sell_sent = False
        try:
            while True:
                timeout = 600
                if info["kind"] == "multiplier" and not sell_sent:
                    timeout = max(info["sell_at"] - time.time(), 0)
                try:
                    msg = await asyncio.wait_for(q.get(), timeout=timeout)
                except asyncio.TimeoutError:
                    if info["kind"] != "multiplier" or sell_sent:
                        raise
                    sell_sent = True
                    await self.sell(info["contract_id"])
                    continue
                if msg is None:
                    raise ConnectionError("connection lost while watching contract")
                if "error" in msg:
                    raise DerivAPIError(msg["error"])
                sub_id = (msg.get("subscription") or {}).get("id", sub_id)
                poc = msg["proposal_open_contract"]
                if poc.get("is_sold") or poc.get("status") in ("won", "lost", "sold"):
                    break
        finally:
            self.api.unsubscribe_queue(rid)
            if sub_id and self.api.ws:
                asyncio.create_task(self.api.forget(sub_id))

        self.pending_contract = None
        profit = round(float(poc["profit"]), 2)
        try:
            if BANKROLL:
                raise ValueError("bankroll mode tracks its own balance")
            self.balance = float((await self.api.request({"balance": 1}))["balance"]["balance"])
        except Exception:  # noqa: BLE001
            self.balance = round(self.balance + profit, 2)
        self.finish_trade(info["direction"], info["contract_type"], info["stake"],
                          info["ma_fast"], info["ma_slow"], info["rsi"],
                          poc.get("entry_spot") or poc.get("entry_tick"),
                          poc.get("exit_tick") or poc.get("sell_spot") or poc.get("current_spot"),
                          float(poc.get("payout", 0)), profit > 0, profit, info["contract_id"])

    async def sell(self, contract_id):
        """Close an open contract (Multipliers) at market price."""
        try:
            sold = (await self.api.request({"sell": contract_id, "price": 0}))["sell"]
            log(f"SOLD #{contract_id} for {float(sold['sold_for']):.2f}")
        except DerivAPIError as e:   # e.g. already closed by take profit / stop out
            log(f"Sell #{contract_id}: {e}")

    async def simulate_multiplier(self, contract_type, stake, commission):
        """Dry run for Multipliers: P/L = stake x multiplier x price change % - commission,
        closed after MULT_HOLD_SECONDS, at take profit / stop loss, or when the stake is lost."""
        loop = asyncio.get_running_loop()
        done = loop.create_future()
        state = {"entry": None, "entry_epoch": None}
        sign = 1 if contract_type == "MULTUP" else -1

        def listener(epoch, quote):
            if done.done():
                return
            if state["entry"] is None:
                state.update(entry=quote, entry_epoch=epoch)
                return
            pnl = stake * self.multiplier * sign * (quote - state["entry"]) / state["entry"] - commission
            if (pnl <= -stake or (MULT_STOP_LOSS and pnl <= -MULT_STOP_LOSS)
                    or (MULT_TAKE_PROFIT and pnl >= MULT_TAKE_PROFIT)
                    or epoch >= state["entry_epoch"] + MULT_HOLD_SECONDS):
                done.set_result((quote, round(max(pnl, -stake), 2)))

        self.tick_listeners.append(listener)
        try:
            exit_spot, profit = await asyncio.wait_for(done, MULT_HOLD_SECONDS + 120)
        finally:
            self.tick_listeners.remove(listener)
        return state["entry"], exit_spot, profit

    async def simulate(self, contract_type):
        """Dry run: mimic Deriv settlement on the live price feed.
        Entry = first tick after purchase; exit = Nth tick after entry
        (tick contracts) or the last tick at/before expiry (time contracts)."""
        loop = asyncio.get_running_loop()
        done = loop.create_future()
        state = {"entry": None, "entry_epoch": None, "ticks": 0, "last": None}
        expiry_secs = duration_to_seconds(self.duration, self.duration_unit)

        def listener(epoch, quote):
            if done.done():
                return
            if state["entry"] is None:
                state.update(entry=quote, entry_epoch=epoch)
                return
            state["ticks"] += 1
            if self.duration_unit == "t":
                if state["ticks"] >= self.duration:
                    done.set_result(quote)
            elif epoch >= state["entry_epoch"] + expiry_secs:
                done.set_result(quote if epoch == state["entry_epoch"] + expiry_secs else state["last"])
            state["last"] = quote

        self.tick_listeners.append(listener)
        try:
            timeout = 120 if self.duration_unit == "t" else expiry_secs + 120
            exit_spot = await asyncio.wait_for(done, timeout)
        finally:
            self.tick_listeners.remove(listener)
        entry = state["entry"]
        won = exit_spot > entry if contract_type == "CALL" else exit_spot < entry
        return entry, exit_spot, won

    def finish_trade(self, direction, contract_type, stake, ma_fast, ma_slow, rsi,
                     entry, exit_spot, payout, won, profit, contract_id):
        self.check_day_rollover()
        self.daily_pnl = round(self.daily_pnl + profit, 2)
        result = "WIN" if won else "LOSS"
        log(f"{'✅' if won else '❌'} {result} {direction} | entry {entry} exit {exit_spot} | "
            f"P/L {profit:+.2f} | day {self.daily_pnl:+.2f} | balance {self.balance:.2f} {self.currency}")
        row = {
            "time": now_utc().isoformat(timespec="seconds"), "mode": self.mode,
            "account": self.account, "symbol": SYMBOL, "direction": direction,
            "contract_type": contract_type, "duration": self.duration_label,
            "stake": f"{stake:.2f}", "ma_fast": f"{ma_fast:.5f}", "ma_slow": f"{ma_slow:.5f}",
            "rsi": f"{rsi:.1f}",
            "entry_spot": entry, "exit_spot": exit_spot, "payout": f"{payout:.2f}",
            "result": result, "profit": f"{profit:.2f}", "daily_pnl": f"{self.daily_pnl:.2f}",
            "balance": f"{self.balance:.2f}", "contract_id": contract_id,
        }
        new_file = not CSV_FILE.exists()
        with CSV_FILE.open("a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
            if new_file:
                writer.writeheader()
            writer.writerow(row)
        self.update_stake(won)
        self.risk_allows(self.stake)   # stop right away if the limit is reached

    # --------------------------------------------------------------- main loop
    async def stream_candles(self):
        rid, q = await self.api.subscribe({
            "ticks_history": SYMBOL, "adjust_start_time": 1, "count": 100, "end": "latest",
            "granularity": CANDLE_GRANULARITY, "style": "candles",
        })
        while not self.stopped:
            try:
                msg = await asyncio.wait_for(q.get(), timeout=1)
            except asyncio.TimeoutError:
                continue
            if msg is None:
                raise ConnectionError(f"connection closed by server ({self.api.close_info})")
            if "error" in msg:
                raise DerivAPIError(msg["error"])
            if msg["msg_type"] == "candles":
                candles = msg["candles"]
                # last candle is still forming
                self.closes = pd.Series(
                    [float(c["close"]) for c in candles[:-1]],
                    index=[int(c["epoch"]) for c in candles[:-1]], dtype=float,
                )
                self.current_open_time = int(candles[-1]["epoch"])
                self.current_close = float(candles[-1]["close"])
                fast, slow = self._ma()
                log(f"Loaded {len(self.closes)} candles for {SYMBOL} | "
                    f"MA{MA_FAST}={fast.iloc[-1]:.5f} MA{MA_SLOW}={slow.iloc[-1]:.5f}")
            elif msg["msg_type"] == "ohlc":
                o = msg["ohlc"]
                open_time, close, epoch = int(o["open_time"]), float(o["close"]), int(o["epoch"])
                if self.current_open_time is not None and open_time > self.current_open_time:
                    self.on_candle_closed(self.current_open_time, self.current_close)
                self.current_open_time, self.current_close = open_time, close
                self.on_tick(epoch, close)

    async def keepalive(self):
        """Deriv closes connections that send nothing for ~2 minutes."""
        while True:
            await asyncio.sleep(KEEPALIVE_SECONDS)
            try:
                await self.api.request({"ping": 1}, timeout=20)
            except DerivAPIError:
                pass

    async def run_session(self):
        url, account = await get_session(self.token)
        self.api = DerivAPI(url)
        await self.api.connect()
        log(f"Connected to {url.split('?')[0]}")
        pinger = asyncio.create_task(self.keepalive())
        try:
            await self.authorize(account)
            await self.load_active_symbols(show=SHOW_ACTIVE_SYMBOLS and not self.symbols_shown)
            self.symbols_shown = True
            await self.resolve_contract()
            if self.pending_contract:
                log(f"Resuming open contract #{self.pending_contract['contract_id']}")
                self.trade_task = asyncio.create_task(self.watch_contract())
            await self.stream_candles()
            if self.trade_task and not self.trade_task.done():
                if DRY_RUN:
                    self.trade_task.cancel()     # simulated trade, nothing to settle
                else:
                    log("Waiting for open contract to settle (Ctrl+C again to quit now)...")
                    try:
                        await self.trade_task
                    except asyncio.CancelledError:
                        log("Quit without waiting - the contract still settles on Deriv")
        finally:
            pinger.cancel()
            if self.trade_task and not self.trade_task.done():
                self.trade_task.cancel()
            await self.api.close()

    async def run(self):
        log(f"Vera Bot | {self.mode} | {SYMBOL} | MA{MA_FAST}/MA{MA_SLOW} + RSI{RSI_PERIOD} on "
            f"{CANDLE_GRANULARITY}s candles | stake {STAKE} | martingale {MARTINGALE} | "
            f"daily stop -{DAILY_STOP_LOSS} | today P/L {self.daily_pnl:+.2f}")
        if not DRY_RUN:
            log("!!! LIVE MODE - real contracts will be bought !!!")
        self.risk_allows(0)   # daily limit already hit earlier today?
        delay = 2
        while not self.stopped:
            started = time.time()
            try:
                await self.run_session()
                delay = 2
            except DerivAPIError as e:
                if e.code in ("InvalidToken", "AuthorizationRequired"):
                    raise SystemExit(f"Authorization failed: {e}")
                log(f"API error: {e}")
            except (ConnectionError, OSError, asyncio.TimeoutError,
                    websockets.exceptions.WebSocketException) as e:
                explain_http_520(e)
                log(f"Connection problem: {e!r}")
            if self.stopped:
                break
            if time.time() - started > 120:   # connection was healthy for a while
                delay = 2
            log(f"Reconnecting in {delay}s...")
            try:
                await asyncio.wait_for(self.stop_event.wait(), delay)
            except asyncio.TimeoutError:
                pass
            delay = min(delay * 2, 60)
        log(f"Vera Bot stopped. {self.stop_reason} | today P/L {self.daily_pnl:+.2f} | "
            f"trades logged in {CSV_FILE.name}")


# Fallback names/groups from the symbol code, for API versions that don't send them
SYMBOL_GROUPS = [
    ("1HZ", "Derived", "Volatility (1s) Indices"), ("R_", "Derived", "Volatility Indices"),
    ("BOOM", "Derived", "Crash/Boom Indices"), ("CRASH", "Derived", "Crash/Boom Indices"),
    ("JD", "Derived", "Jump Indices"), ("stpRNG", "Derived", "Step Indices"),
    ("RB", "Derived", "Range Break Indices"), ("RDB", "Derived", "Daily Reset Indices"),
    ("WLD", "Derived", "Basket Indices"), ("OTC_", "Stock Indices", "Stock Indices"),
    ("cry", "Cryptocurrencies", "Crypto"),
    ("frxXA", "Commodities", "Metals"), ("frxXP", "Commodities", "Metals"),
    ("frx", "Forex", "Forex Pairs"),
]


def guess_name(code):
    if code.startswith("frx") or code.startswith("cry"):
        return f"{code[3:6]}/{code[6:]}"
    if code.startswith("1HZ"):
        return f"Volatility {code[3:-1]} (1s) Index"
    if code.startswith("R_"):
        return f"Volatility {code[2:]} Index"
    for prefix, word in (("BOOM", "Boom"), ("CRASH", "Crash"), ("JD", "Jump")):
        if code.startswith(prefix):
            return f"{word} {code[len(prefix):]} Index"
    if code.startswith("RB"):
        return f"Range Break {code[2:]} Index"
    if code.startswith("stpRNG"):
        return f"Step Index {code[6:] or 1}"
    if code.startswith("WLD"):
        return f"{code[3:]} Basket"
    if code.startswith("OTC_"):
        return f"{code[4:]} stock index"
    return {"RDBULL": "Bull Market Index", "RDBEAR": "Bear Market Index"}.get(code, "")


def normalize_symbol(sym):
    """Fill symbol / display_name / market / submarket whatever the API version calls them."""
    def first(*keys):
        for k in keys:
            if sym.get(k):
                return str(sym[k])
        return ""
    code = first("symbol", "underlying_symbol")
    sym["symbol"] = code
    sym["display_name"] = first("display_name", "underlying_symbol_name", "symbol_display_name",
                                "name") or guess_name(code)
    market = first("market_display_name", "market_name")
    submarket = first("submarket_display_name", "submarket_name")
    if not (market and submarket):
        group = next(((m, sm) for prefix, m, sm in SYMBOL_GROUPS if code.startswith(prefix)),
                     (first("market") or "Other", first("submarket") or "Other"))
        market, submarket = market or group[0], submarket or group[1]
    sym["market_display_name"], sym["submarket_display_name"] = market, submarket
    return sym


def print_symbols(symbols):
    """Print active symbols grouped by market."""
    print(f"\n===== {len(symbols)} active Deriv symbols =====", flush=True)
    groups = {}
    for sym in symbols:
        key = (sym.get("market_display_name", "?"), sym.get("submarket_display_name", "?"))
        groups.setdefault(key, []).append(sym)
    for market, submarket in sorted(groups):
        print(f"\n[{market} / {submarket}]")
        for sym in sorted(groups[(market, submarket)], key=lambda x: x["symbol"]):
            state = "open" if sym.get("exchange_is_open") else "closed"
            if sym.get("is_trading_suspended"):
                state = "suspended"
            print(f"  {sym['symbol']:<14} {sym.get('display_name', ''):<34} {state}")
    print(flush=True)


async def list_symbols():
    url, account = await get_session(os.getenv("DERIV_TOKEN", "").strip())
    api = DerivAPI(url)
    await api.connect()
    try:
        if account is None and os.getenv("DERIV_TOKEN", "").strip() and not API_URL:
            await api.request({"authorize": os.getenv("DERIV_TOKEN").strip()})
        resp = await api.request({"active_symbols": "brief"})
        symbols = resp["active_symbols"]
        for sym in symbols:
            normalize_symbol(sym)
        print_symbols(symbols)
    finally:
        await api.close()


async def main():
    global SYMBOL
    args = sys.argv[1:]
    if not API_URL:
        check_env()
    if "--symbols" in args:
        try:
            await list_symbols()
        except websockets.exceptions.WebSocketException as e:
            explain_http_520(e)
            raise
        return
    if args:
        SYMBOL = args[0]
    bot = TradingBot(os.getenv("DERIV_TOKEN", "").strip())
    loop = asyncio.get_running_loop()

    def on_signal():
        if bot.stopped and bot.trade_task and not bot.trade_task.done():
            bot.trade_task.cancel()          # second Ctrl+C: don't wait for the contract
        bot.stop("stopped by user (Ctrl+C)")

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, on_signal)
    await bot.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)
