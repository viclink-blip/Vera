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
           python3 vera_bot.py --backtest   (test every contract type on recent ticks)
           python3 vera_bot.py --train      (train + test the AI models, VERA_STRATEGY=AI)
"""

import asyncio
import csv
import json
import math
import os
import signal
import sys
import time
import collections
import urllib.error
import urllib.request
from datetime import datetime, time as dtime, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
from pathlib import Path

import pandas as pd
import websockets
import websockets.exceptions
from dotenv import load_dotenv

load_dotenv(Path(__file__).with_name(".env"))


def setting(name, default):
    """A setting from .env if present (VERA_<NAME>=...), else the default below.
    Keeping your own values in .env means `git pull` never conflicts with them."""
    raw = os.getenv(f"VERA_{name}")
    if raw is None or raw.strip() == "":
        return default
    raw = raw.strip()
    if isinstance(default, bool):
        return raw.lower() in ("1", "true", "yes", "on")
    if isinstance(default, (int, float)):
        value = float(raw)
        return int(value) if value.is_integer() else value
    return raw

# ============================================================================
#  SETTINGS - edit these, or override them in .env as VERA_<NAME>=value
#  (e.g. VERA_STAKE=50, VERA_DRY_RUN=false) so `git pull` never conflicts
# ============================================================================
ACCOUNT_TYPE = setting("ACCOUNT_TYPE", "demo")  # "demo" or "real" - which of your Deriv accounts to use

SYMBOL = setting("SYMBOL", "R_75")  # one or more Deriv symbols, comma separated:
                             # "R_75" or "R_75,R_100" (each market is watched on its own;
                             # balance, daily P/L and limits are shared). Examples:
                             # R_75, R_100, 1HZ100V, BOOM1000,
                             # CRASH500, JD25, frxEURUSD, frxXAUUSD, cryBTCUSD ...
                             # run `python3 vera_bot.py --symbols` to see them all
SYMBOLS = [x.strip() for x in str(SYMBOL).split(",") if x.strip()]
SHOW_ACTIVE_SYMBOLS = setting("SHOW_ACTIVE_SYMBOLS", True)  # print every active symbol at startup

CONTRACT = setting("CONTRACT", "RISEFALL")  # what to trade:
                             #   RISEFALL   - Rise/Fall by the MA/RSI signal (Multipliers if
                             #                the symbol has no Rise/Fall, e.g. Boom/Crash)
                             #   MULTIPLIER - Multipliers Up/Down by the MA/RSI signal
                             #   DIGITEVEN, DIGITODD, DIGITMATCH, DIGITDIFF, DIGITOVER,
                             #   DIGITUNDER - last-digit contracts (see DIGIT_BARRIER)
                             #   AUTO       - before each trade, compare live payouts of all
                             #                of the above and take the smallest Deriv edge
DIGIT_BARRIER = setting("DIGIT_BARRIER", 5)  # the digit for MATCH / DIFF / OVER / UNDER
DIGIT_TICKS = setting("DIGIT_TICKS", 5)      # digit contracts last this many ticks (1-10)
CONTRACT_MODE = "auto"       # with CONTRACT = RISEFALL: "auto" falls back to Multipliers when
                             # the symbol has no Rise/Fall; "risefall" never does
STAKE = setting("STAKE", 1)  # USD per trade (Deriv minimum is 0.35)
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

BACKTEST_TICKS = 20000       # default history for `python3 vera_bot.py --backtest`

STRATEGY = setting("STRATEGY", "MA")   # "MA": MA/RSI signals (CONTRACT decides what to buy)
                             # "AI": machine-learning models pick when and what to trade,
                             #       only if they pass the walk-forward test (see vera_ai.py)
AI_MIN_EDGE = setting("AI_MIN_EDGE", 0.02)       # trade only above +2% predicted expected return
AI_TRAIN_TICKS = setting("AI_TRAIN_TICKS", 40000)  # history the models learn from
AI_RETRAIN_HOURS = setting("AI_RETRAIN_HOURS", 6)  # retrain on fresh ticks this often
AI_FORCE = setting("AI_FORCE", False)  # True = trade even without a proven edge (demo testing)

MAX_OPEN_TRADES = setting("MAX_OPEN_TRADES", 3)  # trades allowed open at the same time
TREND_TRADE_MINUTES = setting("TREND_TRADE_MINUTES", 6)  # no trade for this many minutes ->
                             # trade the current trend (MA7 above MA14 = Rise, below = Fall).
                             # 6 gives at least 5 trades per 30 min. 0 = crossovers only
WAIT_NEXT_DAY = setting("WAIT_NEXT_DAY", True)  # after a daily stop loss / take profit,
                             # pause until midnight (TIMEZONE) and carry on (for 24h running)
TIMEZONE = setting("TIMEZONE", "Africa/Nairobi")  # clock for logs, the CSV and the trading
                             # day (daily P/L resets at midnight here). Africa/Nairobi =
                             # East Africa Time (UTC+3); e.g. Africa/Dar_es_Salaam, UTC

MARTINGALE = setting("MARTINGALE", False)  # after a loss, next stake = previous stake x MULTIPLIER
MARTINGALE_MULTIPLIER = 2.1
MARTINGALE_MAX_STEPS = 5     # reset to STAKE after this many losses in a row

DAILY_STOP_LOSS = setting("DAILY_STOP_LOSS", 10)  # (0 = off) stop the bot when today's P/L reaches -10 USD
DAILY_TAKE_PROFIT = setting("DAILY_TAKE_PROFIT", 0)  # stop when today's P/L reaches +X USD (0 = disabled)
BALANCE_TARGET = setting("BALANCE_TARGET", 0)  # stop for good when the balance reaches this
                             # (e.g. 30), so you can withdraw. 0 = off. With BANKROLL the
                             # bankroll balance counts, otherwise the account balance
BANKROLL = setting("BANKROLL", 50)  # trade as if the account held only this much, e.g. 50
                             # (0 = use the whole account balance). It carries over
                             # between runs; rename vera_trades.csv to start it over.

DRY_RUN = setting("DRY_RUN", True)  # True = simulate trades on live prices, no real buys
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


try:
    TZ = ZoneInfo(TIMEZONE)
except (ZoneInfoNotFoundError, ValueError):
    print(f"Unknown TIMEZONE {TIMEZONE!r} - using East Africa Time (UTC+3)", flush=True)
    TZ = timezone(timedelta(hours=3), "EAT")


def now_local():
    return datetime.now(TZ)


def log(msg):
    print(f"[{now_local():%Y-%m-%d %H:%M:%S}] {msg}", flush=True)


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


DIGIT_TYPES = ("DIGITEVEN", "DIGITODD", "DIGITMATCH", "DIGITDIFF", "DIGITOVER", "DIGITUNDER")
DIGIT_NAMES = {"DIGITEVEN": "EVEN", "DIGITODD": "ODD", "DIGITMATCH": "MATCH",
               "DIGITDIFF": "DIFF", "DIGITOVER": "OVER", "DIGITUNDER": "UNDER"}


def needs_barrier(contract):
    return contract in ("DIGITMATCH", "DIGITDIFF", "DIGITOVER", "DIGITUNDER")


def digit_label(contract, barrier):
    name = DIGIT_NAMES[contract]
    return f"{name} {barrier}" if needs_barrier(contract) else name


def digit_win_chance(contract, barrier):
    """Exact chance to win - every last digit 0-9 is equally likely."""
    if contract in ("DIGITEVEN", "DIGITODD"):
        return 0.5
    if contract == "DIGITMATCH":
        return 0.1
    if contract == "DIGITDIFF":
        return 0.9
    return (9 - barrier) / 10 if contract == "DIGITOVER" else barrier / 10


def digit_wins(contract, barrier, digit):
    if contract == "DIGITEVEN":
        return digit % 2 == 0
    if contract == "DIGITODD":
        return digit % 2 == 1
    if contract == "DIGITMATCH":
        return digit == barrier
    if contract == "DIGITDIFF":
        return digit != barrier
    return digit > barrier if contract == "DIGITOVER" else digit < barrier


def last_digit(price, decimals):
    return int(f"{float(price):.{decimals}f}"[-1])


def pip_decimals(info):
    pip = info.get("pip") or info.get("pip_size")
    try:
        return max(0, round(-math.log10(float(pip))))
    except (TypeError, ValueError):
        return None


def auto_candidates():
    b = DIGIT_BARRIER
    return [("RISEFALL", None), ("DIGITEVEN", None), ("DIGITODD", None), ("DIGITDIFF", b),
            ("DIGITMATCH", b), ("DIGITOVER", 1), ("DIGITUNDER", 8)]


def compute_indicators(closes):
    """MA_FAST, MA_SLOW and Wilder's RSI series for candle closes."""
    fast = closes.rolling(MA_FAST).mean()
    slow = closes.rolling(MA_SLOW).mean()
    delta = closes.diff()
    gain = delta.clip(lower=0).ewm(alpha=1 / RSI_PERIOD, adjust=False).mean()
    loss = (-delta.clip(upper=0)).ewm(alpha=1 / RSI_PERIOD, adjust=False).mean()
    rsi = (100 - 100 / (1 + gain / loss)).fillna(100.0)   # no losses -> RSI 100
    return fast, slow, rsi


def decide(f_prev, s_prev, f_now, s_now, minutes_since_trade):
    """Strategy: MA crossover, or a trend trade after TREND_TRADE_MINUTES without a trade.
    Returns (direction, "cross" | "trend") or (None, None)."""
    if f_prev <= s_prev and f_now > s_now:
        return "RISE", "cross"
    if f_prev >= s_prev and f_now < s_now:
        return "FALL", "cross"
    if TREND_TRADE_MINUTES and minutes_since_trade >= TREND_TRADE_MINUTES - 5 / 60:
        return ("RISE" if f_now > s_now else "FALL"), "trend"
    return None, None


def rsi_allows(direction, rsi):
    if not RSI_FILTER:
        return True
    if direction == "RISE":
        return rsi < RSI_OVERBOUGHT
    return rsi > RSI_OVERSOLD


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


class Shared:
    """Money and limits shared by every market the bot watches."""

    def __init__(self):
        self.balance = DRY_RUN_START_BALANCE
        self.sim_balance_set = False
        self.symbols_shown = False
        self.stake = float(STAKE)
        self.loss_streak = 0
        self.trading_day = now_local().date()
        self.daily_pnl = 0.0
        self.open_stake = 0.0                  # stake in trades that haven't settled yet
        self.daily_limit_hit = False
        self.stopped = False
        self.stop_reason = ""
        self.stop_event = asyncio.Event()
        self.pnl_account = None                # account whose P/L daily_pnl holds
        self.bots = []


SHARED_FIELDS = ("pnl_account", "balance", "sim_balance_set", "symbols_shown", "stake", "loss_streak",
                 "trading_day", "daily_pnl", "open_stake", "daily_limit_hit", "stopped",
                 "stop_reason", "stop_event")


class TradingBot:
    """Watches one market; money and limits live in the Shared object."""

    def __init__(self, token, symbol=None, shared=None):
        self.symbol = symbol or SYMBOLS[0]
        self.shared = shared or Shared()
        self.shared.bots.append(self)
        self.first = len(self.shared.bots) == 1
        self.token = token
        self.api = None
        self.account = "none"
        self.is_virtual = None
        self.currency = CURRENCY
        self.duration = DURATION
        self.duration_unit = DURATION_UNIT
        self.kind = "risefall"                  # risefall / multiplier / digit / auto
        self.contract = "RISEFALL"              # set by resolve_contract()
        self.available_types = set()
        self.auto_risefall = True
        self.decimals = None                    # price decimals, for last digits
        self.multiplier = MULTIPLIER
        self.symbol_key = "underlying_symbol"   # new API name; falls back to "symbol"

        self.closes = pd.Series(dtype=float)   # closed candles, index = open_time
        self.current_open_time = None
        self.current_close = None
        self.last_tick_epoch = None

        if self.first:
            self._migrate_csv()

        self.trade_tasks = set()               # running trades on this market
        self.pending_contracts = {}            # contract_id -> info, resumed after reconnect
        self.last_trade_time = time.time()
        self.tick_listeners = []               # dry-run simulators waiting on ticks
        self.ticks = collections.deque(maxlen=200)   # recent prices for the AI
        self.ai = None
        self.ai_training = False
        self.ai_payouts = {}
        self.ai_last_note = 0.0

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
        """Set the balance the bot trades with, once per run for dry run / bankroll.
        Also loads today's P/L - for this account only, so demo and real don't mix."""
        if self.pnl_account != self.account:
            self.pnl_account = self.account
            self.daily_pnl = self._load_today_pnl()
            log(f"Today's P/L on {self.account}: {self.daily_pnl:+.2f} {self.currency}")
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
            ts = pd.to_datetime(df["time"], utc=True, errors="coerce").dt.tz_convert(TZ)
            today = ((ts.dt.date == now_local().date()) & (df["mode"] == self.mode)
                     & (df["account"].astype(str) == str(self.account)))
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
        """Fetch every active Deriv symbol, optionally print them, and check self.symbol."""
        resp = await self.api.request({"active_symbols": "brief"})
        symbols = resp["active_symbols"]
        for sym in symbols:
            normalize_symbol(sym)
        if show:
            print_symbols(symbols)
        by_code = {sym["symbol"]: sym for sym in symbols}
        info = by_code.get(self.symbol)
        if info is None:
            hits = [c for c, sym in by_code.items()
                    if self.symbol.lower() in (c.lower(), sym.get("display_name", "").lower())]
            hint = f" Did you mean {hits[0]}?" if hits else " Run with --symbols to list them."
            raise SystemExit(f"Symbol {self.symbol!r} is not an active Deriv symbol.{hint}")
        self.decimals = pip_decimals(info) if pip_decimals(info) is not None else self.decimals
        status = "OPEN" if info.get("exchange_is_open") else "CLOSED"
        if info.get("is_trading_suspended"):
            status = "SUSPENDED"
        log(f"Symbol {self.symbol} = {info.get('display_name')} "
            f"({info.get('market_display_name')} / {info.get('submarket_display_name')}) - market {status}")
        if status != "OPEN":
            log("Market not open now - trades will be rejected until it opens")

    async def resolve_contract(self):
        """Check CONTRACT against what self.symbol offers and set duration / multiplier."""
        contract = str(CONTRACT).upper()
        if contract not in ("RISEFALL", "MULTIPLIER", "AUTO") + DIGIT_TYPES:
            raise SystemExit(f"Unknown CONTRACT {CONTRACT!r}. Use RISEFALL, MULTIPLIER, AUTO or "
                             f"{', '.join(DIGIT_TYPES)}")
        if contract in ("DIGITOVER", "DIGITUNDER", "DIGITMATCH", "DIGITDIFF"):
            lo, hi = {"DIGITOVER": (0, 8), "DIGITUNDER": (1, 9)}.get(contract, (0, 9))
            if not lo <= DIGIT_BARRIER <= hi:
                raise SystemExit(f"DIGIT_BARRIER for {contract} must be {lo}-{hi}")
        await self._resolve_direction_contract("multiplier" if contract == "MULTIPLIER" else CONTRACT_MODE)
        self.contract = "MULTIPLIER" if self.kind == "multiplier" else "RISEFALL"
        if contract in ("RISEFALL", "MULTIPLIER"):
            if self.kind == "multiplier":
                log(f"Contract: Multipliers Up/Down x{self.multiplier}, close after {MULT_HOLD_SECONDS}s")
            else:
                log(f"Contract: Rise/Fall {self.duration_label(self.contract)}")
        if contract in DIGIT_TYPES:
            if self.available_types and contract not in self.available_types:
                raise SystemExit(f"{contract} is not offered for {self.symbol}")
            self.contract, self.kind = contract, "digit"
            log(f"Contract: {digit_label(contract, DIGIT_BARRIER)} over {DIGIT_TICKS} ticks "
                f"(win chance {digit_win_chance(contract, DIGIT_BARRIER):.0%})")
        elif contract == "AUTO":
            self.auto_risefall = self.contract == "RISEFALL"
            self.kind = "auto"
            log("Contract: AUTO - before each trade, compares live payouts and takes the "
                "smallest Deriv edge")

    async def _resolve_direction_contract(self, mode):
        """Pick Rise/Fall or Multipliers for self.symbol and check duration / multiplier."""
        try:
            try:
                resp = await self.api.request({"contracts_for": self.symbol, "currency": self.currency})
            except DerivAPIError as e:
                if e.code != "InputValidationFailed":
                    raise
                resp = await self.api.request({"contracts_for": self.symbol})   # new API: no currency
        except DerivAPIError as e:
            log(f"contracts_for failed ({e}); using Rise/Fall {DURATION}{DURATION_UNIT}")
            return
        available = resp["contracts_for"]["available"]
        self.available_types = {c.get("contract_type") for c in available}
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
        log(f"Contract types for {self.symbol}: {', '.join(types)}")

        if mode == "multiplier" or (mode == "auto" and not offers):
            if not mult_offers:
                raise SystemExit(f"Neither Rise/Fall nor Multipliers are offered for {self.symbol}")
            self.kind = "multiplier"
            allowed_mults = sorted({int(m) for c in mult_offers for m in c.get("multiplier_range", [])})
            if allowed_mults and MULTIPLIER not in allowed_mults:
                self.multiplier = min(allowed_mults, key=lambda m: (abs(m - MULTIPLIER), m))
                log(f"Multiplier x{MULTIPLIER} not offered for {self.symbol}; using x{self.multiplier} "
                    f"(allowed {allowed_mults})")
            return
        if not offers:
            raise SystemExit(f"Rise/Fall is not offered for {self.symbol} (types: {', '.join(types)})")
        self.kind = "risefall"

        ranges = []
        for c in offers:
            try:
                ranges.append((parse_duration(c["min_contract_duration"]),
                               parse_duration(c["max_contract_duration"])))
            except (KeyError, ValueError, TypeError, IndexError):
                pass
        if not ranges:   # duration format unknown - let the proposal validate it
            log(f"Rise/Fall {DURATION}{DURATION_UNIT} (durations not listed: "
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
            return
        mins = [lo for lo, _ in ranges]
        if not AUTO_ADJUST_DURATION:
            raise SystemExit(f"{DURATION}{DURATION_UNIT} not allowed for {self.symbol}. "
                             f"Minimums offered: {[f'{v}{u}' for v, u in mins]}")
        # prefer ticks, else shortest time based duration
        ticks = [m for m in mins if m[1] == "t"]
        best = min(ticks) if ticks else min(mins, key=lambda m: duration_to_seconds(*m))
        self.duration, self.duration_unit = best
        log(f"{DURATION}{DURATION_UNIT} not offered for {self.symbol}; using {best[0]}{best[1]}")

    # ------------------------------------------------------------- strategy
    def _ma(self):
        fast = self.closes.rolling(MA_FAST).mean()
        slow = self.closes.rolling(MA_SLOW).mean()
        return fast, slow

    def on_candle_closed(self, open_time, close):
        self.closes.loc[open_time] = close
        self.closes = self.closes.iloc[-300:]
        need = max(MA_SLOW, RSI_PERIOD) + 1
        if len(self.closes) < need:
            log(f"Collecting candles {len(self.closes)}/{need}")
            return
        fast, slow, rsi_s = compute_indicators(self.closes)
        f_prev, s_prev, f_now, s_now = fast.iloc[-2], slow.iloc[-2], fast.iloc[-1], slow.iloc[-1]
        rsi = rsi_s.iloc[-1]
        log(f"{self.symbol} candle close {close} | MA{MA_FAST}={f_now:.5f} MA{MA_SLOW}={s_now:.5f} "
            f"RSI{RSI_PERIOD}={rsi:.1f}")
        if STRATEGY == "AI":
            self.ai_decide(f_now, s_now, rsi)
            return

        direction, why = decide(f_prev, s_prev, f_now, s_now, (time.time() - self.last_trade_time) / 60)
        if not direction:
            return
        side = "above" if direction == "RISE" else "below"
        if why == "cross":
            log(f">>> {self.symbol} Signal {direction}: MA{MA_FAST} crossed {side} MA{MA_SLOW} | RSI {rsi:.1f}")
        else:
            log(f">>> {self.symbol} Trend trade {direction}: no trade for {TREND_TRADE_MINUTES} min, MA{MA_FAST} "
                f"{side} MA{MA_SLOW} | RSI {rsi:.1f}")
        rsi_ok = rsi_allows(direction, rsi)
        if not rsi_ok and self.kind in ("risefall", "multiplier"):
            limit = f">= {RSI_OVERBOUGHT} (overbought)" if direction == "RISE" else f"<= {RSI_OVERSOLD} (oversold)"
            log(f"RSI {rsi:.1f} {limit} - {direction.title()} skipped")
            return
        open_now = sum(len(b.trade_tasks) for b in self.shared.bots)
        if open_now >= MAX_OPEN_TRADES:
            log(f"{open_now} trades already open (MAX_OPEN_TRADES) - {self.symbol} signal skipped")
            return
        self.last_trade_time = time.time()
        self.start_task(self.trade(direction, f_now, s_now, rsi, rsi_ok))

    # ----------------------------------------------------------------- AI
    async def load_ai_payouts(self):
        """Live payout of every contract the AI can choose, at the current stake."""
        from vera_ai import candidates
        stake = round(self.stake, 2)
        payouts = {}
        for contract, barrier in candidates():
            ct_ok = contract in ("CALL", "PUT") or not self.available_types or contract in self.available_types
            if not ct_ok:
                continue
            req = {"proposal": 1, "amount": stake, "basis": "stake", "currency": self.currency,
                   self.symbol_key: self.symbol, "contract_type": contract,
                   "duration": DIGIT_TICKS, "duration_unit": "t"}
            if barrier is not None:
                req["barrier"] = str(barrier)
            try:
                payouts[(contract, barrier)] = float((await self.request_proposal(req)).get("payout") or 0)
            except DerivAPIError:
                pass
        if not payouts:
            raise DerivAPIError({"code": "NoPayouts", "message": "no contract could be priced for the AI"})
        self.ai_payouts = payouts
        return payouts

    async def ensure_ai(self):
        from vera_ai import VeraAI
        if self.ai is None:
            self.ai = VeraAI.load(self.symbol)
        ai = self.ai
        stale = (ai is None or ai.stake != round(self.stake, 2) or ai.horizon != DIGIT_TICKS
                 or time.time() - ai.trained_at > AI_RETRAIN_HOURS * 3600)
        if stale:
            await self.train_ai()
        else:
            self.ai_payouts = ai.payouts
            log(ai.summary() + f" | trained {(time.time() - ai.trained_at) / 3600:.1f} h ago")
            prices, _ = await download_ticks(self.api, self.symbol, self.ticks.maxlen)
            self.ticks.extend(float(x) for x in prices)

    async def train_ai(self):
        from vera_ai import VeraAI
        if self.ai_training:
            return
        self.ai_training = True
        try:
            log(f"AI {self.symbol}: downloading {AI_TRAIN_TICKS} ticks and live payouts to train...")
            prices, _ = await download_ticks(self.api, self.symbol, AI_TRAIN_TICKS)
            payouts = await self.load_ai_payouts()
            decimals = self.decimals if self.decimals is not None else infer_decimals(prices)
            ai = VeraAI(self.symbol, decimals, DIGIT_TICKS, round(self.stake, 2), payouts)
            await asyncio.to_thread(ai.fit, [float(x) for x in prices], AI_MIN_EDGE)
            ai.save()
            self.ai = ai
            self.ticks.extend(float(x) for x in prices[-self.ticks.maxlen:])
            log(ai.summary())
        except (DerivAPIError, ValueError) as e:
            log(f"AI {self.symbol}: training failed ({e}) - will retry later")
        finally:
            self.ai_training = False

    def ai_decide(self, ma_fast, ma_slow, rsi):
        from vera_ai import label
        if self.ai is None or self.ai_training:
            return
        if time.time() - self.ai.trained_at > AI_RETRAIN_HOURS * 3600:
            asyncio.create_task(self.train_ai())
            return
        pred = self.ai.predict(list(self.ticks))
        if pred is None:
            log(f"AI {self.symbol}: collecting ticks {len(self.ticks)}/30")
            return
        contract, barrier, prob, ev = self.ai.best_contract(*pred, payouts=self.ai_payouts or None)
        name = label(contract, barrier)
        if not (self.ai.passed or AI_FORCE):
            if time.time() - self.ai_last_note > 3600:
                self.ai_last_note = time.time()
                log(f"AI {self.symbol}: no proven edge in the unseen-tick test - standing aside "
                    f"(best idea now {name}, predicted {ev:+.1%}). Retrains every {AI_RETRAIN_HOURS} h.")
            return
        if ev <= AI_MIN_EDGE:
            log(f"AI {self.symbol}: best {name} predicted {ev:+.1%} - not above +{AI_MIN_EDGE:.0%}, no trade")
            return
        open_now = sum(len(b.trade_tasks) for b in self.shared.bots)
        if open_now >= MAX_OPEN_TRADES:
            log(f"{open_now} trades already open (MAX_OPEN_TRADES) - {self.symbol} AI signal skipped")
            return
        log(f">>> AI {self.symbol}: {name} | win chance {prob:.1%} | predicted {ev:+.1%}")
        direction = "RISE" if contract == "CALL" else "FALL" if contract == "PUT" else "AI"
        self.last_trade_time = time.time()
        self.start_task(self.trade(direction, ma_fast, ma_slow, rsi, True, ai=(contract, barrier, prob)))

    def start_task(self, coro):
        task = asyncio.create_task(coro)
        self.trade_tasks.add(task)
        task.add_done_callback(self._task_done)

    def _task_done(self, task):
        self.trade_tasks.discard(task)
        if not task.cancelled() and task.exception() is not None:
            exc = task.exception()
            if not isinstance(exc, (ConnectionError, asyncio.TimeoutError)):
                log(f"Trade error: {exc!r}")

    def on_tick(self, epoch, quote):
        self.last_tick_epoch = epoch
        self.ticks.append(quote)
        for listener in list(self.tick_listeners):
            listener(epoch, quote)

    # --------------------------------------------------------------- risk
    def check_day_rollover(self):
        today = now_local().date()
        if today != self.trading_day:
            log(f"New day {today} - daily P/L reset (was {self.daily_pnl:+.2f})")
            self.trading_day = today
            self.daily_pnl = 0.0

    def risk_allows(self, stake):
        self.check_day_rollover()
        if DAILY_STOP_LOSS and self.daily_pnl <= -DAILY_STOP_LOSS:
            self.stop(f"Daily stop loss hit ({self.daily_pnl:+.2f} {self.currency})", daily=True)
            return False
        if BALANCE_TARGET and self.sim_balance_set and self.balance >= BALANCE_TARGET:
            self.stop(f"Balance target reached: {self.balance:.2f} >= {BALANCE_TARGET} {self.currency}. "
                      "Withdraw your profit, then raise or remove VERA_BALANCE_TARGET to trade again")
            return False
        if DAILY_TAKE_PROFIT and self.daily_pnl >= DAILY_TAKE_PROFIT:
            self.stop(f"Daily take profit hit ({self.daily_pnl:+.2f} {self.currency})", daily=True)
            return False
        at_risk = self.open_stake + stake
        if DAILY_STOP_LOSS and self.daily_pnl - at_risk < -DAILY_STOP_LOSS:
            if self.open_stake:   # wait for the open trades instead of stopping
                log("Skipped: open trades + this stake could pass the daily stop loss")
                return False
            self.stop(f"Next stake {stake:.2f} could exceed daily stop loss "
                      f"(P/L {self.daily_pnl:+.2f}, limit -{DAILY_STOP_LOSS})", daily=True)
            return False
        if self.open_stake and at_risk > self.balance:
            log(f"Skipped: balance {self.balance:.2f} is tied up in open trades")
            return False
        if stake > self.balance:
            what = "Bankroll" if BANKROLL else "Balance"
            self.stop(f"{what} {self.balance:.2f} too low for stake {stake:.2f}")
            return False
        return True

    def stop(self, reason, daily=False):
        if not self.stopped:
            self.daily_limit_hit = daily
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
    def proposal_request(self, direction, stake, contract, barrier=None):
        """Build the proposal for one contract (RISEFALL, MULTIPLIER or a DIGIT type)."""
        req = {"proposal": 1, "amount": stake, "basis": "stake",
               "currency": self.currency, self.symbol_key: self.symbol}
        if contract in DIGIT_TYPES:
            req["contract_type"] = contract
            req["duration"] = DIGIT_TICKS
            req["duration_unit"] = "t"
            if needs_barrier(contract):
                req["barrier"] = str(barrier)
        elif contract == "MULTIPLIER":
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

    def duration_label(self, contract):
        if contract == "MULTIPLIER":
            return f"x{self.multiplier}/{MULT_HOLD_SECONDS}s"
        if contract in DIGIT_TYPES:
            return f"{DIGIT_TICKS}t"
        return f"{self.duration}{self.duration_unit}"

    async def request_proposal(self, req):
        try:
            return (await self.api.request(req))["proposal"]
        except DerivAPIError as e:
            if e.code != "InputValidationFailed":
                raise
            # API version uses the other field name for the symbol - switch and retry
            other = "symbol" if self.symbol_key == "underlying_symbol" else "underlying_symbol"
            req[other] = req.pop(self.symbol_key)
            proposal = (await self.api.request(req))["proposal"]
            self.symbol_key = other
            return proposal

    async def pick_contract(self, direction, stake, rsi_ok):
        """Returns (contract, barrier, request, proposal) for this trade."""
        if self.kind != "auto":
            req = self.proposal_request(direction, stake, self.contract, DIGIT_BARRIER)
            return self.contract, DIGIT_BARRIER, req, await self.request_proposal(req)
        best, report = None, []
        for contract, barrier in auto_candidates():
            if contract == "RISEFALL" and not (self.auto_risefall and rsi_ok):
                continue
            if contract != "RISEFALL" and self.available_types and contract not in self.available_types:
                continue
            req = self.proposal_request(direction, stake, contract, barrier)
            try:
                proposal = await self.request_proposal(req)
            except DerivAPIError:
                continue
            payout = float(proposal.get("payout") or 0)
            chance = 0.5 if contract == "RISEFALL" else digit_win_chance(contract, barrier)
            edge = chance * payout / stake - 1
            name = direction if contract == "RISEFALL" else digit_label(contract, barrier)
            report.append(f"{name} {edge:+.1%}")
            if best is None or edge > best[0]:
                best = (edge, name, contract, barrier, req, proposal)
        if best is None:
            raise DerivAPIError({"code": "NoContract", "message": "no contract could be priced"})
        log(f"AUTO {self.symbol} expected return per trade: {' | '.join(report)} -> {best[1]}")
        return best[2:]

    async def trade(self, direction, ma_fast, ma_slow, rsi, rsi_ok=True, ai=None):
        stake = round(self.stake, 2)
        if self.stopped or not self.risk_allows(stake):
            return
        self.open_stake = round(self.open_stake + stake, 2)
        try:
            await self._trade(direction, stake, ma_fast, ma_slow, rsi, rsi_ok, ai)
        finally:
            self.open_stake = round(self.open_stake - stake, 2)

    async def _trade(self, direction, stake, ma_fast, ma_slow, rsi, rsi_ok, ai=None):
        try:
            if ai:
                contract, barrier, prob = ai
                if contract in ("CALL", "PUT"):
                    req = self.proposal_request(direction, stake, "RISEFALL")
                    req["duration"], req["duration_unit"] = DIGIT_TICKS, "t"
                    contract = "RISEFALL"
                else:
                    req = self.proposal_request(direction, stake, contract, barrier)
                proposal = await self.request_proposal(req)
            else:
                contract, barrier, req, proposal = await self.pick_contract(direction, stake, rsi_ok)
        except DerivAPIError as e:
            log(f"Proposal rejected: {e}")
            return
        contract_type = req["contract_type"]
        label = digit_label(contract, barrier) if contract in DIGIT_TYPES else direction
        duration = self.duration_label(contract)
        payout = float(proposal.get("payout") or 0)
        if ai:
            label = "AI " + label
            duration = f"{DIGIT_TICKS}t"
            live_ev = prob * payout / stake - 1
            if live_ev <= AI_MIN_EDGE and not AI_FORCE:
                log(f"AI {self.symbol}: live payout {payout:.2f} gives {live_ev:+.1%} - trade cancelled")
                return
        extra = (f"x{self.multiplier} commission {float(proposal.get('commission') or 0):.2f}"
                 if contract == "MULTIPLIER" else f"payout {payout:.2f}")
        log(f"Proposal {contract_type}{' ' + str(barrier) if needs_barrier(contract) else ''} "
            f"{self.symbol} stake {stake:.2f} {extra} spot {proposal.get('spot')}")

        if DRY_RUN:
            if contract == "MULTIPLIER":
                entry, exit_, profit = await self.simulate_multiplier(
                    contract_type, stake, float(proposal.get("commission") or 0))
                won = profit > 0
            else:
                entry, exit_, won = await self.simulate(contract_type, barrier,
                                                        DIGIT_TICKS if ai else None)
                profit = round(payout - stake, 2) if won else -stake
            self.balance = round(self.balance + profit, 2)
            self.finish_trade(label, contract_type, stake, ma_fast, ma_slow, rsi,
                              entry, exit_, payout, won, profit, "dry-run", duration)
            return

        try:
            buy = (await self.api.request({"buy": proposal["id"], "price": proposal["ask_price"]}))["buy"]
        except DerivAPIError as e:
            log(f"Buy failed: {e}")
            return
        contract_id = buy["contract_id"]
        log(f"BOUGHT {self.symbol} {contract_type} #{contract_id} for {float(buy['buy_price']):.2f} "
            f"(balance {float(buy['balance_after']):.2f})")
        info = dict(direction=label, contract_type=contract_type, stake=stake,
                    ma_fast=ma_fast, ma_slow=ma_slow, rsi=rsi, duration=duration, barrier=barrier,
                    contract_id=contract_id,
                    kind="multiplier" if contract == "MULTIPLIER" else "fixed",
                    sell_at=time.time() + MULT_HOLD_SECONDS)
        self.pending_contracts[contract_id] = info
        await self.watch_contract(info)

    async def watch_contract(self, info):
        """Follow a live contract until it is sold/expired, then record it."""
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

        self.pending_contracts.pop(info["contract_id"], None)
        profit = round(float(poc["profit"]), 2)
        try:
            if BANKROLL:
                raise ValueError("bankroll mode tracks its own balance")
            self.balance = float((await self.api.request({"balance": 1}))["balance"]["balance"])
        except Exception:  # noqa: BLE001
            self.balance = round(self.balance + profit, 2)
        def first(*keys):
            return next((poc[k] for k in keys if poc.get(k) not in (None, "")), None)
        entry = first("entry_spot_display_value", "entry_tick_display_value", "entry_spot", "entry_tick")
        exit_ = first("exit_spot_display_value", "exit_tick_display_value", "exit_spot", "exit_tick",
                      "sell_spot_display_value", "sell_spot", "current_spot")
        if info["contract_type"] in DIGIT_TYPES and exit_ is not None:
            text = str(exit_)
            digit = int(text[-1]) if isinstance(exit_, str) and text[-1].isdigit() \
                else last_digit(exit_, self.decimals or 2)
            if digit_wins(info["contract_type"], info.get("barrier"), digit) != (profit > 0):
                log(f"Note: the exit price shown ({exit_}) is not the tick Deriv settled on - "
                    f"Deriv's result counts. Fields sent: {', '.join(sorted(poc))}")
        self.finish_trade(info["direction"], info["contract_type"], info["stake"],
                          info["ma_fast"], info["ma_slow"], info["rsi"], entry, exit_,
                          float(poc.get("payout", 0)), profit > 0, profit, info["contract_id"],
                          info.get("duration"))

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

    async def simulate(self, contract_type, barrier=None, ticks=None):
        """Dry run: mimic Deriv settlement on the live price feed.
        Entry = first tick after purchase; exit = Nth tick after entry
        (tick contracts) or the last tick at/before expiry (time contracts)."""
        loop = asyncio.get_running_loop()
        done = loop.create_future()
        state = {"entry": None, "entry_epoch": None, "ticks": 0, "last": None}
        digit = contract_type in DIGIT_TYPES
        dur, unit = (DIGIT_TICKS, "t") if digit else (self.duration, self.duration_unit)
        if ticks:
            dur, unit = ticks, "t"
        expiry_secs = duration_to_seconds(dur, unit)

        def listener(epoch, quote):
            if done.done():
                return
            if state["entry"] is None:
                state.update(entry=quote, entry_epoch=epoch)
                return
            state["ticks"] += 1
            if unit == "t":
                if state["ticks"] >= dur:
                    done.set_result(quote)
            elif epoch >= state["entry_epoch"] + expiry_secs:
                done.set_result(quote if epoch == state["entry_epoch"] + expiry_secs else state["last"])
            state["last"] = quote

        self.tick_listeners.append(listener)
        try:
            timeout = 120 if unit == "t" else expiry_secs + 120
            exit_spot = await asyncio.wait_for(done, timeout)
        finally:
            self.tick_listeners.remove(listener)
        entry = state["entry"]
        if digit:
            won = digit_wins(contract_type, barrier, last_digit(exit_spot, self.decimals or 2))
        else:
            won = exit_spot > entry if contract_type == "CALL" else exit_spot < entry
        return entry, exit_spot, won

    def finish_trade(self, direction, contract_type, stake, ma_fast, ma_slow, rsi,
                     entry, exit_spot, payout, won, profit, contract_id, duration=None):
        self.check_day_rollover()
        self.daily_pnl = round(self.daily_pnl + profit, 2)
        result = "WIN" if won else "LOSS"
        log(f"{'✅' if won else '❌'} {result} {self.symbol} {direction} | entry {entry} exit {exit_spot} | "
            f"P/L {profit:+.2f} | day {self.daily_pnl:+.2f} | balance {self.balance:.2f} {self.currency}")
        row = {
            "time": now_local().isoformat(timespec="seconds"), "mode": self.mode,
            "account": self.account, "symbol": self.symbol, "direction": direction,
            "contract_type": contract_type, "duration": duration or self.duration_label(self.contract),
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
            "ticks_history": self.symbol, "adjust_start_time": 1, "count": 100, "end": "latest",
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
                log(f"Loaded {len(self.closes)} candles for {self.symbol} | "
                    f"MA{MA_FAST}={fast.iloc[-1]:.5f} MA{MA_SLOW}={slow.iloc[-1]:.5f}")
            elif msg["msg_type"] == "ohlc":
                o = msg["ohlc"]
                if self.decimals is None and isinstance(o["close"], str) and "." in o["close"]:
                    self.decimals = len(o["close"].split(".")[1])
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
            if not self.risk_allows(0):   # balance target / daily limit already reached
                return
            show = SHOW_ACTIVE_SYMBOLS and not self.symbols_shown
            self.symbols_shown = True
            await self.load_active_symbols(show=show)
            await self.resolve_contract()
            if STRATEGY == "AI":
                await self.ensure_ai()
            for info in list(self.pending_contracts.values()):
                log(f"Resuming open contract #{info['contract_id']}")
                self.start_task(self.watch_contract(info))
            await self.stream_candles()
            if self.trade_tasks:
                if DRY_RUN:
                    for task in list(self.trade_tasks):
                        task.cancel()            # simulated trades, nothing to settle
                else:
                    log(f"Waiting for {len(self.trade_tasks)} open contract(s) to settle "
                        "(Ctrl+C again to quit now)...")
                    await asyncio.gather(*self.trade_tasks, return_exceptions=True)
        finally:
            pinger.cancel()
            for task in list(self.trade_tasks):
                task.cancel()
            await self.api.close()

    async def run(self):
        log(f"Vera Bot | {self.mode} | {self.symbol} | MA{MA_FAST}/MA{MA_SLOW} + RSI{RSI_PERIOD} on "
            f"{CANDLE_GRANULARITY}s candles | stake {STAKE} | martingale {MARTINGALE} | "
            f"daily stop {f'-{DAILY_STOP_LOSS}' if DAILY_STOP_LOSS else 'off'} | "
            f"take profit {f'+{DAILY_TAKE_PROFIT}' if DAILY_TAKE_PROFIT else 'off'} | "
            f"target {BALANCE_TARGET or 'off'} | bankroll {BANKROLL or 'off'}")
        if not self.first:
            await asyncio.sleep(3)   # stagger logins when watching several markets
        elif not DRY_RUN:
            log("!!! LIVE MODE - real contracts will be bought !!!")
        if self.first:
            log(f"Markets: {', '.join(SYMBOLS)} | Time zone: {TIMEZONE} (now {now_local():%H:%M %Z}) - "
                "the trading day resets at midnight")
            log(f"Trades: crossovers + trend trade after {TREND_TRADE_MINUTES or 'off'} min without one | "
                f"max {MAX_OPEN_TRADES} open | wait for next day {WAIT_NEXT_DAY}")
        while True:
            self.risk_allows(0)   # daily limit already hit today?
            await self.run_until_stopped()
            if not (WAIT_NEXT_DAY and self.daily_limit_hit):
                break
            now = now_local()
            midnight = datetime.combine(now.date() + timedelta(days=1), dtime(0), tzinfo=TZ)
            wait = midnight.timestamp() - now.timestamp() + 5
            log(f"Daily limit reached - pausing until 00:00 {now:%Z} ({wait / 3600:.1f} h). "
                "Ctrl+C to quit.")
            self.stop_event.clear()
            try:
                await asyncio.wait_for(self.stop_event.wait(), wait)
            except asyncio.TimeoutError:
                pass
            if not self.daily_limit_hit:   # Ctrl+C while waiting
                break
            self.stopped, self.stop_reason, self.daily_limit_hit = False, "", False
            self.stop_event.clear()
            self.check_day_rollover()
            log("New trading day - Vera Bot resumes")
        log(f"Vera Bot stopped. {self.stop_reason} | today P/L {self.daily_pnl:+.2f} | "
            f"trades logged in {CSV_FILE.name}")

    async def run_until_stopped(self):
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


def _shared_attr(name):
    return property(lambda self: getattr(self.shared, name),
                    lambda self, value: setattr(self.shared, name, value))


for _name in SHARED_FIELDS:
    setattr(TradingBot, _name, _shared_attr(_name))


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


def infer_decimals(prices):
    return max(len(str(x).split(".")[1]) if "." in str(x) else 0 for x in prices[-500:])


async def download_ticks(api, symbol, n_ticks):
    """Most recent n_ticks prices/times, paged backwards (the API sends 1000-5000 per request)."""
    prices, times, end = [], [], "latest"
    while len(prices) < n_ticks:
        want = min(5000, n_ticks - len(prices))
        hist = (await api.request({"ticks_history": symbol, "end": end, "count": want,
                                   "style": "ticks"}, timeout=60)).get("history") or {}
        p, t = list(hist.get("prices") or []), list(hist.get("times") or [])
        if times:   # keep only ticks older than what we already have
            keep = [i for i, x in enumerate(t) if int(x) < int(times[0])]
            p, t = [p[i] for i in keep], [t[i] for i in keep]
        if not p:
            break
        prices, times, end = p + prices, t + times, int(t[0]) - 1
        if len(prices) % 10000 < len(p):
            log(f"  {symbol}: {len(prices)} ticks...")
    return prices, times


async def train_cli(symbol):
    """`--train`: train and test the AI for one market, print the report."""
    token = os.getenv("DERIV_TOKEN", "").strip()
    bot = TradingBot(token, symbol)
    url, account = await get_session(token)
    bot.api = DerivAPI(url)
    await bot.api.connect()
    try:
        await bot.authorize(account)
        await bot.load_active_symbols(show=False)
        await bot.resolve_contract()
        await bot.train_ai()
        if bot.ai:
            r = bot.ai.report
            print(f"\n===== AI {symbol} =====\n{bot.ai.summary()}\n"
                  f"Contracts it picked in the unseen test: {r.get('picks')}\n"
                  "Log-loss below the chance value = the model learned something real; equal = nothing to learn.")
    finally:
        await bot.api.close()


async def backtest(n_ticks, symbol):
    """Replay the strategy on recent ticks for every contract type, priced with live payouts."""
    import numpy as np
    token = os.getenv("DERIV_TOKEN", "").strip()
    url, account = await get_session(token)
    api = DerivAPI(url)
    await api.connect()
    try:
        if account is None and token and not API_URL:
            await api.request({"authorize": token})
        currency = (account or {}).get("currency") or CURRENCY
        symbols = [normalize_symbol(x) for x in (await api.request({"active_symbols": "brief"}))["active_symbols"]]
        info = next((x for x in symbols if x["symbol"] == symbol), None)
        if info is None:
            raise SystemExit(f"Symbol {symbol!r} is not an active Deriv symbol")
        decimals = pip_decimals(info)

        log(f"Backtest {symbol}: downloading {n_ticks} ticks...")
        prices, times = await download_ticks(api, symbol, n_ticks)
        if len(prices) < 500:
            raise SystemExit(f"Only {len(prices)} ticks available - not enough for a backtest")
        if decimals is None:
            decimals = infer_decimals(prices)
        price = np.array([float(x) for x in prices])
        epoch = np.array([int(x) for x in times])
        digits = np.array([last_digit(x, decimals) for x in price])

        # live payouts for each contract, priced once at STAKE
        try:
            avail = (await api.request({"contracts_for": symbol}))["contracts_for"]["available"]
            types = {c.get("contract_type") for c in avail}
        except DerivAPIError:
            types = set()
        key = ["underlying_symbol"]

        async def payout_for(contract_type, extra):
            req = {"proposal": 1, "amount": STAKE, "basis": "stake", "currency": currency,
                   key[0]: symbol, "contract_type": contract_type, **extra}
            try:
                return float((await api.request(req))["proposal"]["payout"])
            except DerivAPIError as e:
                if e.code != "InputValidationFailed" or key[0] == "symbol":
                    return None
                key[0] = "symbol"
                return await payout_for(contract_type, extra)

        candidates = auto_candidates()
        if str(CONTRACT).upper() in DIGIT_TYPES and (str(CONTRACT).upper(), DIGIT_BARRIER) not in candidates:
            candidates.append((str(CONTRACT).upper(), DIGIT_BARRIER if needs_barrier(str(CONTRACT).upper()) else None))
        payouts = {}
        for contract, barrier in candidates:
            if contract == "RISEFALL":
                for ct in ("CALL", "PUT"):
                    payouts[(ct, None)] = await payout_for(ct, {"duration": DURATION, "duration_unit": DURATION_UNIT})
            elif not types or contract in types:
                extra = {"duration": DIGIT_TICKS, "duration_unit": "t"}
                if needs_barrier(contract):
                    extra["barrier"] = str(barrier)
                payouts[(contract, barrier)] = await payout_for(contract, extra)
    finally:
        await api.close()

    # 1-minute candles from the ticks, then the same decisions as live trading
    minute = epoch // CANDLE_GRANULARITY * CANDLE_GRANULARITY
    closes = pd.Series(price).groupby(minute).last().iloc[:-1]   # last candle still forming
    fast, slow, rsi = (x.to_numpy() for x in compute_indicators(closes))
    candle_end = closes.index.to_numpy() + CANDLE_GRANULARITY
    need = max(MA_SLOW, RSI_PERIOD) + 1

    def run(contract, barrier):
        direction_based = contract == "RISEFALL"
        results, last_trade = [], candle_end[0]
        for i in range(need, len(closes)):
            d, _ = decide(fast[i - 1], slow[i - 1], fast[i], slow[i], (candle_end[i] - last_trade) / 60)
            if not d or (direction_based and not rsi_allows(d, rsi[i])):
                continue
            last_trade = candle_end[i]
            entry = int(np.searchsorted(epoch, candle_end[i]))     # first tick after the decision
            if contract in DIGIT_TYPES:
                exit_ = entry + DIGIT_TICKS
                if exit_ >= len(price):
                    break
                won = digit_wins(contract, barrier, int(digits[exit_]))
                pay = payouts.get((contract, barrier))
            else:
                if DURATION_UNIT == "t":
                    exit_ = entry + DURATION
                else:
                    exit_ = int(np.searchsorted(epoch, epoch[min(entry, len(epoch) - 1)]
                                                + duration_to_seconds(DURATION, DURATION_UNIT), "right")) - 1
                if exit_ >= len(price) or entry >= len(price):
                    break
                ct = "CALL" if d == "RISE" else "PUT"
                won = price[exit_] > price[entry] if ct == "CALL" else price[exit_] < price[entry]
                pay = payouts.get((ct, None))
            if not pay:
                return None
            results.append(pay - STAKE if won else -STAKE)
        return results

    hours = (epoch[-1] - epoch[0]) / 3600
    print(f"\n===== Backtest {symbol}: {len(price)} ticks, {len(closes)} candles, "
          f"{hours:.1f} h, stake {STAKE} {currency} =====")
    print(f"Strategy: MA{MA_FAST}/MA{MA_SLOW} crossovers + trend trade after {TREND_TRADE_MINUTES or 'off'} "
          f"min, RSI filter {'on' if RSI_FILTER else 'off'} (Rise/Fall only)\n")
    print(f"{'Contract':<14}{'Trades':>7}{'Win %':>8}{'± luck':>8}{'Need %':>8}{'Payout':>9}{'P/L':>11}"
          f"{'Per trade':>11}{'Worst run':>11}")
    rows = []
    for contract, barrier in candidates:
        name = f"Rise/Fall {DURATION}{DURATION_UNIT}" if contract == "RISEFALL" else \
            f"{digit_label(contract, barrier)} {DIGIT_TICKS}t"
        res = run(contract, barrier)
        if not res:
            print(f"{name:<14}  not offered / no trades")
            continue
        wins = sum(1 for r in res if r > 0)
        streak = worst = 0
        for r in res:
            streak = streak + 1 if r < 0 else 0
            worst = max(worst, streak)
        pay = max(r for r in res) + STAKE if wins else payouts.get((contract, barrier)) or 0
        rows.append((sum(res) / len(res), name))
        chance = 0.5 if contract == "RISEFALL" else digit_win_chance(contract, barrier)
        luck = 2 * math.sqrt(chance * (1 - chance) / len(res))   # 95% range from luck alone
        print(f"{name:<14}{len(res):>7}{wins / len(res):>8.1%}{luck:>8.1%}{STAKE / pay:>8.1%}{pay:>9.2f}"
              f"{sum(res):>+11.2f}{sum(res) / len(res):>+11.2f}{worst:>9} L")
    if rows:
        best = max(rows)
        print(f"\nBest here: {best[1]} ({best[0]:+.2f} per trade).\n"
              "'Need %' = win rate that breaks even. '± luck' = how far Win % can move by pure chance\n"
              "with this many trades. A result only means something if Win % beats Need % by more\n"
              "than the luck range - otherwise it's luck. More ticks (--backtest 100000) = less luck.")


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
    global SYMBOLS
    args = sys.argv[1:]
    if not API_URL:
        check_env()
    if "--train" in args:
        for symbol in SYMBOLS:
            await train_cli(symbol)
        return
    if "--backtest" in args:
        i = args.index("--backtest")
        n = int(args[i + 1]) if len(args) > i + 1 and args[i + 1].isdigit() else BACKTEST_TICKS
        for symbol in SYMBOLS:
            await backtest(n, symbol)
        return
    if "--symbols" in args:
        try:
            await list_symbols()
        except websockets.exceptions.WebSocketException as e:
            explain_http_520(e)
            raise
        return
    if args and not args[0].startswith("-"):
        SYMBOLS = [x.strip() for x in args[0].split(",") if x.strip()]
    token = os.getenv("DERIV_TOKEN", "").strip()
    bots, shared = [], None
    for symbol in SYMBOLS:
        bot = TradingBot(token, symbol, shared)
        shared = bot.shared
        bots.append(bot)
    loop = asyncio.get_running_loop()

    def on_signal():
        if shared.stopped:
            for bot in bots:
                for task in list(bot.trade_tasks):
                    task.cancel()            # second Ctrl+C: don't wait for open contracts
        shared.daily_limit_hit = False       # Ctrl+C always quits, also while waiting for tomorrow
        bots[0].stop("stopped by user (Ctrl+C)")
        shared.stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, on_signal)
    async def run_market(bot):
        try:
            await bot.run()
        except SystemExit as e:        # one market can't run -> stop them all, cleanly
            shared.daily_limit_hit = False
            bot.stop(f"{bot.symbol}: {e}")
            shared.stop_event.set()
            return f"{bot.symbol}: {e}"
        return None

    errors = [e for e in await asyncio.gather(*(run_market(bot) for bot in bots)) if e]
    if errors:
        raise SystemExit("\n".join(errors))


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)
