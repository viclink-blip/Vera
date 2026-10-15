#!/usr/bin/env python3
"""
Deriv MA-crossover trading bot.

Strategy : MA7 / MA14 crossover on 1-minute candles, filtered by RSI(14).
           MA7 crosses above MA14 (and RSI not overbought) -> Rise / Up
           MA7 crosses below MA14 (and RSI not oversold)   -> Fall / Down
Symbols  : any Deriv symbol (R_75, 1HZ100V, BOOM1000, CRASH500, frxEURUSD,
           frxXAUUSD, ...). Uses Rise/Fall where the symbol offers it, else
           Multipliers (e.g. Boom/Crash).
API      : wss://ws.binaryws.com/websockets/v3?app_id=1089
Auth     : DERIV_TOKEN in .env (demo or real token - the token decides the account)

Run      : python3 deriv_bot.py              (uses SYMBOL below)
           python3 deriv_bot.py BOOM1000     (override the symbol)
           python3 deriv_bot.py --symbols    (list active symbols and exit)
"""

import asyncio
import csv
import json
import os
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import websockets
from dotenv import load_dotenv

load_dotenv(Path(__file__).with_name(".env"))

# ============================================================================
#  SETTINGS - edit these
# ============================================================================
SYMBOL = "R_75"              # any Deriv symbol, e.g. R_75, R_100, 1HZ100V, BOOM1000,
                             # CRASH500, JD25, frxEURUSD, frxXAUUSD, cryBTCUSD ...
                             # run `python3 deriv_bot.py --symbols` to see them all
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

DRY_RUN = True               # True = simulate trades on live prices, no real buys
DRY_RUN_START_BALANCE = 1000.0  # simulated balance when no token is set

APP_ID = 1089
API_URL = os.getenv("DERIV_API_URL") or f"wss://ws.binaryws.com/websockets/v3?app_id={APP_ID}"
CSV_FILE = Path(__file__).with_name("deriv_trades.csv")
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

    async def connect(self):
        self.ws = await websockets.connect(
            self.url, ping_interval=20, ping_timeout=20, max_size=2**22
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
        if not CSV_FILE.exists():
            return
        with CSV_FILE.open() as f:
            header = f.readline().strip().split(",")
        if header != CSV_COLUMNS:
            pd.read_csv(CSV_FILE).reindex(columns=CSV_COLUMNS).to_csv(CSV_FILE, index=False)

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

    async def authorize(self):
        if not self.token:
            if not DRY_RUN:
                raise SystemExit("DERIV_TOKEN missing in .env - required when DRY_RUN = False")
            log("No DERIV_TOKEN - running dry run without an account")
            return
        auth = (await self.api.request({"authorize": self.token}))["authorize"]
        self.account = auth["loginid"]
        self.is_virtual = bool(auth.get("is_virtual"))
        self.currency = auth.get("currency") or CURRENCY
        if not DRY_RUN:
            self.balance = float(auth["balance"])
        kind = "DEMO" if self.is_virtual else "REAL"
        log(f"Authorized {self.account} ({kind}) balance {float(auth['balance']):.2f} {self.currency}")

    async def load_active_symbols(self, show):
        """Fetch every active Deriv symbol, optionally print them, and check SYMBOL."""
        resp = await self.api.request({"active_symbols": "brief"})
        symbols = resp["active_symbols"]
        for sym in symbols:  # newer API versions call it underlying_symbol
            sym["symbol"] = sym.get("symbol") or sym.get("underlying_symbol", "")
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
            resp = await self.api.request({"contracts_for": SYMBOL, "currency": self.currency})
        except DerivAPIError as e:
            log(f"contracts_for failed ({e}); using Rise/Fall {DURATION}{DURATION_UNIT}")
            return
        available = resp["contracts_for"]["available"]
        offers = [
            c for c in available
            if c["contract_type"] == "CALL" and c.get("barriers") == 0
            and c["expiry_type"] in ("tick", "intraday", "daily")
        ]
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

        def allowed(value, unit):
            for c in offers:
                lo_v, lo_u = parse_duration(c["min_contract_duration"])
                hi_v, hi_u = parse_duration(c["max_contract_duration"])
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
        mins = [parse_duration(c["min_contract_duration"]) for c in offers]
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
            self.stop(f"Insufficient balance {self.balance:.2f} for stake {stake:.2f}")
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
               "currency": self.currency, "symbol": SYMBOL}
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
            proposal = (await self.api.request(req))["proposal"]
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
                raise ConnectionError("candle stream closed")
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

    async def run_session(self):
        self.api = DerivAPI(API_URL)
        await self.api.connect()
        log(f"Connected to {API_URL}")
        try:
            await self.authorize()
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
            if self.trade_task and not self.trade_task.done():
                self.trade_task.cancel()
            await self.api.close()

    async def run(self):
        log(f"Deriv bot | {self.mode} | {SYMBOL} | MA{MA_FAST}/MA{MA_SLOW} + RSI{RSI_PERIOD} on "
            f"{CANDLE_GRANULARITY}s candles | stake {STAKE} | martingale {MARTINGALE} | "
            f"daily stop -{DAILY_STOP_LOSS} | today P/L {self.daily_pnl:+.2f}")
        if not DRY_RUN:
            log("!!! LIVE MODE - real contracts will be bought !!!")
        self.risk_allows(0)   # daily limit already hit earlier today?
        delay = 2
        while not self.stopped:
            try:
                await self.run_session()
                delay = 2
            except DerivAPIError as e:
                if e.code in ("InvalidToken", "AuthorizationRequired"):
                    raise SystemExit(f"Authorization failed: {e}")
                log(f"API error: {e}")
            except (ConnectionError, OSError, asyncio.TimeoutError,
                    websockets.exceptions.WebSocketException) as e:
                log(f"Connection problem: {e!r}")
            if self.stopped:
                break
            log(f"Reconnecting in {delay}s...")
            try:
                await asyncio.wait_for(self.stop_event.wait(), delay)
            except asyncio.TimeoutError:
                pass
            delay = min(delay * 2, 60)
        log(f"Bot stopped. {self.stop_reason} | today P/L {self.daily_pnl:+.2f} | "
            f"trades logged in {CSV_FILE.name}")


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
    api = DerivAPI(API_URL)
    await api.connect()
    try:
        resp = await api.request({"active_symbols": "brief"})
        symbols = resp["active_symbols"]
        for sym in symbols:
            sym["symbol"] = sym.get("symbol") or sym.get("underlying_symbol", "")
        print_symbols(symbols)
    finally:
        await api.close()


async def main():
    global SYMBOL
    args = sys.argv[1:]
    if "--symbols" in args:
        await list_symbols()
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
