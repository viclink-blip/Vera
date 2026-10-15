#!/usr/bin/env python3
"""
Deriv MA-crossover trading bot.

Strategy : MA7 / MA14 crossover on 1-minute candles.
           MA7 crosses above MA14 -> Rise (CALL)
           MA7 crosses below MA14 -> Fall (PUT)
API      : wss://ws.binaryws.com/websockets/v3?app_id=1089
Auth     : DERIV_TOKEN in .env (demo or real token - the token decides the account)

Run      : python3 deriv_bot.py
"""

import asyncio
import csv
import json
import os
import signal
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import websockets
from dotenv import load_dotenv

load_dotenv(Path(__file__).with_name(".env"))

# ============================================================================
#  SETTINGS - edit these
# ============================================================================
SYMBOL = "R_75"              # Volatility 75 Index. Forex example: "frxEURUSD"
STAKE = 1                    # USD per trade (Deriv minimum is 0.35)
CURRENCY = "USD"

DURATION = 5                 # contract length...
DURATION_UNIT = "t"          # ..."t" ticks, "s" seconds, "m" minutes, "h", "d"
AUTO_ADJUST_DURATION = True  # if DURATION isn't offered for SYMBOL (e.g. ticks on
                             # forex), switch to the shortest duration Deriv allows

MA_FAST = 7
MA_SLOW = 14
CANDLE_GRANULARITY = 60      # seconds -> 1-minute candles

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
    "duration", "stake", "ma_fast", "ma_slow", "entry_spot", "exit_spot",
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

        self.closes = pd.Series(dtype=float)   # closed candles, index = open_time
        self.current_open_time = None
        self.current_close = None
        self.last_tick_epoch = None

        self.stake = float(STAKE)
        self.loss_streak = 0
        self.trading_day = now_utc().date()
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

    async def resolve_duration(self):
        """Make sure the Rise/Fall duration is offered for SYMBOL."""
        try:
            resp = await self.api.request({"contracts_for": SYMBOL, "currency": self.currency})
        except DerivAPIError as e:
            log(f"contracts_for failed ({e}); using {DURATION}{DURATION_UNIT}")
            return
        offers = [
            c for c in resp["contracts_for"]["available"]
            if c["contract_type"] == "CALL" and c.get("barriers") == 0
            and c["expiry_type"] in ("tick", "intraday")
        ]
        if not offers:
            raise SystemExit(f"Rise/Fall is not offered for {SYMBOL} right now (market closed?)")

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
            log(f"Contract duration: {DURATION}{DURATION_UNIT}")
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

    def on_candle_closed(self, open_time, close):
        self.closes.loc[open_time] = close
        self.closes = self.closes.iloc[-200:]
        if len(self.closes) < MA_SLOW + 1:
            log(f"Collecting candles {len(self.closes)}/{MA_SLOW + 1}")
            return
        fast, slow = self._ma()
        f_prev, s_prev, f_now, s_now = fast.iloc[-2], slow.iloc[-2], fast.iloc[-1], slow.iloc[-1]
        log(f"{SYMBOL} candle close {close} | MA{MA_FAST}={f_now:.5f} MA{MA_SLOW}={s_now:.5f}")

        direction = None
        if f_prev <= s_prev and f_now > s_now:
            direction = "RISE"
        elif f_prev >= s_prev and f_now < s_now:
            direction = "FALL"
        if not direction:
            return
        log(f">>> Signal {direction}: MA{MA_FAST} crossed {'above' if direction == 'RISE' else 'below'} MA{MA_SLOW}")
        if self.trade_task and not self.trade_task.done():
            log("Trade still open - signal skipped")
            return
        self.trade_task = asyncio.create_task(self.trade(direction, f_now, s_now))

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
    async def trade(self, direction, ma_fast, ma_slow):
        stake = round(self.stake, 2)
        if self.stopped or not self.risk_allows(stake):
            return
        contract_type = "CALL" if direction == "RISE" else "PUT"
        try:
            proposal = (await self.api.request({
                "proposal": 1, "amount": stake, "basis": "stake",
                "contract_type": contract_type, "currency": self.currency,
                "duration": self.duration, "duration_unit": self.duration_unit,
                "symbol": SYMBOL,
            }))["proposal"]
        except DerivAPIError as e:
            log(f"Proposal rejected: {e}")
            return
        payout = float(proposal["payout"])
        log(f"Proposal {contract_type} stake {stake:.2f} payout {payout:.2f} spot {proposal['spot']}")

        if DRY_RUN:
            entry, exit_, won = await self.simulate(contract_type)
            profit = round(payout - stake, 2) if won else -stake
            self.balance = round(self.balance + profit, 2)
            self.finish_trade(direction, contract_type, stake, ma_fast, ma_slow,
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
                                     ma_fast=ma_fast, ma_slow=ma_slow, contract_id=contract_id)
        await self.watch_contract()

    async def watch_contract(self):
        """Follow a live contract until it is sold/expired, then record it."""
        info = self.pending_contract
        rid, q = await self.api.subscribe({"proposal_open_contract": 1,
                                           "contract_id": info["contract_id"]})
        sub_id = None
        try:
            while True:
                msg = await asyncio.wait_for(q.get(), timeout=600)
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
                          info["ma_fast"], info["ma_slow"],
                          poc.get("entry_spot") or poc.get("entry_tick"),
                          poc.get("exit_tick") or poc.get("sell_spot"),
                          float(poc.get("payout", 0)), profit > 0, profit, info["contract_id"])

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

    def finish_trade(self, direction, contract_type, stake, ma_fast, ma_slow,
                     entry, exit_spot, payout, won, profit, contract_id):
        self.check_day_rollover()
        self.daily_pnl = round(self.daily_pnl + profit, 2)
        result = "WIN" if won else "LOSS"
        log(f"{'✅' if won else '❌'} {result} {direction} | entry {entry} exit {exit_spot} | "
            f"P/L {profit:+.2f} | day {self.daily_pnl:+.2f} | balance {self.balance:.2f} {self.currency}")
        row = {
            "time": now_utc().isoformat(timespec="seconds"), "mode": self.mode,
            "account": self.account, "symbol": SYMBOL, "direction": direction,
            "contract_type": contract_type, "duration": f"{self.duration}{self.duration_unit}",
            "stake": f"{stake:.2f}", "ma_fast": f"{ma_fast:.5f}", "ma_slow": f"{ma_slow:.5f}",
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
            await self.resolve_duration()
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
        log(f"Deriv bot | {self.mode} | {SYMBOL} | MA{MA_FAST}/MA{MA_SLOW} on "
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


async def main():
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
