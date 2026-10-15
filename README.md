# Vera Bot

**Vera Bot** is a Python bot for the [Deriv](https://deriv.com) WebSocket API that works with **any Deriv symbol**. It trades when a moving-average crossover happens on 1-minute candles, filtered by RSI.

- **Signal:** MA7 crosses above MA14 → **Rise / Up**. MA7 crosses below MA14 → **Fall / Down**.
- **RSI(14) filter:** a Rise is skipped when RSI ≥ 70 (overbought) and a Fall is skipped when RSI ≤ 30 (oversold).
- **Any symbol:** Volatility (`R_75`, `1HZ100V`), Boom/Crash (`BOOM1000`, `CRASH500`), Jump, forex (`frxEURUSD`), metals (`frxXAUUSD`), crypto and more. At startup the bot prints every active Deriv symbol and checks yours.
- **Contract type is chosen automatically:** Rise/Fall when the symbol offers it, otherwise **Multipliers** (Up/Down). Boom/Crash only offer Multipliers.
- **Safety:** dry run by default, a daily stop loss, optional martingale, and every trade logged to `vera_trades.csv`.

> ⚠️ Trading binary/digital options carries a high risk of loss. Nothing here is a profitable strategy guarantee. Always test on a **demo** account first.

---

## 1. Install (Linux)

```bash
git clone <this repo> && cd <repo>
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Needs Python 3.9 or newer.

## 2. Get your Deriv token and App ID

Deriv's current API uses a **personal access token** (starts with `pat_`) together with the **App ID** of an app you register. The old `ws.binaryws.com` / `app_id=1089` connection is shut down and returns HTTP 520.

1. Log in at **https://developers.deriv.com** with your Deriv account.
2. In the dashboard, **register a new application** and choose the PAT (personal access token) type. Copy its **App ID**.
3. Under that app, **create a personal access token** with the **trade** scope (plus account/read if offered). Copy it straight away, because Deriv shows it only once.

One token can trade both your demo and your real account. Choose which one with `ACCOUNT_TYPE` at the top of `vera_bot.py` (`"demo"` by default). At startup the bot prints the account it's using, e.g. `Logged in DOT1234567 (DEMO)`.

Keep the token secret. Don't paste it into chats or screenshots, and don't `cat .env` where others can see it. If it leaks, delete it in the dashboard and create a new one.

## 3. Configure

```bash
cp .env.example .env
nano .env
```

`.env` must contain exactly these two lines (no spaces, no quotes):

```
DERIV_TOKEN=pat_xxxxxxxxxxxxxxxx
DERIV_APP_ID=12345
```

Settings are at the top of `vera_bot.py`:

| Setting | Default | Meaning |
|---|---|---|
| `ACCOUNT_TYPE` | `"demo"` | `"demo"` or `"real"`: which of your Deriv accounts to use |
| `SYMBOL` | `"R_75"` | Any Deriv symbol: `BOOM1000`, `CRASH500`, `frxEURUSD`, `frxXAUUSD`… |
| `SHOW_ACTIVE_SYMBOLS` | `True` | Print all active symbols at startup |
| `CONTRACT_MODE` | `"auto"` | `"auto"`, `"risefall"` or `"multiplier"` |
| `STAKE` | `1` | USD per trade (Deriv minimum 0.35) |
| `DURATION` / `DURATION_UNIT` | `5`, `"t"` | Contract length: 5 ticks. Use `1`, `"m"` for 1 minute |
| `AUTO_ADJUST_DURATION` | `True` | Switches to the shortest duration Deriv allows if the one you set isn't offered (forex has no tick contracts) |
| `MULTIPLIER` | `100` | Multiplier value (the nearest allowed value is used) |
| `MULT_HOLD_SECONDS` | `60` | Close a Multiplier position after this many seconds |
| `MULT_TAKE_PROFIT` / `MULT_STOP_LOSS` | `0` | USD limits Deriv applies to Multiplier positions (0 = off) |
| `MA_FAST` / `MA_SLOW` | `7` / `14` | Moving-average lengths |
| `RSI_PERIOD` / `RSI_FILTER` | `14` / `True` | RSI length; turn the filter on or off |
| `RSI_OVERBOUGHT` / `RSI_OVERSOLD` | `70` / `30` | RSI filter levels |
| `CANDLE_GRANULARITY` | `60` | Candle size in seconds |
| `MARTINGALE` | `False` | After a loss, multiply the next stake by `MARTINGALE_MULTIPLIER` (2.1) |
| `MARTINGALE_MAX_STEPS` | `5` | Go back to `STAKE` after this many losses in a row |
| `DAILY_STOP_LOSS` | `10` | Stop when today's P/L reaches -10 USD |
| `DAILY_TAKE_PROFIT` | `0` | Stop when today's P/L reaches +X (0 = off) |
| `BANKROLL` | `50` | Trade as if the account held only this much USD. The bot stops when it can't cover the next stake. It carries over between runs; rename `vera_trades.csv` to start over. `0` = use the whole account balance |
| `DRY_RUN` | `True` | Simulate trades on live prices without buying |

## 4. Run

```bash
python3 vera_bot.py               # trade SYMBOL from the top of the file
python3 vera_bot.py BOOM1000      # trade another symbol without editing the file
python3 vera_bot.py --symbols     # just list all active Deriv symbols and exit
```

Symbol codes are case-sensitive (`BOOM1000`, not `boom1000`). If you mistype one, the bot suggests the correct code.

Stop it with **Ctrl+C**. In live mode the bot waits for an open contract to settle before it exits. Press Ctrl+C again to quit straight away; the contract still settles on Deriv.

### Dry run → live

1. Run with `DRY_RUN = True` (the default). The bot uses real Deriv prices and real proposals (payouts) and settles each simulated trade the way Deriv does: the entry spot is the next tick and the exit is the Nth tick after it (or the price at expiry). A tie counts as a loss. A token is optional in dry run.
2. Set `DRY_RUN = False` with a **demo** token. The bot now buys real contracts with virtual money.
3. Move to a real-account token only once you are happy with the results.

Run it in the background on a server:

```bash
nohup python3 vera_bot.py >> bot.log 2>&1 &
tail -f bot.log
```

## How it works

1. Sends `DERIV_TOKEN` and `DERIV_APP_ID` to `https://api.derivws.com`, picks your demo or real account, and gets a one-time, already-logged-in WebSocket URL. A fresh URL is requested on every reconnect.
2. Calls `active_symbols`, prints them grouped by market, and checks that `SYMBOL` exists and whether its market is open.
3. Calls `contracts_for` to pick the contract: Rise/Fall when it is offered (checking the duration), otherwise Multipliers (checking the multiplier).
4. Subscribes to 1-minute candles with `ticks_history` (`style: candles`, `subscribe: 1`). This loads the last 100 candles and then streams every tick.
5. When a candle closes, it recalculates MA7, MA14 and RSI14 with pandas and checks for a crossover that passes the RSI filter.
6. On a signal it sends a `proposal` (`CALL`/`PUT` with a duration, or `MULTUP`/`MULTDOWN` with a multiplier), then a `buy`, then follows the contract with `proposal_open_contract`. Multiplier positions are closed with `sell` after `MULT_HOLD_SECONDS`, unless take profit, stop loss or a stop out closes them first.
7. Logs each trade to the console and to `vera_trades.csv`, updates the stake (martingale) and checks the daily limits.

Other behaviour:

- Only one trade is open at a time. Signals that arrive while a trade is open are skipped.
- If the connection drops, the bot reconnects with backoff. It also resumes watching a live contract that was open at the time.
- Today's P/L (UTC day) is read back from the CSV at startup, so restarting the bot does not reset the daily stop loss.
- Before each trade the bot checks that losing the next stake would not push you past `DAILY_STOP_LOSS`. If it would, the bot stops. This matters most with martingale on.

### CSV columns

`time, mode, account, symbol, direction, contract_type, duration, stake, ma_fast, ma_slow, rsi, entry_spot, exit_spot, payout, result, profit, daily_pnl, balance, contract_id`

## Notes

- **Multipliers:** the most you can lose is the stake, and Deriv charges a commission. Boom/Crash have sudden spikes, so a position can be stopped out within seconds. In dry run, Multiplier P/L is estimated as `stake × multiplier × price change % − commission`. This is close to Deriv's figure but not exact.
- If you upgraded from the earlier version, an existing `deriv_trades.csv` is renamed to `vera_trades.csv` and gets the new `rsi` column added automatically.
- Forex (`frx…`) only trades on weekdays, and Deriv does not offer tick-duration Rise/Fall on it. With `AUTO_ADJUST_DURATION` the bot picks the shortest allowed duration and prints it.
- Old-style tokens (not starting with `pat_`) still use the legacy `ws.derivws.com` connection. Deriv has shut that down, so create a `pat_` token instead.
- Never commit your `.env`. It is already in `.gitignore`.
