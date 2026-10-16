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

Settings are at the top of `vera_bot.py`. The most common ones can also be set in `.env` as `VERA_<NAME>=value`: `VERA_ACCOUNT_TYPE`, `VERA_SYMBOL`, `VERA_STAKE`, `VERA_DRY_RUN`, `VERA_BANKROLL`, `VERA_DAILY_STOP_LOSS`, `VERA_DAILY_TAKE_PROFIT`, `VERA_BALANCE_TARGET`, `VERA_MARTINGALE`, `VERA_MAX_OPEN_TRADES`, `VERA_TREND_TRADE_MINUTES`, `VERA_WAIT_NEXT_DAY`, `VERA_TIMEZONE`, `VERA_CONTRACT`, `VERA_DIGIT_BARRIER` and `VERA_DIGIT_TICKS`. Values in `.env` win over the file, and keeping your changes there means `git pull` never conflicts with them. `0` turns `DAILY_STOP_LOSS` and `BANKROLL` off.

| Setting | Default | Meaning |
|---|---|---|
| `ACCOUNT_TYPE` | `"demo"` | `"demo"` or `"real"`: which of your Deriv accounts to use |
| `SYMBOL` | `"R_75"` | Any Deriv symbol: `BOOM1000`, `CRASH500`, `frxEURUSD`, `frxXAUUSD`… |
| `SHOW_ACTIVE_SYMBOLS` | `True` | Print all active symbols at startup |
| `CONTRACT` | `"RISEFALL"` | What to trade: `RISEFALL`, `MULTIPLIER`, `DIGITEVEN`, `DIGITODD`, `DIGITMATCH`, `DIGITDIFF`, `DIGITOVER`, `DIGITUNDER`, or `AUTO` (before each trade, compare live payouts of all of them and take the one with the smallest Deriv edge) |
| `DIGIT_BARRIER` | `5` | The digit for Matches / Differs / Over / Under (Over 0-8, Under 1-9) |
| `DIGIT_TICKS` | `5` | Length of digit contracts in ticks (1-10) |
| `CONTRACT_MODE` | `"auto"` | With `CONTRACT = RISEFALL`: `"auto"` falls back to Multipliers when the symbol has no Rise/Fall; `"risefall"` never does |
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
| `MAX_OPEN_TRADES` | `3` | How many trades can be open at the same time; a new signal is only skipped when this many are open |
| `TREND_TRADE_MINUTES` | `6` | If no trade happened for this many minutes, trade the current trend (MA7 above MA14 = Rise, below = Fall, RSI filter still applies). `6` gives at least 5 trades per 30 minutes; `0` = crossovers only |
| `TIMEZONE` | `"Africa/Nairobi"` | Clock for the log, `vera_trades.csv` and the trading day. Daily P/L, take profit and stop loss reset at midnight in this time zone. Default is East Africa Time (UTC+3); any name like `Africa/Dar_es_Salaam`, `Africa/Kampala` or `UTC` works |
| `WAIT_NEXT_DAY` | `True` | After the daily stop loss / take profit, pause until midnight (`TIMEZONE`) and carry on instead of exiting |
| `MARTINGALE` | `False` | After a loss, multiply the next stake by `MARTINGALE_MULTIPLIER` (2.1) |
| `MARTINGALE_MAX_STEPS` | `5` | Go back to `STAKE` after this many losses in a row |
| `DAILY_STOP_LOSS` | `10` | Stop when today's P/L reaches -10 USD |
| `DAILY_TAKE_PROFIT` | `0` | Stop when today's P/L reaches +X (0 = off) |
| `BALANCE_TARGET` | `0` | Stop for good when the balance reaches this amount (e.g. `30`), so you can withdraw the profit. It doesn't restart until you raise or remove it. `0` = off |
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

## Backtest: test before you trade

```bash
python3 vera_bot.py --backtest            # last 20,000 ticks of SYMBOL
python3 vera_bot.py --backtest 100000     # more history = more reliable
```

The backtest downloads recent ticks, runs the same strategy as live trading, and settles every contract type (Rise/Fall, Even, Odd, Differs, Matches, Over, Under) with Deriv's current payouts. It prints the number of trades, the win rate, the luck range, the break-even win rate, P/L and the worst losing streak for each. A contract only makes money if its win rate beats the break-even rate by more than the luck range over many trades.

## Run it 24 hours a day

`vera-bot.service` runs the bot in the background with systemd. It restarts the bot if it crashes and starts it again after a reboot:

```bash
mkdir -p ~/.config/systemd/user
cp ~/vera-bot/vera-bot.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now vera-bot
sudo loginctl enable-linger $USER      # keep it running after logout / reboot
journalctl --user -u vera-bot -f       # watch the log (Ctrl+C stops watching, not the bot)
systemctl --user stop vera-bot         # stop the bot
systemctl --user restart vera-bot      # restart it after git pull or .env changes
```

The computer must stay on, awake and online. On a laptop, disable sleep and lid-close suspend, or run the bot on a small Linux VPS.

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
- Today's P/L (day in `TIMEZONE`, East Africa Time by default) is read back from the CSV at startup, so restarting the bot does not reset the daily stop loss.
- Before each trade the bot checks that losing the next stake would not push you past `DAILY_STOP_LOSS`. If it would, the bot stops. This matters most with martingale on.

### CSV columns

`time, mode, account, symbol, direction, contract_type, duration, stake, ma_fast, ma_slow, rsi, entry_spot, exit_spot, payout, result, profit, daily_pnl, balance, contract_id`

## Notes

- **Multipliers:** the most you can lose is the stake, and Deriv charges a commission. Boom/Crash have sudden spikes, so a position can be stopped out within seconds. In dry run, Multiplier P/L is estimated as `stake × multiplier × price change % − commission`. This is close to Deriv's figure but not exact.
- If you upgraded from the earlier version, an existing `deriv_trades.csv` is renamed to `vera_trades.csv` and gets the new `rsi` column added automatically.
- Forex (`frx…`) only trades on weekdays, and Deriv does not offer tick-duration Rise/Fall on it. With `AUTO_ADJUST_DURATION` the bot picks the shortest allowed duration and prints it.
- Old-style tokens (not starting with `pat_`) still use the legacy `ws.derivws.com` connection. Deriv has shut that down, so create a `pat_` token instead.
- Never commit your `.env`. It is already in `.gitignore`.
