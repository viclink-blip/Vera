# Deriv MA Crossover Bot

A Python bot for the [Deriv](https://deriv.com) WebSocket API. It trades Rise/Fall contracts when a moving-average crossover happens on 1-minute candles.

- **Signal:** MA7 crosses above MA14 → **Rise** (CALL). MA7 crosses below MA14 → **Fall** (PUT).
- **Markets:** `R_75` (Volatility 75) by default. Switch to `frxEURUSD` or any other symbol at the top of `deriv_bot.py`.
- **Safety:** dry run by default, a daily stop loss, optional martingale, and every trade logged to `deriv_trades.csv`.

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

## 2. Get your Deriv API token

1. Log in at **https://app.deriv.com**.
2. Choose the account the bot should trade from the account switcher (top right):
   - **Demo** account (`VRTC…`) – virtual money, use this first.
   - **Real** account (`CR…`) – real money.
3. Open **Account Settings → Security and safety → API token**
   (direct link: https://app.deriv.com/account/api-token).
4. Name the token (e.g. `mybot`) and tick the scopes **Read** and **Trade**.
5. Click **Create**, then copy the token.

A token belongs to the account that was active when you created it. To switch between demo and real, create a token on each account and change `DERIV_TOKEN`. The bot prints which account it is using at startup: `Authorized VRTC1234567 (DEMO)`.

## 3. Configure

```bash
cp .env.example .env
nano .env          # paste: DERIV_TOKEN=xxxxxxxxxxxx
```

Settings are at the top of `deriv_bot.py`:

| Setting | Default | Meaning |
|---|---|---|
| `SYMBOL` | `"R_75"` | Market to trade. Forex: `"frxEURUSD"` |
| `STAKE` | `1` | USD per trade (Deriv minimum 0.35) |
| `DURATION` / `DURATION_UNIT` | `5`, `"t"` | Contract length: 5 ticks. Use `1`, `"m"` for 1 minute |
| `AUTO_ADJUST_DURATION` | `True` | Switches to the shortest duration Deriv allows if the one you set isn't offered (forex has no tick contracts) |
| `MA_FAST` / `MA_SLOW` | `7` / `14` | Moving-average lengths |
| `CANDLE_GRANULARITY` | `60` | Candle size in seconds |
| `MARTINGALE` | `False` | After a loss, multiply the next stake by `MARTINGALE_MULTIPLIER` (2.1) |
| `MARTINGALE_MAX_STEPS` | `5` | Go back to `STAKE` after this many losses in a row |
| `DAILY_STOP_LOSS` | `10` | Stop when today's P/L reaches -10 USD |
| `DAILY_TAKE_PROFIT` | `0` | Stop when today's P/L reaches +X (0 = off) |
| `DRY_RUN` | `True` | Simulate trades on live prices without buying |

## 4. Run

```bash
python3 deriv_bot.py
```

Stop it with **Ctrl+C**. In live mode the bot waits for an open contract to settle before it exits. Press Ctrl+C again to quit straight away; the contract still settles on Deriv.

### Dry run → live

1. Run with `DRY_RUN = True` (the default). The bot uses real Deriv prices and real proposals (payouts) and settles each simulated trade the way Deriv does: the entry spot is the next tick and the exit is the Nth tick after it (or the price at expiry). A tie counts as a loss. A token is optional in dry run.
2. Set `DRY_RUN = False` with a **demo** token. The bot now buys real contracts with virtual money.
3. Move to a real-account token only once you are happy with the results.

Run it in the background on a server:

```bash
nohup python3 deriv_bot.py >> bot.log 2>&1 &
tail -f bot.log
```

## How it works

1. Connects to `wss://ws.binaryws.com/websockets/v3?app_id=1089` and authorizes with `DERIV_TOKEN`.
2. Calls `contracts_for` to check that the Rise/Fall duration is offered for the symbol.
3. Subscribes to 1-minute candles with `ticks_history` (`style: candles`, `subscribe: 1`). This loads the last 100 candles and then streams every tick.
4. When a candle closes, it recalculates MA7 and MA14 with pandas and checks for a crossover.
5. On a signal it sends a `proposal`, then a `buy`, then follows the contract with `proposal_open_contract` until it is sold.
6. Logs each trade to the console and to `deriv_trades.csv`, updates the stake (martingale) and checks the daily limits.

Other behaviour:

- Only one trade is open at a time. Signals that arrive while a trade is open are skipped.
- If the connection drops, the bot reconnects with backoff. It also resumes watching a live contract that was open at the time.
- Today's P/L (UTC day) is read back from the CSV at startup, so restarting the bot does not reset the daily stop loss.
- Before each trade the bot checks that losing the next stake would not push you past `DAILY_STOP_LOSS`. If it would, the bot stops. This matters most with martingale on.

### CSV columns

`time, mode, account, symbol, direction, contract_type, duration, stake, ma_fast, ma_slow, entry_spot, exit_spot, payout, result, profit, daily_pnl, balance, contract_id`

## Notes

- Forex (`frx…`) only trades on weekdays, and Deriv does not offer tick-duration Rise/Fall on it. With `AUTO_ADJUST_DURATION` the bot picks the shortest allowed duration and prints it.
- `app_id=1089` is Deriv's public test app id. For heavy use, register your own at https://api.deriv.com and change `APP_ID`.
- Never commit your `.env`. It is already in `.gitignore`.
