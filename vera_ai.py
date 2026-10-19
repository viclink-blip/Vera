"""
Vera AI - machine-learning signals for Vera Bot.

Two gradient-boosting models learn from recent ticks of a market:
  * a direction model: will the price be higher after N ticks?  (Rise / Fall)
  * a digit model:     what is the last digit of the tick N ticks ahead? (0-9)
From those, the win chance of every contract is derived (Rise, Fall, Even, Odd,
Matches / Differs / Over / Under on every digit).

Before the AI may trade, a walk-forward test runs the models on the most recent
ticks they never saw, priced with Deriv's real payouts. The model only "passes"
if that out-of-sample test is profitable and clearly better than luck
(z-score >= 2 over at least 30 trades). On markets driven by a random number
generator (Volatility, Jump, Step indices ...) it is expected to fail - and then
Vera Bot stands aside instead of trading.
"""

import math
import pickle
import time
from pathlib import Path

import numpy as np
from numpy.lib.stride_tricks import sliding_window_view

WINDOW = 30            # ticks of history per decision
TEST_SHARE = 0.3       # newest 30% of ticks are the unseen test set
CALIB_SHARE = 0.1      # the 10% before that tune how much to trust the models
TEST_SPACING = 30      # test decisions every 30 ticks (about one per minute)
MIN_TEST_TRADES = 30
MIN_Z = 2.0
MODEL_DIR = Path(__file__).with_name("ai_models")


def digits_of(prices, decimals):
    return (np.rint(np.asarray(prices, dtype=float) * 10 ** decimals).astype(np.int64) % 10)


def features(prices, digits):
    """Feature rows for every decision point; row j uses ticks j .. j+WINDOW-1."""
    p = np.asarray(prices, dtype=float)
    w = sliding_window_view(p, WINDOW)
    d = sliding_window_view(np.asarray(digits), WINDOW)
    r = np.diff(w, axis=1) / w[:, :-1] * 1e4                  # returns in basis points
    std = w.std(axis=1) + 1e-12
    cols = [
        r[:, -10:],                                            # last 10 moves
        r.mean(axis=1, keepdims=True),
        r.std(axis=1, keepdims=True),
        ((w[:, -1] - w.mean(axis=1)) / std)[:, None],          # where the price sits in the window
        ((w[:, -1] - w[:, -10:].mean(axis=1)) / std)[:, None],  # short momentum
        (r > 0).mean(axis=1, keepdims=True),
        np.stack([(d == k).mean(axis=1) for k in range(10)], axis=1),   # digit frequencies
        np.eye(10)[d[:, -1]],                                  # last digit
        d[:, -3:].astype(float),                               # last 3 digits
    ]
    return np.hstack(cols)


def targets(prices, digits, horizon):
    """For decision row j (decision at tick t=j+WINDOW-1): entry = t+1, exit = entry+horizon."""
    p = np.asarray(prices, dtype=float)
    n_rows = len(p) - WINDOW + 1
    t = np.arange(n_rows) + WINDOW - 1
    exit_ = t + 1 + horizon
    ok = exit_ < len(p)
    rise = np.zeros(n_rows, dtype=int)
    dig = np.zeros(n_rows, dtype=int)
    rise[ok] = (p[exit_[ok]] > p[t[ok] + 1]).astype(int)
    dig[ok] = np.asarray(digits)[exit_[ok]]
    return rise, dig, ok


def candidates(allowed=None):
    """Contracts the AI can choose: (contract_type, barrier). `allowed` limits the types,
    e.g. ("DIGITEVEN", "DIGITODD") for a dedicated Even/Odd AI."""
    out = [("CALL", None), ("PUT", None), ("DIGITEVEN", None), ("DIGITODD", None)]
    out += [("DIGITDIFF", b) for b in range(10)] + [("DIGITMATCH", b) for b in range(10)]
    out += [("DIGITOVER", b) for b in range(9)] + [("DIGITUNDER", b) for b in range(1, 10)]
    if allowed:
        out = [(c, b) for c, b in out if c in allowed]
    return out


def win_prob(contract, barrier, p_rise, p_digits):
    if contract == "CALL":
        return p_rise
    if contract == "PUT":
        return 1 - p_rise
    if contract == "DIGITEVEN":
        return p_digits[0::2].sum()
    if contract == "DIGITODD":
        return p_digits[1::2].sum()
    if contract == "DIGITDIFF":
        return 1 - p_digits[barrier]
    if contract == "DIGITMATCH":
        return p_digits[barrier]
    if contract == "DIGITOVER":
        return p_digits[barrier + 1:].sum()
    return p_digits[:barrier].sum()          # DIGITUNDER


def outcome(contract, barrier, rise, digit):
    if contract == "CALL":
        return rise == 1
    if contract == "PUT":
        return rise == 0
    if contract == "DIGITEVEN":
        return digit % 2 == 0
    if contract == "DIGITODD":
        return digit % 2 == 1
    if contract == "DIGITDIFF":
        return digit != barrier
    if contract == "DIGITMATCH":
        return digit == barrier
    if contract == "DIGITOVER":
        return digit > barrier
    return digit < barrier


def label(contract, barrier):
    names = {"CALL": "RISE", "PUT": "FALL", "DIGITEVEN": "EVEN", "DIGITODD": "ODD",
             "DIGITDIFF": "DIFF", "DIGITMATCH": "MATCH", "DIGITOVER": "OVER", "DIGITUNDER": "UNDER"}
    return names[contract] + ("" if barrier is None else f" {barrier}")


class VeraAI:
    """Trained models for one market plus their walk-forward report."""

    def __init__(self, symbol, decimals, horizon, stake, payouts, allowed=()):
        self.symbol, self.decimals, self.horizon = symbol, decimals, horizon
        self.allowed = tuple(sorted(allowed))          # empty = every contract
        self.stake, self.payouts = stake, payouts      # {(contract, barrier): payout at `stake`}
        self.rise_model = self.digit_model = None
        self.base_rise, self.base_digits = 0.5, np.full(10, 0.1)   # plain frequencies
        self.trust_rise = self.trust_digit = 0.0   # 0 = ignore the model, 1 = trust it fully
        self.report, self.passed, self.trained_at = {}, False, 0.0

    # ------------------------------------------------------------------ train
    def fit(self, prices, min_edge=0.0):
        from sklearn.ensemble import HistGradientBoostingClassifier
        from sklearn.metrics import log_loss

        digits = digits_of(prices, self.decimals)
        X = features(prices, digits)
        rise, dig, ok = targets(prices, digits, self.horizon)
        X, rise, dig = X[ok], rise[ok], dig[ok]
        split = int(len(X) * (1 - TEST_SHARE))
        fit_end = int(len(X) * (1 - TEST_SHARE - CALIB_SHARE))
        if fit_end < 2000:
            raise ValueError(f"only {len(X)} samples - download more ticks")

        def model():
            return HistGradientBoostingClassifier(max_iter=150, learning_rate=0.05, max_leaf_nodes=15,
                                                  l2_regularization=1.0, random_state=1)
        self.rise_model = model().fit(X[:fit_end], rise[:fit_end])
        self.digit_model = model().fit(X[:fit_end], dig[:fit_end])
        self.base_rise = float(rise[:fit_end].mean())
        self.base_digits = np.bincount(dig[:fit_end], minlength=10) / fit_end

        # calibration: how far to move from plain frequencies towards the model,
        # chosen on ticks the models did not learn from (and before the test set)
        Xc, rc, dc = X[fit_end:split], rise[fit_end:split], dig[fit_end:split]
        raw_rc = self.rise_model.predict_proba(Xc)[:, 1]
        raw_dc = self._raw_digit_proba(Xc)
        grid = np.linspace(0, 1, 11)
        self.trust_rise = float(min(grid, key=lambda t: log_loss(
            rc, self.base_rise + t * (raw_rc - self.base_rise), labels=[0, 1])))
        self.trust_digit = float(min(grid, key=lambda t: log_loss(
            dc, self.base_digits + t * (raw_dc - self.base_digits), labels=list(range(10)))))

        Xt, rt, dt = X[split:], rise[split:], dig[split:]
        pr = self._rise_proba(Xt)
        pd_ = self._digit_proba(Xt)
        report = {
            "samples": len(X), "test_samples": len(Xt),
            "trust_rise": self.trust_rise, "trust_digit": self.trust_digit,
            "rise_accuracy": float(((pr > 0.5) == rt).mean()),
            "digit_accuracy": float((pd_.argmax(axis=1) == dt).mean()),
            # Even/Odd: predict "even" when the model gives even digits more than 50%
            "evenodd_accuracy": float(((pd_[:, 0::2].sum(axis=1) > 0.5) == (dt % 2 == 0)).mean()),
            # log-loss below chance level means the model learned something real
            "rise_logloss": float(log_loss(rt, pr, labels=[0, 1])), "rise_logloss_chance": math.log(2),
            "digit_logloss": float(log_loss(dt, pd_, labels=list(range(10)))),
            "digit_logloss_chance": math.log(10),
        }
        # out-of-sample trading simulation, one decision per TEST_SPACING ticks
        results, picks = [], {}
        for i in range(0, len(Xt), TEST_SPACING):
            best = self.best_contract(pr[i], pd_[i])
            if best is None or best[3] <= min_edge:
                continue
            contract, barrier, prob, ev = best
            won = outcome(contract, barrier, rt[i], dt[i])
            pay = self.payouts[(contract, barrier)]
            results.append(pay - self.stake if won else -self.stake)
            picks[label(contract, barrier)] = picks.get(label(contract, barrier), 0) + 1
        n = len(results)
        mean = float(np.mean(results)) if n else 0.0
        sd = float(np.std(results)) if n > 1 else 0.0
        z = mean / (sd / math.sqrt(n)) if n > 1 and sd > 0 else 0.0
        report.update(test_trades=n, test_pl=round(float(sum(results)), 2),
                      test_win_rate=float(np.mean([x > 0 for x in results])) if n else 0.0,
                      test_return_per_trade=mean / self.stake if n else 0.0, z=z, picks=picks)
        self.passed = n >= MIN_TEST_TRADES and mean > 0 and z >= MIN_Z
        self.report, self.trained_at = report, time.time()
        return self

    # ---------------------------------------------------------------- predict
    def _raw_digit_proba(self, X):
        raw = self.digit_model.predict_proba(X)
        full = np.zeros((len(X), 10))
        full[:, self.digit_model.classes_] = raw
        return full

    def _digit_proba(self, X):
        return self.base_digits + self.trust_digit * (self._raw_digit_proba(X) - self.base_digits)

    def _rise_proba(self, X):
        raw = self.rise_model.predict_proba(X)[:, 1]
        return self.base_rise + self.trust_rise * (raw - self.base_rise)

    def predict(self, prices):
        """Probabilities for the decision right after the last tick in `prices`."""
        p = np.asarray(prices, dtype=float)[-WINDOW:]
        if len(p) < WINDOW:
            return None
        X = features(p, digits_of(p, self.decimals))[-1:]
        return float(self._rise_proba(X)[0]), self._digit_proba(X)[0]

    def best_contract(self, p_rise, p_digits, payouts=None):
        """Highest predicted expected return: (contract, barrier, win_prob, expected_return)."""
        payouts = payouts or self.payouts
        best = None
        for (contract, barrier), pay in payouts.items():
            if not pay:
                continue
            prob = win_prob(contract, barrier, p_rise, p_digits)
            ev = prob * pay / self.stake - 1
            if best is None or ev > best[3]:
                best = (contract, barrier, float(prob), float(ev))
        return best

    # ---------------------------------------------------------------- storage
    def path(self):
        return MODEL_DIR / f"{self.symbol}.pkl"

    def save(self):
        MODEL_DIR.mkdir(exist_ok=True)
        with self.path().open("wb") as f:
            pickle.dump(self, f)

    @staticmethod
    def load(symbol):
        path = MODEL_DIR / f"{symbol}.pkl"
        if not path.exists():
            return None
        try:
            with path.open("rb") as f:
                return pickle.load(f)
        except Exception:  # noqa: BLE001 - corrupt / incompatible file: retrain
            return None

    def summary(self):
        r = self.report
        verdict = "PASSED - may trade" if self.passed else "NO PROVEN EDGE - stands aside"
        only = f" [{'/'.join(label(c, None) for c in self.allowed)} only]" if getattr(self, "allowed", ()) else ""
        return (f"AI {self.symbol}{only}: {verdict} | unseen test: {r.get('test_trades', 0)} trades, "
                f"win {r.get('test_win_rate', 0):.0%}, P/L {r.get('test_pl', 0):+.2f}, "
                f"{r.get('test_return_per_trade', 0):+.1%}/trade, z={r.get('z', 0):.1f} | "
                f"trust in model: direction {self.trust_rise:.0%}, digits {self.trust_digit:.0%} | "
                f"even/odd acc {r.get('evenodd_accuracy', 0):.1%} (chance 50%), "
                f"digit acc {r.get('digit_accuracy', 0):.1%} (chance 10%), "
                f"rise acc {r.get('rise_accuracy', 0):.1%} (chance 50%) | "
                f"log-loss digit {r.get('digit_logloss', 0):.3f} vs {r.get('digit_logloss_chance', 0):.3f}, "
                f"rise {r.get('rise_logloss', 0):.3f} vs {r.get('rise_logloss_chance', 0):.3f}")
