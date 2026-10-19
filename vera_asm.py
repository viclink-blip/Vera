"""
Python bridge to the x86-64 assembly maths in vera_asm.S (libvera_asm.so).

If the library isn't built (or the CPU isn't x86-64), `available()` is False and
Vera Bot uses its Python/pandas maths instead. Run this file to self-test the
assembly against pandas:  python3 vera_asm.py
"""

import ctypes
import math
from pathlib import Path

import numpy as np

LIB_PATH = Path(__file__).with_name("libvera_asm.so")
_lib = None


def _load():
    global _lib
    if _lib is None and LIB_PATH.exists():
        try:
            lib = ctypes.CDLL(str(LIB_PATH))
            dptr, lptr = ctypes.POINTER(ctypes.c_double), ctypes.POINTER(ctypes.c_long)
            lib.vera_sma.argtypes = [dptr, ctypes.c_long, ctypes.c_long]
            lib.vera_sma.restype = ctypes.c_double
            lib.vera_rsi.argtypes = [dptr, ctypes.c_long, ctypes.c_long]
            lib.vera_rsi.restype = ctypes.c_double
            lib.vera_digit_hist.argtypes = [lptr, ctypes.c_long, lptr]
            lib.vera_digit_hist.restype = None
            _lib = lib
        except OSError:
            _lib = False
    return _lib or None


def available():
    return _load() is not None


def _doubles(values):
    arr = np.ascontiguousarray(values, dtype=np.float64)
    return arr, arr.ctypes.data_as(ctypes.POINTER(ctypes.c_double))


def sma(values, period):
    arr, ptr = _doubles(values)
    return _load().vera_sma(ptr, len(arr), int(period))


def rsi(closes, period):
    arr, ptr = _doubles(closes)
    return _load().vera_rsi(ptr, len(arr), int(period))


def digit_hist(digits):
    arr = np.ascontiguousarray(digits, dtype=np.int64)
    out = np.zeros(10, dtype=np.int64)
    _load().vera_digit_hist(arr.ctypes.data_as(ctypes.POINTER(ctypes.c_long)), len(arr),
                            out.ctypes.data_as(ctypes.POINTER(ctypes.c_long)))
    return out


def self_test():
    import pandas as pd
    if not available():
        print(f"{LIB_PATH.name} not built - run ./build_asm.sh")
        return False
    rng = np.random.default_rng(1)
    ok = True
    for n in (2, 15, 100, 300):
        c = 1000 + np.cumsum(rng.normal(0, 1, n))
        s = pd.Series(c)
        for period in (7, 14):
            want = s.rolling(period).mean().iloc[-1]
            got = sma(c, period)
            ok &= (math.isnan(want) and math.isnan(got)) or abs(got - want) < 1e-9
        d = s.diff()
        g = d.clip(lower=0).ewm(alpha=1 / 14, adjust=False).mean()
        l = (-d.clip(upper=0)).ewm(alpha=1 / 14, adjust=False).mean()
        want = (100 - 100 / (1 + g / l)).fillna(100.0).iloc[-1]
        ok &= abs(rsi(c, 14) - want) < 1e-9
    up = np.arange(30, dtype=float)                     # only gains -> RSI 100
    ok &= rsi(up, 14) == 100.0
    digits = rng.integers(0, 10, 5000)
    ok &= bool((digit_hist(digits) == np.bincount(digits, minlength=10)).all())
    print("assembly self-test:", "PASSED - results match pandas" if ok else "FAILED")
    return bool(ok)


if __name__ == "__main__":
    raise SystemExit(0 if self_test() else 1)
