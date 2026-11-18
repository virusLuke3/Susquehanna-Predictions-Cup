"""Original PRC poll/price gates; no Polymarket fee or order assumptions."""

from __future__ import annotations

from decimal import Decimal

D = Decimal


def dec(value: str | int | Decimal) -> Decimal:
    if isinstance(value, (float, bool)):
        raise TypeError("Use Decimal, integer, or decimal string; not float/bool.")
    x = D(value)
    if not x.is_finite():
        raise ValueError("Non-finite value.")
    return x


def price(value: str | int | Decimal) -> Decimal:
    x = dec(value)
    if not 0 <= x <= 1:
        raise ValueError("Price must be in [0,1].")
    return x


def nonnegative(value: str | int | Decimal) -> Decimal:
    x = dec(value)
    if x < 0:
        raise ValueError("Negative value.")
    return x


def paired_poll_z(
    y_old: Decimal,
    y_new: Decimal,
    var_old: Decimal,
    var_new: Decimal,
    *,
    comparability_verified: bool,
    independent_samples_verified: bool,
) -> Decimal:
    """Contrast variances, not single-option variances or reported MOEs.
    This measures a survey contrast change; never estimates election probabilities.
    Constant house-effect cancellation is an assumption, not guaranteed by flags.
    """
    a, b, va, vb = map(dec, (y_old, y_new, var_old, var_new))
    if not comparability_verified or not independent_samples_verified:
        raise ValueError("Comparability/independence has not been verified.")
    if not (-1 <= a <= 1 and -1 <= b <= 1) or va <= 0 or vb <= 0:
        raise ValueError("Invalid contrast or variance.")
    return (b - a) / (va + vb).sqrt()


def poll_gate(
    y_old: Decimal, y_new: Decimal, var_old: Decimal, var_new: Decimal, **kwargs: bool
) -> bool:
    z = paired_poll_z(y_old, y_new, var_old, var_new, **kwargs)
    return abs(dec(y_new) - dec(y_old)) >= D("0.03") and abs(z) >= 2


def price_gate(
    m_pre: Decimal, m5: Decimal, m10: Decimal, m15: Decimal, bid: Decimal, ask: Decimal
) -> bool:
    """Run ONCE per release at t0+15min. All times/metadata verified upstream.
    Apply to actual SIG YES and NO sides separately; both passing => reject.
    """
    p0, p5, p10, p15, b, a = map(price, (m_pre, m5, m10, m15, bid, ask))
    if a < b:
        raise ValueError("Crossed/inconsistent book.")
    return (
        D("0.01") <= p5 - p0 <= D("0.04")
        and p5 <= p10 <= p15
        and p15 - p0 <= D("0.06")
        and D("0.20") <= a <= D("0.80")
        and a - b <= D("0.01")
    )
