from typing import Tuple
import math

# max number of significant digits for a 64-bit float
_MAX_SIGNIFICANT_DIGITS_AFTER_DOT = 15

_MIN_EXP = -323
_MAX_EXP = 308


def sround(value: float, ndigits: int = 0, int_part=False) -> float:
    """
    Round *value* to significant number of digits *ndigits*.

    :param value: The value to round.
    :param ndigits: The number of digits after the first significant digit.
    :param int_part:
    :return:
    """
    ndigits_extra = _ndigits_extra(value, int_part=int_part)
    ndigits += ndigits_extra
    return round(value, ndigits=ndigits)


def sround_range(range_value: Tuple[float, float], ndigits: int = 0, int_part=False) -> Tuple[float, float]:
    value_1, value_2 = range_value
    ndigits_extra_1 = _ndigits_extra(value_1, int_part=int_part)
    ndigits_extra_2 = _ndigits_extra(value_2, int_part=int_part)
    ndigits += min(ndigits_extra_1, ndigits_extra_2)
    return round(value_1, ndigits=ndigits), round(value_2, ndigits=ndigits)


def _ndigits_extra(value: float, int_part: bool) -> int:
    ndigits = -int(math.floor(_limited_log10(value)))
    if ndigits < 0 and not int_part:
        return 0
    return ndigits


def _limited_log10(value: float) -> float:
    if value > 0.0:
        exp = math.log10(value)
    elif value < 0.0:
        exp = math.log10(-value)
    else:
        return _MIN_EXP

    if exp < _MIN_EXP:
        return _MIN_EXP
    if exp > _MAX_EXP:
        return _MAX_EXP

    return exp
