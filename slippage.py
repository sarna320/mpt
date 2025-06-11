def alpha_to_tao_with_slippage_row(
    alpha_amount: float,
    alpha_in: float,
    tao_in: float,
    price: float,
    is_dynamic: bool = True,
    percentage: bool = False,
) -> tuple[float, float]:
    if not is_dynamic:
        tao_out = alpha_amount * price
        return tao_out, 0.0
    k = tao_in * alpha_in
    if k == 0 or (alpha_in + alpha_amount) == 0:
        tao_out = alpha_amount * price
        return tao_out, 0.0
    new_alpha_in = alpha_in + alpha_amount
    new_tao_reserve = k / new_alpha_in
    tao_returned = tao_in - new_tao_reserve
    tao_ideal = alpha_amount * price
    slippage = tao_ideal - tao_returned
    if slippage < 0:
        slippage = 0.0
    return tao_returned, slippage


def tao_to_alpha_with_slippage_row(
    tao_amount: float,
    alpha_in: float,
    tao_in: float,
    price: float,
    is_dynamic: bool = True,
    percentage: bool = False,
) -> tuple[float, float]:
    if not is_dynamic:
        alpha_out = tao_amount / price if price != 0 else 0.0
        return alpha_out, 0.0
    k = tao_in * alpha_in
    if k == 0 or (tao_in + tao_amount) == 0:
        alpha_out = tao_amount / price if price != 0 else 0.0
        return alpha_out, 0.0
    new_tao_in = tao_in + tao_amount
    new_alpha_in = k / new_tao_in
    alpha_returned = alpha_in - new_alpha_in
    alpha_ideal = tao_amount / price if price != 0 else 0.0
    slippage = alpha_ideal - alpha_returned
    if slippage < 0:
        slippage = 0.0
    return alpha_returned, slippage
