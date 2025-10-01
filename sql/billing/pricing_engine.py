def apply_tiers(amount, tiers):
    """
    Calculate cost based on tiered pricing.

    Args:
        amount (float): Quantity or amount to price.
        tiers (list of dict): List of tiers, each dict has keys:
            - 'upto' (float or None): Maximum quantity for this tier.
            - 'unit_price' (float): Price per unit for this tier.

    Returns:
        float: Total cost rounded to 6 decimals.
    """
    remaining = amount
    cost = 0.0
    prev = 0

    for t in tiers:
        upto = t.get('upto')
        price = t['unit_price']

        # Determine quantity for this tier
        if upto is None:
            qty = remaining
        else:
            qty = max(0, min(remaining, upto - prev))

        # Accumulate cost
        cost += qty * price
        prev = upto if upto else prev
        remaining -= qty

        if remaining <= 0:
            break

    return round(cost, 6)


def apply_discount(subtotal, percent, flat):
    """
    Apply percentage and flat discounts to subtotal.

    Args:
        subtotal (float): Original subtotal amount.
        percent (float): Percentage discount to apply.
        flat (float): Flat discount to apply.

    Returns:
        float: Discounted total, never less than 0, rounded to 6 decimals.
    """
    discounted_total = subtotal * (1 - percent / 100.0) - flat
    return round(max(0, discounted_total), 6)
