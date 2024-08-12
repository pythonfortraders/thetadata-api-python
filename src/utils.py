def is_valid_date_format(date_string: str) -> bool:
    return len(date_string) == 8 and date_string.isdigit()


def is_valid_right(right: str) -> bool:
    """Check if option type is valid('C' for call or 'P' for put)."""
    return right in ["C", "P"]


def is_valid_ivl(ivl: int) -> bool:
    return 100 <= ivl <= 3600000
