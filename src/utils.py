def is_valid_date_format(date_string: str) -> bool:
    return len(date_string) == 8 and date_string.isdigit()
