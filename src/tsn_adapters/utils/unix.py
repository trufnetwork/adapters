def check_unix_timestamp(timestamp: int) -> bool:
    # Define valid range for seconds since epoch
    min_valid_timestamp = 0  # 1970-01-01
    max_valid_timestamp = 4102444800  # 2100-01-01

    if timestamp < min_valid_timestamp or timestamp > max_valid_timestamp:
        raise ValueError(f"Timestamp {timestamp} is out of range")

    return True
