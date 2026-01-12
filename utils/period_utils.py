def add_period_strings(base_period: str, offset_period: str) -> str:
    """
    Cộng 2 chuỗi thời gian lại với nhau.

    Args:
        base_period: Chuỗi dạng "M2907" (tháng 7 năm 2029)
        offset_period: Chuỗi dạng "MP04" (plus 4 tháng)

    Returns:
        Chuỗi kết quả dạng "M2911" (tháng 11 năm 2029)

    Example:
        >>> add_period_strings("M2907", "MP04")
        "M2911"
        >>> add_period_strings("M2512", "MP01")
        "M2601"
    """
    if not base_period or len(base_period) < 5 or base_period[0] != 'M':
        raise ValueError(f"Invalid base_period format: {base_period}. Expected format: M2907")

    year_str = base_period[1:3]
    month_str = base_period[3:5]
    base_year = 2000 + int(year_str)
    base_month = int(month_str)

    if not offset_period or len(offset_period) < 4 or not offset_period.startswith('MP'):
        raise ValueError(f"Invalid offset_period format: {offset_period}. Expected format: MP04")

    offset_months = int(offset_period[2:])

    total_months = base_month + offset_months
    new_year = base_year + (total_months - 1) // 12
    new_month = ((total_months - 1) % 12) + 1

    year_suffix = str(new_year)[-2:]
    result = f"M{year_suffix}{new_month:02d}"

    return result


def add_period_with_offset(base_period: str, offset: int) -> str:
    """
    Tính toán period mới dựa trên base_period và offset (có thể dương hoặc âm).

    Args:
        base_period: Chuỗi dạng "M2501" (tháng 1 năm 2025)
        offset: Số tháng cần cộng/trừ (dương = tịnh tiến, âm = lùi)

    Returns:
        Chuỗi kết quả dạng "M2502" hoặc "M2412"

    Example:
        >>> add_period_with_offset("M2501", 1)
        "M2502"
        >>> add_period_with_offset("M2501", -1)
        "M2412"
        >>> add_period_with_offset("M2512", 1)
        "M2601"
    """
    if not base_period or len(base_period) < 5 or base_period[0] != 'M':
        raise ValueError(f"Invalid base_period format: {base_period}. Expected format: M2501")

    year_str = base_period[1:3]
    month_str = base_period[3:5]
    base_year = 2000 + int(year_str)
    base_month = int(month_str)

    total_months = (base_year * 12 + base_month) + offset
    new_year = (total_months - 1) // 12
    new_month = ((total_months - 1) % 12) + 1

    year_suffix = str(new_year)[-2:]
    result = f"M{year_suffix}{new_month:02d}"

    return result
