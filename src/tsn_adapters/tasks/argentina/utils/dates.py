from datetime import datetime

SPANISH_WEEKDAY_MAP: dict[str, str] = {
    "Monday": "Lunes",
    "Tuesday": "Martes",
    "Wednesday": "Miercoles",
    "Thursday": "Jueves",
    "Friday": "Viernes",
    "Saturday": "Sabado",
    "Sunday": "Domingo",
}


def date_to_weekday(date_str: str) -> str:
    """Converts a date string (YYYY-MM-DD) to its Spanish weekday name."""
    en_value = datetime.strptime(date_str, "%Y-%m-%d").strftime("%A")
    try:
        return SPANISH_WEEKDAY_MAP[en_value]
    except KeyError:
        raise ValueError(f"Invalid weekday: {en_value}") from KeyError
