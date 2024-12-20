import re
from typing import Annotated

from pydantic import constr, BeforeValidator

# Create a proper custom type using Pydantic's Annotated type
ShortIso8601Date = Annotated[
    str,
    BeforeValidator(lambda x: x if isinstance(x, str) and bool(re.match(r'^\d{4}-\d{2}-\d{2}$', x)) else None),
    constr(pattern=r'^\d{4}-\d{2}-\d{2}$')
]
