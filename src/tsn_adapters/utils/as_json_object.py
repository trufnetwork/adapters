from typing import Dict, Any


def as_json_object(template: str) -> Dict[str, Any]:
    """
    Prefect block to convert a Jinja template to a JSON object. See https://github.com/PrefectHQ/prefect/pull/15132
    """
    return {
        "__prefect_kind": "json",
        "value": {
            "__prefect_kind": "jinja",
            "template": template,
        },
    }
