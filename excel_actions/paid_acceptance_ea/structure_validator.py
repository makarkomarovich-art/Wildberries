from __future__ import annotations

from typing import Any, Dict, List


class ValidationError(Exception):
    pass


def validate_acceptance_response(data: Any) -> None:
    if not isinstance(data, list):
        raise ValidationError("Response must be an array at root")

    sample = data[: min(10, len(data))]
    for idx, row in enumerate(sample):
        _validate_row(row, idx)


def _validate_row(row: Dict[str, Any], idx: int) -> None:
    if not isinstance(row, dict):
        raise ValidationError(f"Row[{idx}] must be an object")

    if "shkCreateDate" not in row or not isinstance(row["shkCreateDate"], str):
        raise ValidationError(f"Row[{idx}] missing or invalid 'shkCreateDate'")

    # API field name is nmID as per spec
    if "nmID" not in row or not isinstance(row["nmID"], int):
        raise ValidationError(f"Row[{idx}] missing or invalid 'nmID'")

    if "count" not in row or not isinstance(row["count"], int):
        raise ValidationError(f"Row[{idx}] missing or invalid 'count'")

    if "total" not in row or not isinstance(row["total"], (int, float)):
        raise ValidationError(f"Row[{idx}] missing or invalid 'total'")


def get_validation_report(data: Any) -> Dict[str, Any]:
    errors: List[str] = []
    if not isinstance(data, list):
        return {"valid": False, "errors": ["Response must be an array"], "error_count": 1}

    for idx, row in enumerate(data[: min(20, len(data))]):
        try:
            _validate_row(row, idx)
        except ValidationError as e:
            errors.append(str(e))

    return {"valid": len(errors) == 0, "errors": errors, "error_count": len(errors)}


