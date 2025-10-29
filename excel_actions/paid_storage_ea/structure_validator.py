"""
Structure validation for Paid Storage report.

We validate minimal required fields used downstream:
- date: string (RFC3339 date acceptable, stored as DATE later)
- nmId: integer
- vendorCode: string
- warehousePrice: number

Other fields are optional for our aggregation (giId is collected to array if present).
"""

from __future__ import annotations

from typing import Any, Dict, List, Tuple


class ValidationError(Exception):
    pass


def validate_paid_storage_response(data: Any) -> None:
    if not isinstance(data, list):
        raise ValidationError("Response must be an array")

    # Validate a few sample rows (all would be fine but costly on big reports)
    sample = data[: min(5, len(data))]
    for idx, row in enumerate(sample):
        _validate_row(row, idx)


def _validate_row(row: Dict[str, Any], idx: int) -> None:
    if not isinstance(row, dict):
        raise ValidationError(f"Row[{idx}] must be an object")

    if "date" not in row or not isinstance(row["date"], str):
        raise ValidationError(f"Row[{idx}] missing or invalid 'date'")

    if "nmId" not in row or not isinstance(row["nmId"], int):
        raise ValidationError(f"Row[{idx}] missing or invalid 'nmId'")

    if "vendorCode" not in row or not isinstance(row["vendorCode"], str):
        raise ValidationError(f"Row[{idx}] missing or invalid 'vendorCode'")

    if "warehousePrice" not in row or not isinstance(row["warehousePrice"], (int, float)):
        raise ValidationError(f"Row[{idx}] missing or invalid 'warehousePrice'")


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


