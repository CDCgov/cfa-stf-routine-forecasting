"""
Validation for Hubverse handoff pointer JSON artifacts.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pipelines.epiautogp.forecast_spec import ForecastSpec


def resolve_artifact_path(
    raw_path: str | Path,
    *,
    base_path: Path | None = None,
    source_label: str,
) -> Path:
    """Resolve a local file path (absolute or relative) to a concrete path."""
    path = Path(raw_path)
    if path.is_absolute():
        return path.resolve()
    if base_path is not None:
        return (base_path.parent / path).resolve()
    return (Path.cwd() / path).resolve()


def _load_pointer(
    pointer_uri: str | Path,
    *,
    source_label: str,
) -> tuple[dict[str, Any], Path]:
    """Read a handoff pointer JSON and return it with its resolved path."""
    resolved = resolve_artifact_path(pointer_uri, source_label=source_label)
    try:
        pointer = json.loads(resolved.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"{source_label} pointer is not valid JSON: {pointer_uri}"
        ) from exc
    if not isinstance(pointer, dict):
        raise ValueError(f"{source_label} pointer JSON must be an object.")
    return pointer, resolved


def _check_fields(
    data: dict[str, Any],
    *,
    expected: dict[str, object],
    label: str,
    source_label: str,
) -> None:
    """Raise if any expected field value is absent or different."""
    for field, expected_value in expected.items():
        actual_value = data.get(field)
        if actual_value != expected_value:
            field_label = (
                "target (disease)"
                if label == "pointer" and field == "target"
                else field
            )
            raise ValueError(
                f"{source_label} {label} {field_label} mismatch: "
                f"expected {expected_value!r}, got {actual_value!r}."
            )


def _expected_pointer_fields(
    *,
    forecast_spec: ForecastSpec,
    expected_pointer_disease: str | None,
) -> dict[str, object]:
    """Build top-level pointer fields expected for a forecast spec."""
    expected: dict[str, object] = {
        "artifact_type": "handoff_pointer",
        "report_date": forecast_spec.report_date.isoformat(),
    }
    # The pointer JSON key is `target`, but for these Hubverse producers that
    # value is the disease slug derived from ForecastSpec.disease.
    if expected_pointer_disease is not None:
        expected["target"] = expected_pointer_disease
    return expected


def _validate_pointer(
    pointer: dict[str, Any],
    *,
    expected_pointer_disease: str | None,
    forecast_spec: ForecastSpec,
    source_label: str,
) -> str:
    """Validate pointer metadata and return hubverse.model_output_uri."""
    _check_fields(
        pointer,
        expected=_expected_pointer_fields(
            forecast_spec=forecast_spec,
            expected_pointer_disease=expected_pointer_disease,
        ),
        label="pointer",
        source_label=source_label,
    )

    hubverse = pointer.get("hubverse")
    if not isinstance(hubverse, dict):
        raise ValueError(f"{source_label} pointer is missing a hubverse object.")

    _check_fields(
        hubverse,
        expected={"validation_status": "passed"},
        label="Hubverse output",
        source_label=source_label,
    )
    if "round_id" in hubverse:
        _check_fields(
            hubverse,
            expected={"round_id": forecast_spec.report_date.isoformat()},
            label="Hubverse",
            source_label=source_label,
        )

    model_output_uri = hubverse.get("model_output_uri")
    if not isinstance(model_output_uri, str) or not model_output_uri:
        raise ValueError(
            f"{source_label} pointer hubverse.model_output_uri is required."
        )

    return model_output_uri


def load_hubverse_model_output_path(
    pointer_uri: str | Path,
    *,
    expected_pointer_disease: str | None,
    forecast_spec: ForecastSpec,
    source_label: str,
) -> Path:
    """Load a pointer and resolve its Hubverse model-output artifact path."""
    pointer, resolved_pointer_uri = _load_pointer(
        pointer_uri,
        source_label=source_label,
    )
    model_output_uri = _validate_pointer(
        pointer,
        expected_pointer_disease=expected_pointer_disease,
        forecast_spec=forecast_spec,
        source_label=source_label,
    )
    return resolve_artifact_path(
        model_output_uri,
        base_path=resolved_pointer_uri,
        source_label=source_label,
    )
