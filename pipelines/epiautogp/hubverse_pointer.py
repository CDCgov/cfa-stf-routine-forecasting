"""
Validation for Hubverse handoff pointer JSON artifacts.

A handoff pointer is a JSON file from the `stf-routine-nowcasting-nhsn` pipeline.
It asserts that a model output passed validation, and other information about the run,
and points at the parquet that holds the probabilistic nowcasts (and other metadata about 
what was produced).
"""

from __future__ import annotations

import json
from pathlib import Path

from pipelines.epiautogp.forecast_spec import ForecastSpec

def _expect(
    data: dict,
    field: str,
    expected: object,
    *,
    where: str,
    display_as: str | None = None,
) -> None:
    """
    Raise if data[field] does not equal expected.
    TODO: This is a bit of a hacky way to validate the pointer contents. Move to pydantic somehow?
    """
    actual = data.get(field)
    if actual != expected:
        raise ValueError(
            f"{where} {display_as or field} mismatch: "
            f"expected {expected!r}, got {actual!r}."
        )


def load_hubverse_model_output_path(
    pointer_path: str | Path,
    *,
    forecast_spec: ForecastSpec,
    expected_pointer_disease: str | None,
) -> Path:
    """
    Validate a Hubverse handoff pointer and return its model-output parquet path.

    Checks that the pointer's report date and disease match the forecast run
    and that the upstream producer marked the output as validated. The
    pointer's `model_output_uri` may be absolute or relative to the pointer
    file.
    """
    pointer_path = Path(pointer_path).absolute()
    try:
        pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Hubverse pointer is not valid JSON: {pointer_path}") from exc
    if not isinstance(pointer, dict):
        raise ValueError("Hubverse pointer JSON must be an object.")

    _expect(pointer, "artifact_type", "handoff_pointer", where="pointer")
    _expect(
        pointer,
        "report_date",
        forecast_spec.report_date.isoformat(),
        where="pointer",
    )
    if expected_pointer_disease is not None:
        _expect(
            pointer,
            "target",
            expected_pointer_disease,
            where="pointer",
            display_as="target (disease)",
        )

    hubverse = pointer.get("hubverse")
    if not isinstance(hubverse, dict):
        raise ValueError("Hubverse pointer is missing a hubverse object.")

    _expect(hubverse, "validation_status", "passed", where="Hubverse output")
    if "round_id" in hubverse:
        _expect(
            hubverse,
            "round_id",
            forecast_spec.report_date.isoformat(),
            where="Hubverse",
        )

    model_output_uri = hubverse.get("model_output_uri")
    if not isinstance(model_output_uri, str) or not model_output_uri:
        raise ValueError("Hubverse pointer hubverse.model_output_uri is required.")

    model_output_path = Path(model_output_uri)
    if not model_output_path.is_absolute():
        model_output_path = pointer_path.parent / model_output_path
    return model_output_path.resolve()
