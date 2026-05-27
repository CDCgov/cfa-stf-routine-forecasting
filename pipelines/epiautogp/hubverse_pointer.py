"""
Validation for Hubverse handoff pointer JSON artifacts.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

from pipelines.epiautogp.forecast_spec import ForecastSpec


def _repo_root() -> Path:
    """Return the repository root for blobfuse mount discovery."""
    return Path(__file__).resolve().parents[2]


def _blobfuse_candidate_paths(*, container: str, blob_path: str) -> list[Path]:
    """List local paths where a mounted Blob artifact may exist."""
    roots = [
        Path.cwd(),
        _repo_root(),
        Path.cwd() / "blobfuse" / "mounts",
        _repo_root() / "blobfuse" / "mounts",
        Path("/mnt"),
    ]
    candidates: list[Path] = []
    for root in roots:
        candidate = root / container / blob_path
        if candidate not in candidates:
            candidates.append(candidate)
    return candidates


def _blobfuse_path(raw_uri: str, *, source_label: str) -> Path:
    """Resolve an az:// URI to an existing blobfuse-mounted path."""
    rest = raw_uri.removeprefix("az://")
    container, separator, blob_path = rest.partition("/")
    if not container or not separator or not blob_path.strip("/"):
        raise ValueError(f"Unsupported az:// {source_label} URI: {raw_uri}")

    candidates = _blobfuse_candidate_paths(
        container=container,
        blob_path=blob_path.strip("/"),
    )
    for candidate in candidates:
        if candidate.exists():
            return candidate

    formatted_candidates = "\n".join(f"  - {path}" for path in candidates)
    raise FileNotFoundError(
        f"{source_label} az:// artifact is not available through known "
        "mounted Blob paths. Mount blob storage with `make mount` or provide "
        "a local/file:// URI.\n"
        f"az URI: {raw_uri}\n"
        f"Checked:\n{formatted_candidates}"
    )


def resolve_artifact_path(
    raw_uri: str | Path,
    *,
    base_path: Path | None = None,
    source_label: str,
) -> Path:
    """Resolve local, file://, and blobfuse-backed az:// URIs to a local path."""
    raw = str(raw_uri)
    if raw.startswith("az://"):
        return _blobfuse_path(raw, source_label=source_label)

    parsed = urlparse(raw)
    if parsed.scheme == "file":
        return Path(unquote(parsed.path)).resolve()
    if parsed.scheme:
        raise ValueError(f"Unsupported {source_label} URI scheme: {raw}")

    path = Path(raw)
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
