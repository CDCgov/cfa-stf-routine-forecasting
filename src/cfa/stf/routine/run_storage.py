"""Execution-scoped forecast storage and completed-fit discovery."""

import datetime as dt
import re
from collections.abc import Iterable
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import NamedTuple
from uuid import uuid4
from zoneinfo import ZoneInfo

from pydantic_core import to_json
from yaml12 import format_yaml

RUN_PATTERN = re.compile(r"\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2}_[\w-]+")
NEW_YORK = ZoneInfo("America/New_York")


class FitKey(NamedTuple):
    """Identity of a fit independent of its execution."""

    location: str
    disease: str
    model: str

    def directory(self, execution: Path) -> Path:
        return execution / self.location / self.disease / self.model


def _atomic_write(path: Path, content: bytes) -> None:
    """Stage a file on the same filesystem and clean up after publication or failure."""
    with TemporaryDirectory(prefix=f".{path.name}.", dir=path.parent) as staging:
        temporary = Path(staging) / path.name
        temporary.write_bytes(content)
        temporary.replace(path)


def write_json(path: Path, value: dict) -> None:
    """Serialize metadata and atomically replace the destination file."""
    _atomic_write(path, to_json(value, indent=2))


def execution_directory(
    base: Path, started: dt.datetime, execution_id: str, config: dict
) -> Path:
    """Create the deterministic directory shared by workers of one execution."""
    started = started.astimezone(NEW_YORK)
    name = started.strftime("%Y-%m-%d-%H-%M-%S")
    directory = base / f"{name}_{execution_id}"
    directory.mkdir(parents=True, exist_ok=True)
    write_json(
        directory / "execution.json",
        {
            "execution_id": execution_id,
            "started_at": started.isoformat(),
        },
    )
    _atomic_write(directory / "run_config.yaml", format_yaml(config).encode("utf-8"))
    return directory


def dagster_execution_directory(context, base: Path) -> Path:
    record = context.instance.get_run_record_by_id(context.run_id)
    if record is None or record.start_time is None:
        raise RuntimeError("Dagster execution has no persisted start time")
    return execution_directory(
        base,
        dt.datetime.fromtimestamp(record.start_time, dt.UTC),
        context.run_id,
        context.run.run_config,
    )


def standalone_directory(base: Path, config: dict) -> Path:
    if (base / "execution.json").is_file():
        return base
    return execution_directory(base, dt.datetime.now(dt.UTC), uuid4().hex, config)


def latest_fits(base: Path, keys: Iterable[FitKey] | None = None) -> dict[FitKey, Path]:
    """Select the latest complete fits by execution directory name.

    If keys are supplied, inspect only those fit paths and stop once all are found.
    Missing fits are omitted. Without keys, discover all completed fits.
    """
    requested = None if keys is None else set(keys)
    selected = {}
    for directory in sorted(base.iterdir(), reverse=True):
        if requested is not None and len(selected) == len(requested):
            break
        if not directory.is_dir() or not RUN_PATTERN.fullmatch(directory.name):
            continue
        markers = (
            directory.glob("*/*/*/_COMPLETE")
            if requested is None
            else (
                key.directory(directory) / "_COMPLETE"
                for key in requested - selected.keys()
            )
        )
        for marker in markers:
            if not marker.is_file():
                continue
            fit = marker.parent
            key = FitKey(fit.parent.parent.name, fit.parent.name, fit.name)
            selected.setdefault(key, fit)
    return selected
