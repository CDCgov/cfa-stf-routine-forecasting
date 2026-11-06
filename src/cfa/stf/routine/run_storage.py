"""Execution-scoped forecast storage and completed-fit discovery."""

import datetime as dt
import json
import re
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
    _atomic_write(directory / "launchpad.yaml", format_yaml(config).encode("utf-8"))
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


def latest_fits(base: Path) -> dict[FitKey, Path]:
    """Snapshot the latest complete fit for each location/disease/model."""
    selected = {}
    executions = []
    for directory in base.iterdir():
        if not directory.is_dir() or not RUN_PATTERN.fullmatch(directory.name):
            continue
        metadata = directory / "execution.json"
        if metadata.is_file():
            identity = json.loads(metadata.read_text())
            executions.append(
                (identity["started_at"], identity["execution_id"], directory)
            )
    for _, _, directory in sorted(executions):
        for marker in sorted(directory.glob("*/*/*/complete.json")):
            fit = marker.parent
            selected[FitKey(fit.parent.parent.name, fit.parent.name, fit.name)] = fit
    return selected
