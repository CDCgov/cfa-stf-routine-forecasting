"""Execution-scoped forecast storage and completed-fit discovery."""

import datetime as dt
import json
import re
import time
from pathlib import Path
from typing import NamedTuple
from uuid import uuid4

from yaml12 import format_yaml

RUN_PATTERN = re.compile(r"\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2}(?:_[\w-]+)?")


class FitKey(NamedTuple):
    """Identity of a fit independent of its execution."""

    location: str
    disease: str
    model: str

    def directory(self, execution: Path) -> Path:
        return execution / self.location / self.disease / self.model


def _json_value(value):
    if isinstance(value, (dt.date, dt.datetime)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"Unsupported metadata value: {type(value).__name__}")


def write_json(path: Path, value: dict) -> None:
    temporary = path.with_name(f".{path.name}.{uuid4().hex}")
    temporary.write_text(json.dumps(value, default=_json_value, indent=2))
    temporary.replace(path)


def execution_directory(
    base: Path, started: dt.datetime, execution_id: str, config: dict
) -> Path:
    """Claim a timestamp directory, sharing it only with the same execution."""
    base.mkdir(parents=True, exist_ok=True)
    name = started.astimezone(dt.UTC).strftime("%Y-%m-%d-%H-%M-%S")
    # Exclusive owner creation is safe across mapped workers.
    for candidate in (name, f"{name}_{execution_id}"):
        directory = base / candidate
        directory.mkdir(exist_ok=True)
        owner = directory / "execution_id"
        try:
            with owner.open("x") as stream:
                stream.write(execution_id)
        except FileExistsError:
            for _ in range(100):
                existing = owner.read_text()
                if existing:
                    break
                time.sleep(0.01)
            if not existing:
                raise RuntimeError("Execution owner claim was not completed")
            if existing != execution_id:
                continue
        write_json(
            directory / "execution.json",
            {
                "execution_id": execution_id,
                "started_at": started.astimezone(dt.UTC).isoformat(),
            },
        )
        temporary = directory / f".launchpad.{uuid4().hex}.yaml"
        temporary.write_text(format_yaml(config))
        temporary.replace(directory / "launchpad.yaml")
        return directory
    raise RuntimeError("Execution directory is owned by another run")


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
