"""Edit shared Dagster task config values from clipboard YAML."""

from __future__ import annotations

import argparse
import shutil
import subprocess
from typing import Any

from rich.console import Console
from rich.panel import Panel
from rich.prompt import IntPrompt, Prompt
from rich.table import Table
from yaml12 import format_yaml, parse_yaml

console = Console()

SHARED_CONFIG_KEYS = ("diseases", "exclude_last_n_days", "locations", "n_training_days")
DISEASE_KEYS = ("diseases", "postprocess_diseases")


def read_clipboard() -> str:
    """Read text from the system clipboard."""
    clipboard_commands = (
        ["pbpaste"],
        ["wl-paste", "--no-newline"],
        ["xclip", "-selection", "clipboard", "-o"],
        ["xsel", "--clipboard", "--output"],
    )

    for command in clipboard_commands:
        if shutil.which(command[0]) is None:
            continue

        result = subprocess.run(command, capture_output=True, text=True, check=False)
        if result.returncode == 0:
            return result.stdout

    raise RuntimeError(
        "No supported clipboard reader found. Tried pbpaste, wl-paste, xclip, and xsel."
    )


def write_clipboard(text: str) -> None:
    """Write text to the system clipboard."""
    clipboard_commands = (
        ["pbcopy"],
        ["wl-copy"],
        ["xclip", "-selection", "clipboard"],
        ["xsel", "--clipboard", "--input"],
    )

    for command in clipboard_commands:
        if shutil.which(command[0]) is None:
            continue

        result = subprocess.run(command, input=text, text=True, check=False)
        if result.returncode == 0:
            return

    raise RuntimeError(
        "No supported clipboard writer found. Tried pbcopy, wl-copy, xclip, and xsel."
    )


def parse_csv_list(value: str) -> list[str]:
    """Parse a comma-separated list into cleaned string values."""
    items = [item.strip() for item in value.split(",")]
    cleaned = [item for item in items if item]
    if not cleaned:
        raise ValueError("Expected at least one value.")
    return cleaned


def load_yaml(text: str) -> dict[str, Any]:
    """Load YAML text into a mapping."""
    data = parse_yaml(text)
    if not isinstance(data, dict):
        raise ValueError("Clipboard YAML must parse to a top-level mapping.")
    return data


def get_ops(data: dict[str, Any]) -> dict[str, Any]:
    """Return the ops mapping from a Dagster config document."""
    ops = data.get("ops")
    if not isinstance(ops, dict) or not ops:
        raise ValueError("Expected a non-empty top-level 'ops' mapping.")
    return ops


def get_default_shared_values(data: dict[str, Any]) -> dict[str, Any]:
    """Get default shared values from the first op config that defines them."""
    ops = get_ops(data)
    for op_name, op_data in ops.items():
        if not isinstance(op_data, dict):
            raise ValueError(f"Op '{op_name}' must be a mapping.")

        config = op_data.get("config")
        if not isinstance(config, dict):
            raise ValueError(f"Op '{op_name}' must contain a 'config' mapping.")

        if not all(key in config for key in SHARED_CONFIG_KEYS):
            continue

        diseases = config["diseases"]
        locations = config["locations"]
        exclude_last_n_days = config["exclude_last_n_days"]
        n_training_days = config["n_training_days"]

        if not isinstance(diseases, list) or not all(
            isinstance(item, str) for item in diseases
        ):
            raise ValueError("'diseases' must be a list of strings.")

        if not isinstance(locations, list) or not all(
            isinstance(item, str) for item in locations
        ):
            raise ValueError("'locations' must be a list of strings.")

        if not isinstance(exclude_last_n_days, int):
            raise ValueError("'exclude_last_n_days' must be an integer.")

        if not isinstance(n_training_days, int):
            raise ValueError("'n_training_days' must be an integer.")

        return {
            "diseases": diseases,
            "exclude_last_n_days": exclude_last_n_days,
            "locations": locations,
            "n_training_days": n_training_days,
        }

    raise ValueError(
        "Could not find any op config containing all of: "
        f"{', '.join(SHARED_CONFIG_KEYS)}."
    )


def apply_shared_values(
    data: dict[str, Any],
    *,
    diseases: list[str],
    exclude_last_n_days: int,
    locations: list[str],
    n_training_days: int,
) -> tuple[dict[str, Any], dict[str, list[str]]]:
    """Apply shared config values to matching op config keys."""
    ops = get_ops(data)
    updated_ops = {
        "diseases": [],
        "exclude_last_n_days": [],
        "locations": [],
        "n_training_days": [],
    }

    for op_name, op_data in ops.items():
        if not isinstance(op_data, dict):
            raise ValueError(f"Op '{op_name}' must be a mapping.")

        config = op_data.get("config")
        if not isinstance(config, dict):
            raise ValueError(f"Op '{op_name}' must contain a 'config' mapping.")

        for disease_key in DISEASE_KEYS:
            if disease_key in config:
                config[disease_key] = list(diseases)
                updated_ops["diseases"].append(op_name)
                break

        if "exclude_last_n_days" in config:
            config["exclude_last_n_days"] = exclude_last_n_days
            updated_ops["exclude_last_n_days"].append(op_name)

        if "locations" in config:
            config["locations"] = list(locations)
            updated_ops["locations"].append(op_name)

        if "n_training_days" in config:
            config["n_training_days"] = n_training_days
            updated_ops["n_training_days"].append(op_name)

    return data, updated_ops


def dump_yaml(data: dict[str, Any]) -> str:
    """Dump YAML with stable key order and block style lists."""
    return format_yaml(data)


def prompt_shared_values(defaults: dict[str, Any]) -> dict[str, Any]:
    """Prompt once for the shared config values."""
    console.print(
        Panel.fit(
            "Paste source YAML into your clipboard first. "
            "This tool will update the shared task parameters and copy the result back.",
            title="Clipboard Config Editor",
        )
    )

    diseases = parse_csv_list(
        Prompt.ask(
            "Diseases (comma-separated)",
            default=", ".join(defaults["diseases"]),
        )
    )
    exclude_last_n_days = IntPrompt.ask(
        "exclude_last_n_days",
        default=defaults["exclude_last_n_days"],
    )
    n_training_days = IntPrompt.ask(
        "n_training_days",
        default=defaults["n_training_days"],
    )
    locations = parse_csv_list(
        Prompt.ask(
            "Locations (comma-separated)",
            default=", ".join(defaults["locations"]),
        )
    )

    return {
        "diseases": diseases,
        "exclude_last_n_days": exclude_last_n_days,
        "locations": locations,
        "n_training_days": n_training_days,
    }


def show_summary(
    updated_values: dict[str, Any], updated_ops: dict[str, list[str]]
) -> None:
    """Render a concise summary of what will be updated."""
    table = Table(title="Shared Values Applied")
    table.add_column("Field", style="cyan")
    table.add_column("Value", overflow="fold", ratio=2)
    table.add_column("Updated ops", overflow="fold", ratio=3)
    table.add_row(
        "Diseases",
        ", ".join(updated_values["diseases"]),
        ", ".join(updated_ops["diseases"]) or "-",
    )
    table.add_row(
        "exclude_last_n_days",
        str(updated_values["exclude_last_n_days"]),
        ", ".join(updated_ops["exclude_last_n_days"]) or "-",
    )
    table.add_row(
        "Locations",
        ", ".join(updated_values["locations"]),
        ", ".join(updated_ops["locations"]) or "-",
    )
    table.add_row(
        "n_training_days",
        str(updated_values["n_training_days"]),
        ", ".join(updated_ops["n_training_days"]) or "-",
    )
    console.print(table)


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI parser."""
    parser = argparse.ArgumentParser(
        description=(
            "Read Dagster YAML from the clipboard, update shared config values "
            "across every op, and copy the result back to the clipboard."
        )
    )
    parser.add_argument(
        "--print-output",
        action="store_true",
        help="Print the rewritten YAML to stdout after copying it to the clipboard.",
    )
    return parser


def main() -> int:
    """Run the CLI."""
    args = build_parser().parse_args()

    try:
        input_text = read_clipboard()
        data = load_yaml(input_text)
        defaults = get_default_shared_values(data)
        updated_values = prompt_shared_values(defaults)
        data, updated_ops = apply_shared_values(data, **updated_values)
        output_text = dump_yaml(data)
        write_clipboard(output_text)
        show_summary(updated_values, updated_ops)
        console.print("[green]Updated YAML copied to clipboard.[/green]")

        if args.print_output:
            console.print()
            console.print(output_text)
    except Exception as exc:  # pragma: no cover - CLI exception boundary
        console.print(f"[red]Error:[/red] {exc}")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
