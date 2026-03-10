"""Compatibility namespace that exposes the top-level ``pipelines`` package."""

from __future__ import annotations

from importlib import import_module

_pipelines = import_module("pipelines")

# Reuse the top-level package search path so submodule imports like
# ``cfa.stf.pipelines.data`` resolve to the existing implementation.
__path__ = _pipelines.__path__
__all__ = getattr(_pipelines, "__all__", [])


def __getattr__(name: str):
    return getattr(_pipelines, name)


def __dir__():
    return sorted(set(globals()) | set(dir(_pipelines)))
