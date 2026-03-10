"""Tests for the cfa.stf.pipelines namespace package alias."""

from __future__ import annotations

from importlib import import_module


def test_namespace_package_exposes_top_level_pipelines():
    namespace_pkg = import_module("cfa.stf.pipelines")
    legacy_pkg = import_module("pipelines")

    assert namespace_pkg.__path__ == legacy_pkg.__path__


def test_namespace_submodule_import_resolves():
    namespace_submodule = import_module("cfa.stf.pipelines.data")
    legacy_submodule = import_module("pipelines.data")

    assert namespace_submodule.__file__ == legacy_submodule.__file__
