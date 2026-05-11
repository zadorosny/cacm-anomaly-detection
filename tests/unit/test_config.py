"""Smoke tests para config e package metadata."""

from __future__ import annotations

import cacm
from cacm.config import settings


def test_package_has_version():
    assert cacm.__version__


def test_settings_paths_are_relative_to_data_root():
    assert settings.bronze_path == settings.data_root / "bronze"
    assert settings.silver_path == settings.data_root / "silver"
    assert settings.gold_path == settings.data_root / "gold"


def test_score_bins_monotonic():
    assert (
        settings.score_bin_baixa
        < settings.score_bin_media
        < settings.score_bin_alta
        < settings.score_bin_critica
        < 1.0
    )
