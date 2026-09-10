from cfa.stf.routine.dagster_defs import ConfigOverride, Location, ModelBaseConfig


def test_model_base_config_defaults_to_150_day_lookback():
    assert ModelBaseConfig().n_lookback_days == 150


def test_location_override_can_select_all_available_history():
    config = ModelBaseConfig(
        n_lookback_days=150,
        config_overrides=[
            ConfigOverride(location=Location.CA, n_lookback_days=None).as_dict()
        ],
    )

    assert config.get_by_location(Location.CA).n_lookback_days is None
