def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "catalog: use the real CFA data catalog instead of mocked test data",
    )
