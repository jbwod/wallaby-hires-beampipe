from scripts.version import check_version, metadata_version
from wallaby_hires.__main__ import package_version


def test_package_and_release_versions_match():
    assert metadata_version() == "0.1.19"
    assert package_version() == metadata_version()
    assert check_version("v0.1.19") == "0.1.19"
