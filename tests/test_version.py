from dj_search.meta import __version__ as version_meta
from dj_search import __version__ as version_pkg


def test_version():
    assert version_pkg == version_meta
