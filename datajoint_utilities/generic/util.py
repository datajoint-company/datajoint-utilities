# type: ignore


def detect_min_python_version(major=3, minor=10, micro=0):
    import sys

    min_py = (major, minor, micro)
    if sys.version_info < min_py:
        raise RuntimeError("Python %s.%s.%s or later is required.\n" % min_py)
