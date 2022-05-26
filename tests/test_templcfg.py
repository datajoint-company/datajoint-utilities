import json
import tempfile
import textwrap
from pathlib import Path
from shutil import rmtree

import pytest
from datajoint_utilities.cmdline.templcfg import run


@pytest.fixture
def template_config(filepath=""):
    return Path(filepath or "./tests/sample_configfile.json")


@pytest.fixture
def temp_dir():
    tmp = Path(tempfile.gettempdir()) / "pytest_templcfg"
    rmtree(tmp, ignore_errors=True)
    tmp.mkdir(0o755, True, True)
    return tmp


@pytest.fixture
def dot_env_file(temp_dir):
    filepath = temp_dir / ".env"
    txt = textwrap.dedent(
        """
        DJ_HOST=datajoint-rds.blah.blah.blah.com
        DJ_USER=admin
        DJ_PASS=shhhhh
        """
    )

    with open(filepath, "w") as fc:
        fc.write(txt)
    return filepath


@pytest.fixture
def template_content(template_config):
    try:
        with open(template_config) as fc:
            config = json.load(fc)
    except (json.JSONDecodeError, FileNotFoundError):
        config = {}
    return config


def test_configure_run(template_config, temp_dir, dot_env_file, template_content):
    cli_args = [
        "-vv",
        f"--env-file={dot_env_file}",
        "--env=",
        "--env=VAR0",
        "--env-os=PYTHONPATH, VAR1 VAR2",
        "-g",
        "VAR3",
        "--env-os=",
        "-e",
        "S3_SECRET=supersecretkey",
        "--env=S3_ACCESS=mysecrets",
        "--env=SOME_SETTING=true",
        "--write-mode=w",
        "--chmod=660",
        f"--sources={template_config}",
        "-t",
        f"{temp_dir}",
    ]
    configure = run(*cli_args)
    with open(temp_dir / "configfile.json") as fc:
        configured = json.load(fc)
    assert configured["database.host"] == "datajoint-rds.blah.blah.blah.com"
    assert configured["stores"]["ephys"]["access_key"] == "mysecrets"
    assert configured["stores"]["ephys"]["secret_key"] == "supersecretkey"
