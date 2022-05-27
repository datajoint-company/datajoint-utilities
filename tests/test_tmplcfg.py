import json
import os
import tempfile
import textwrap
from pathlib import Path
from shutil import rmtree

import pytest
from datajoint_utilities.cmdline.tmplcfg import run


@pytest.fixture
def template_config(filepath=""):
    return Path(filepath or "./tests/sample_configfile.json")


@pytest.fixture
def temp_dir():
    tmp = Path(tempfile.gettempdir()) / "pytest_tmplcfg"
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
        # comment
        EMPTY_VAR=
        NONE_VAR
        S3_SECRET=overriden
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
    os.environ["GLOBAL_VAR"] = ""
    cli_args = [
        "-vv",
        f"--env-file={dot_env_file}",
        "--env-os=USETLS, VAR0 VAR1, GLOBAL_VAR",
        "-g",
        "VAR2",
        "--env-os=",
        "-e",
        "VAR3=~[a-ZA-Z]!@#$%^&*(-_=+)",
        "--env=",
        "--env=VAR4",
        "--env=DB_PREFIX=",
        "--env=SAFEMODE=false",
        "-e",
        "S3_SECRET=supersecretkey",
        "--env=S3_ACCESS=mysecrets",
        "--write-mode=w",
        "--none-val=null",
        "--chmod=660",
        "--allow-empty",
        f"--sources={template_config}",
        "-t",
        f"{temp_dir}",
    ]

    configure = run(*cli_args)

    assert configure.environment["VAR0"] is None
    assert configure.environment["VAR3"] == "~[a-ZA-Z]!@#$%^&*(-_=+)"
    assert configure.environment["VAR4"] == ""
    assert configure.environment["GLOBAL_VAR"] == ""
    assert configure.environment["DB_PREFIX"] == ""
    assert configure.environment["EMPTY_VAR"] == ""
    assert configure.environment["NONE_VAR"] is None

    with open(temp_dir / "configfile.json") as fc:
        configured = json.load(fc)
    assert configured["database.prefix"] == ""
    assert configured["database.use_tls"] is None
    assert configured["safemode"] is False
    assert configured["database.host"] == "datajoint-rds.blah.blah.blah.com"
    assert configured["stores"]["ephys"]["access_key"] == "mysecrets"
    assert configured["stores"]["ephys"]["secret_key"] == "supersecretkey"
