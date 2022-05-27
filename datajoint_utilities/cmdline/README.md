# `cmdline`

General utilities for creating command-line interfaces.

## `tmplcfg.py`

Configure and fill file templates.

_Problem_:

- You have several files that need to have values set, these can be secrets or just configuration options.
  - The same values may need to be set across different files, or some files may need values specific only to that file.
- You have several variables stored in either `.env` files, within the operating system's environment, or need to specify different values than what exists from the default locations.

The `tmplcfg` command-line interface will aggregate variables from various sources and process multiple template files that have predefined replacement tags.

_Example_:

The following variables need to be inserted across three different files: `PGPASSWORD, PGDATABASE, PGUSER, DJANGO_SECRET_KEY, PGREADONLY, DJ_HOST, DJ_PASS, DJ_USER`.

file: `_template.env`

```bash
PGPASSWORD=%PGPASSWORD%
PGDATABASE=%PGDATABASE%
PGHOST=${PGHOST}
PGUSER=admin
```

file: `settings_template.py`

```python
SECRET_KEY = "%DJANGO_SECRET_KEY%"
S3_ACCESS = {}
DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql_psycopg2",
        "NAME": "%PGDATABASE%",
        "USER": "%PGUSER%",
        "PASSWORD": "%PGPASSWORD%",
        "HOST": "%PGHOST%",
        "PORT": "5432",
        "OPTIONS": {"options": "-c default_transaction_read_only=%PGREADONLY%"},
    }
}
```

file: `djconfig_template.json`

```json
{
  "connection.charset": "",
  "connection.init_function": null,
  "database.host": "%DJ_HOST%",
  "database.password": "%DJ_PASS%",
  "database.port": 3306,
  "database.prefix": "",
  "database.reconnect": true,
  "database.use_tls": null,
  "database.user": "%DJ_USER%",
  "display.limit": 35,
  "display.show_tuple_count": true,
  "display.width": 25,
  "enable_python_native_blobs": true,
  "fetch_format": "array",
  "loglevel": "INFO",
  "safemode": true
}
```

Use a combination of existing resources:

- the `--env-file` option for reading from an existing environment file
- the `--env-os` option for reading from the system environment
- the `--env` option for specifying a key-value pair

```bash
tmplcfg \
    --env-file=secrets.env \
    --env-os=PGPASSWORD,PGDATABASE,PGHOST,PGUSER \
    --env=DJANGO_SECRET_KEY=mysecretkey \
    --sources=~/_template.env /app/settings_template.py ~/djconfig_template.json
```

The above command will load all the variables from the specified sources and use those to write to the following new files with the tagged values replaced.

```bash
~/.env
/app/settings.py
~/djconfig.json
```

Additional options:

- specify a different tag identifier instead of the default "%"
- change permissions for written files
- append to existing files during writing
- toggle writing missing variables to files

Variables are sourced in the following order:

1. system environment variables pulled from `os.environ`
2. variables defined with the `--env-file` cli option
3. individual variables defined with the `--env` cli option
4. any default values defined in `self.default_vars` if they are missing from `self.environment`

### Help

```
usage: tmplcfg [-h] [-V] [-v] [--env-file PATH] [-e KEY=VAL] [-g [ENV1,ENV2 ENV3 ...]]
               [--write-mode STR] [--delim STR] [--rdelim STR] [--chmod STR] [--allow-empty]
               [--none-val STR] [-t [PATH ...]] [-s PATH [PATH ...]]

Configure and Fill File Templates

A tool to fill in templated configuration files from dot environment files, system
environment variables, or variables and values passed as command options. The same pool
of defined variables can be used to process multiple files at once.

Example:
    Usage as a console script:

        tmplcfg --help
        tmplcfg --version
        tmplcfg --env-file=.env -e X=1 my/template/file.txt
        tmplcfg -vv --env-file=../.env -e VAR1=VAL --env=VAR2=VAL \
            --write-mode=a --chmod=660 -t settings.py -s settings_template.py

optional arguments:
  -h, --help            show this help message and exit
  -V, --version         show program's version number and exit
  -v, --verbose         increase logging verbosity by specifying multiple (default: 0)
  --env-file PATH       specify an environment file to load (default: None)
  -e KEY=VAL, --env KEY=VAL
                        specify a single environment variable to use (default: None)
  -g [ENV1,ENV2 ENV3 ...], --env-os [ENV1,ENV2 ENV3 ...]
                        a comma-separated or space separated list of global environment
                        variables to try and retrieve from `os.environ` (default: None)
  --write-mode STR      file open mode for the written target file (default: w)
  --delim STR           replacement character delimiter (default: %)
  --rdelim STR          closing/right replacement character delimiter, uses left if missing
                        (default: None)
  --chmod STR           file permissions code (default: None)
  --allow-empty         allow empty variables to be stored in the pool of variables and set
                        empty values when writing to target files (default: False)
  --none-val STR        The value to use if the replacement is 'None' in python. Defaults to an
                        empty string (default: )

file path arguments:
  All file paths must be an absolute path and not a file name.

  -t [PATH ...], --targets [PATH ...]
                        specify a target file to write to (default: None)
  -s PATH [PATH ...], --sources PATH [PATH ...]
                        specify source/template files to be configured (Required) (default:
                        None)
```

Notes:

- Python>=3.9 required
- Joseph Burling <joseph@datajoint.com>

TODO:

- custom secrets variables
- import a custom defaults dictionary passed to EnvironVars
- custom regex to `strip_source_name`
