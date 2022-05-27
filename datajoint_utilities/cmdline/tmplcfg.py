"""Configure and Fill File Templates

A tool to fill in templated configuration files from dot environment files, system
environment variables, or variables and values passed as command options. The same pool
of defined variables can be used to process multiple files at once.

Example:
    Usage as a console script:

        %(prog)s --help
        %(prog)s --version
        %(prog)s --env-file=.env -e X=1 my/template/file.txt
        %(prog)s -vv --env-file=../.env -e VAR1=VAL --env=VAR2=VAL \\
            --write-mode=a --chmod=660 -t settings.py -s settings_template.py
"""


import json
import logging
import os
import re
import sys
from pathlib import Path
from typing import Any, Generator, Literal, Optional, Sequence, Union, overload

import datajoint_utilities.cmdline as cmd
from dotenv import dotenv_values

T_StrPath = Union[str, Path]
T_NAStr = Union[str, None]
T_Environ = dict[str, T_NAStr]
T_WriteMode = Literal["a", "w", "a+", "w+"]

VERSION: str = "0.0.1"
PROG: str = "tmplcfg"
SECRETS: tuple[str, ...] = (
    "ALYX_PWD",
    "DJ_PASS",
    "DJANGO_SECRET_KEY",
    "FLATIRON_SERVER_PWD",
    "HTTP_DATA_SERVER_PWD",
    "PGPASSWORD",
    "S3_ACCESS",
    "S3_SECRET",
)

log: logging.Logger = logging.getLogger(PROG)


class ParseCLIArgs(cmd.ArgparseBase):
    def __init__(self, args: Sequence[str]) -> None:
        super().__init__(
            args,
            PROG,
            VERSION,
            __doc__,
            cmd.HelpFmtDefaultsDocstring,
        )

    def make(self):
        self.parser.add_argument(
            "--env-file",
            dest="env_file",
            help="specify an environment file to load",
            metavar="PATH",
            type=str,
        )
        self.parser.add_argument(
            "-e",
            "--env",
            action=cmd.EnvVarArgs,
            dest="kw_env",
            help="specify a single environment variable to use",
            metavar="KEY=VAL",
            type=str,
        )
        self.parser.add_argument(
            "-g",
            "--env-os",
            action=cmd.CommaSepArgs,
            dest="os_env",
            help="a comma-separated or space separated list of global environment"
            " variables to try and retrieve from `os.environ`",
            metavar="ENV1,ENV2 ENV3",
            type=str,
        )
        self.parser.add_argument(
            "--write-mode",
            choices=(
                "a",
                "w",
                "a+",
                "w+",
            ),
            default="w",
            dest="write_mode",
            help="file open mode for the written target file",
            metavar="STR",
            type=str,
        )
        self.parser.add_argument(
            "--delim",
            default="%",
            dest="ltag",
            help="replacement character delimiter",
            metavar="STR",
            type=str,
        )
        self.parser.add_argument(
            "--rdelim",
            dest="rtag",
            help="closing/right replacement character delimiter, uses left if missing",
            metavar="STR",
            type=str,
        )
        self.parser.add_argument(
            "--chmod",
            dest="chmod",
            help="file permissions code",
            metavar="STR",
            type=str,
        )
        self.parser.add_argument(
            "--allow-empty",
            action="store_true",
            dest="allow_empty",
            help="allow empty variables to be stored in the pool of variables "
            "and set empty values when writing to target files",
        )
        self.parser.add_argument(
            "--none-val",
            default="",
            dest="none_str",
            help="The value to use if the replacement is 'None' in python. "
            "Defaults to an empty string",
            metavar="STR",
            type=str,
        )
        files_group = self.parser.add_argument_group(
            "file path arguments",
            "All file paths must be an absolute path and not a file name.",
        )
        files_group.add_argument(
            "-t",
            "--targets",
            dest="target_paths",
            help="specify a target file to write to",
            metavar="PATH",
            nargs="*",
            type=str,
        )
        files_group.add_argument(
            "-s",
            "--sources",
            dest="source_paths",
            help="specify source/template files to be configured (Required)",
            metavar="PATH",
            nargs="+",
            type=str,
        )


def verbosity_logger(loglevel: int, base_level: str = "WARNING") -> logging.Logger:
    """Setup basic python logging

    Args:
        loglevel (int): Minimum log level number for emitting messages, e.g., '2'
        base_level (str, optional): Base level name for the logger.
            Defaults to "WARNING".

    Returns:
        logging.Logger: Updated module-level logger.
    """

    base_level = base_level.upper()

    if loglevel is None or not isinstance(loglevel, (float, int)):
        loglevel = logging.getLevelName(base_level)
    elif 10 > loglevel >= 1:
        base_loglevel = logging.getLevelName(base_level) / 10
        loglevel = max(int(base_loglevel - min(loglevel, base_loglevel)), 1) * 10
    elif loglevel % 10 == 0:
        loglevel = int(loglevel)
    else:
        loglevel = logging.getLevelName(base_level)

    std_out_handler = logging.StreamHandler(stream=sys.stdout)
    formatter = logging.Formatter(
        fmt="[%(asctime)s %(process)d %(processName)s "
        "%(levelname)s %(name)s]: %(message)s",
        datefmt="%z %Y-%m-%d %H:%M:%S",
    )

    log.handlers = []
    log.setLevel(loglevel)
    std_out_handler.setLevel(loglevel)
    std_out_handler.setFormatter(formatter)
    log.addHandler(std_out_handler)
    log.info(f"logging set to level: '{logging.getLevelName(loglevel)}'")

    return log


def _secret_key() -> str:
    """Create a Django secret key. Requires `django` module.

    Returns:
        _type_: Random string
    """
    try:
        from django.utils.crypto import get_random_string
    except ImportError as err:
        log.error(f"django not installed:\n{err}")
        return None
    chars = "abcdefghijklmnopqrstuvwxyz0123456789!@#$%^&*(-_=+)"
    return get_random_string(50, chars)


def mask_secret(
    key: str, value: T_NAStr, *, secrets: Optional[tuple[str, ...]] = None
) -> str:
    """Replace confidential item with asterisks

    Args:
        key (str): Key used to determine if item is a secret
        value (T_NAStr): Used to return input value or mask
        secrets (Optional[tuple[str, ...]], optional): List of secret keys

    Returns:
        str: Original value or asterisks
    """
    _secrets = secrets or []
    _secrets = {*SECRETS, *_secrets}
    return "********" if key in _secrets else (value or "")


def tag_strs(*args: str, prefix_: str = "%", suffix_: str = "%") -> tuple[str, ...]:
    """Add tags to key names

    Args:
        args (tuple): N positional args treated as keys to tag.
        prefix_ (str, optional): String to append to the left of the key.
            Defaults to "%".
        suffix_ (str, optional): String to append to the right of the key.
            Defaults to "%".

    Returns:
        tuple[str, ...]: tagged item(s), e.g., 'MYVAR' -> ('%MYVAR%',)
    """
    return tuple(f"{prefix_}{v}{suffix_}" for v in args)


def jsprint(dict_: dict[str, Any], mask: Optional[tuple[str, ...]] = None) -> str:
    """Pretty-print basic dictionary using json dumps.

    Args:
        dict_ (dict[str, Any]): A dictionary with standard value types and
            string keys.
        secrets (Optional[tuple[str, ...]], optional): List of secret keys

    Returns:
        str: String of dictionary object.
    """

    return "\n" + json.dumps(
        {k: mask_secret(k, v, secrets=mask) for k, v in dict_.items()}, indent=4
    )


def dict_clean(dict_: dict[Any, Any]) -> dict[Any, Any]:
    """Return a dictionary without the empty items, determined by an
    objects `__bool__` method. **Non-recursive**.

    Args:
        dict_ (dict[Any, Any]): A dictionary.

    Returns:
        dict[Any, Any]: A dictionary subset.
    """
    return {k: v for k, v in dict_.items() if v}


def _as_path(strpath: T_StrPath, must_exist=False) -> Path:
    path = Path(strpath).expanduser()
    if must_exist:
        if not path.exists():
            raise FileExistsError(f"path doesn't exist '{path}'")
        path = path.resolve()
    return path


def as_file_path(path: T_StrPath, must_exist=False) -> Path:
    """File path resolver

    Args:
        path (T_StrPath): Path-like object passed to `Path`
        must_exist (bool, optional): Check if path exists. Defaults to False.

    Raises:
        FileNotFoundError, FileExistsError: Path doesn't exist

    Returns:
        Path: Path to a file.
    """
    file = _as_path(path, must_exist=must_exist)
    if must_exist and not file.is_file():
        raise FileNotFoundError(f"path is not a file '{file}'")
    return file


def as_dir_path(path: T_StrPath, must_exist=False) -> Path:
    """Folder path resolver

    Args:
        path (T_StrPath): Path-like object passed to `Path`
        must_exist (bool, optional): Check if path exists. Defaults to False.

    Raises:
        NotADirectoryError, FileExistsError: Path doesn't exist

    Returns:
        Path: Path to a directory.
    """
    folder = _as_path(path, must_exist=must_exist)
    if must_exist and not folder.is_dir():
        raise NotADirectoryError(f"path is not a folder '{folder}'")
    return folder


def _read_file_content(file: T_StrPath, mode: str = "r") -> str:
    file = as_file_path(file, must_exist=True)
    with open(file, mode) as fc:
        text = fc.read() or ""
    return text


def read_file(
    file: T_StrPath,
    *,
    comment: str = "",
    remove_empty: bool = False,
    as_list: bool = False,
    mode: str = "r",
) -> Union[str, list[str]]:
    """Read a file with comments and return as a string or list

    Args:
        file (T_StrPath): file path
        comment (str, optional): comment string, e.g., "#" or "//".
            Defaults to not parsing comments.
        remove_empty (bool, optional): delete empty lines. Defaults to False.
        as_list (bool, optional): return a list of lines instead of one string.
            Defaults to True.

    Returns:
        Union[str, list[str]]: string or list of strings
    """
    if not comment and not remove_empty and not as_list:
        return _read_file_content(file, mode)

    file = as_file_path(file, must_exist=True)
    text = []
    with open(file, mode) as fc:
        for line in fc:
            if comment and line.startswith(comment):
                continue
            if remove_empty and not line.strip():
                continue
            text.append(line)
    if not as_list:
        text = "".join(text)
    return text


def read_dot_env_file(dot_env_file: T_StrPath) -> T_Environ:
    file = as_file_path(dot_env_file, must_exist=True)
    return dotenv_values(file)


def touch_file(file: T_StrPath, chmod: int = 0o664):
    file = as_file_path(file)
    try:
        file.parent.mkdir(parents=True, exist_ok=True)
        os.chmod(file.parent, 0o775)
        file.touch(exist_ok=True)
        if chmod:
            os.chmod(file, chmod)
    except Exception as err:
        log.warning(f"Failed to create file: {file}\n{err}")


def strip_source_name(file: Path, pattern: Optional[re.Pattern] = None):
    source_file = as_file_path(file)
    if pattern is None:
        pattern = re.compile(r"_template(?=\.)|^sample_")
    target_name = pattern.sub("", source_file.name)
    return source_file.with_name(target_name)


def resolve_path_pairs(
    sources: list[T_StrPath], targets: Optional[list[T_StrPath]] = None
):
    if not sources:
        sources = []
    if not targets:
        targets = []
    if not sources and not targets:
        return list(zip([], []))

    source_paths = [as_file_path(src, must_exist=True) for src in sources]

    n_targs = len(targets)
    if n_targs == 0:
        targets = [strip_source_name(src) for src in source_paths]
    elif n_targs == 1:
        try:
            target_directory = as_dir_path(targets[0], must_exist=True)
            targets = [
                target_directory / strip_source_name(src).name for src in source_paths
            ]
        except (FileExistsError, NotADirectoryError):
            targets = [as_file_path(t) for t in targets]
    else:
        targets = [as_file_path(t) for t in targets]

    if len(targets) != len(source_paths):
        raise ValueError(
            "Matching source and target files not found."
            f"\ntargets(n={len(targets)}): {targets}"
            f"\nsources(n={len(source_paths)}): {source_paths}"
        )

    file_pairs = list(zip(source_paths, targets))
    for src, targ in file_pairs:
        if src == targ:
            raise ValueError(
                f"target file '{targ}' cannot be the same as the source file '{src}'"
            )
    return file_pairs


class EnvironVars:
    def __init__(
        self,
        *,
        defaults: Optional[T_Environ] = None,
        ltag: T_NAStr = None,
        rtag: T_NAStr = None,
    ) -> None:
        self.default_vars: T_Environ = defaults or {"TEMPLATE_CFG_NO_DEFAULTS": "true"}
        self.ltag: str = ltag if ltag is not None else "%"
        self.rtag: str = rtag if rtag is not None else self.ltag
        self.environ_vars: T_Environ = {}
        self.dotfile_vars: T_Environ = {}
        self.environment: T_Environ = {}
        self._secrets: tuple[str, ...] = self.tag_keys(SECRETS)

    def load_vars(
        self,
        *args: str,
        env_file: Optional[T_StrPath] = None,
        allow_empty: bool = True,
        **kwargs: T_NAStr,
    ) -> None:
        """Get variables from various sources and append to `self.environment`.

        Variables are sourced in the following order:
            - system environment variables pulled from `os.environ`
            - variables defined with the `--env-file` cli option
            - individual variables defined with the `--env` cli option
            - any default values defined in `self.default_vars` if they are
                missing from `self.environment`

        Args:
            args: Key values to use to get from `os.environ`.
            env_file (Optional[T_StrPath], optional): A .env file path.
                Defaults to None.
            allow_empty (bool, optional): Allow loading of empty variables.
                Defaults to True.
        """

        self.environment |= self._load_environ_vars(
            *[a for a in args if a and isinstance(a, str)], allow_empty=allow_empty
        )
        self.environment |= self._load_dotfile_vars(env_file, allow_empty=allow_empty)
        self.environment |= kwargs if allow_empty else dict_clean(kwargs)
        self.environment |= self._load_defaults()
        log.debug("%s%s", "all variables loaded:", str(self))

    @overload
    def tag_keys(self, obj: str) -> T_NAStr:
        """Add tags to keys to mark them for replacement.

        Args:
            obj (str): A single key to be tagged.

        Returns:
            T_NAStr: A tagged key or None
        """
        ...

    @overload
    def tag_keys(self, obj: tuple[str, ...]) -> tuple[str, ...]:
        """Add tags to keys to mark them for replacement.

        Args:
            obj (tuple[str, ...]): A sequence of keys to be tagged.

        Returns:
            tuple[str, ...]: A sequence of tagged keys.
        """
        ...

    @overload
    def tag_keys(self, obj: T_Environ) -> T_Environ:
        """Add tags to keys to mark them for replacement.

        Args:
            obj (T_Environ): A dictionary with keys to be tagged.

        Returns:
            T_Environ: A dictionary with keys replaced with tagged versions.
        """
        ...

    def tag_keys(self, obj):
        if isinstance(obj, str):
            tag = tag_strs(obj, prefix_=self.ltag, suffix_=self.rtag)
            return tag[0] if tag else None
        elif isinstance(obj, (tuple, str)):
            return tag_strs(*obj, prefix_=self.ltag, suffix_=self.rtag)
        elif isinstance(obj, dict):
            tags = tag_strs(*obj.keys(), prefix_=self.ltag, suffix_=self.rtag)
            return dict(zip(tags, obj.values()))

    def check_missing(self, *args, raise_error=True):
        if missing := tuple(
            k for k in args if k not in self.environment or not self.environment.get(k)
        ):
            if raise_error:
                raise KeyError(f"missing {missing} from: {args}")
        return missing

    def _load_environ_vars(self, *args, allow_empty: bool = True):
        environ_vars = {arg: os.getenv(arg) for arg in args}
        if not allow_empty:
            environ_vars = dict_clean(environ_vars)
        self.environ_vars |= environ_vars
        log.debug("%s%s", "vars (environ values):", jsprint(environ_vars))
        return environ_vars

    def _load_dotfile_vars(
        self, env_file: Optional[T_StrPath], allow_empty: bool = True
    ) -> dict[str, str]:
        if not env_file:
            return {}
        dotfile_vars = read_dot_env_file(env_file)
        if not allow_empty:
            dotfile_vars = dict_clean(dotfile_vars)
        self.dotfile_vars |= dotfile_vars
        log.debug("%s%s", "vars (.env file values):", jsprint(dotfile_vars))
        return dotfile_vars

    def _load_defaults(self):
        default_vars = {
            k: v for k, v in self.default_vars.items() if k not in self.environment
        }
        log.debug("%s%s", "vars (default values):", jsprint(default_vars))
        return default_vars

    @staticmethod
    def _strip_quoted(value: str) -> str:
        if (value.startswith('"') and value.endswith('"')) or (
            value.startswith("'") and value.endswith("'")
        ):
            value = value[1:-1]
        return value

    def __str__(self) -> str:
        return jsprint(self.environment)

    def __repr__(self) -> str:
        return str(self)


class Configure(EnvironVars):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(
            ltag=kwargs.get("ltag"),
            rtag=kwargs.get("rtag"),
        )
        self.paths = resolve_path_pairs(
            kwargs.get("source_paths"),
            kwargs.get("target_paths"),
        )
        self._set_mappings(
            kwargs.get("env_file"),
            kwargs.get("os_env"),
            kwargs.get("kw_env"),
            kwargs.get("allow_empty", False),
        )
        self.none_str: str = kwargs.get("none_str", "")
        self.write_mode: T_WriteMode = kwargs.get("write_mode", "w")
        self._get_chmod(kwargs.get("chmod"))

    def _get_chmod(self, chmod: T_NAStr) -> None:
        self.chmod = int(chmod, base=8) if chmod else None

    def _set_mappings(
        self,
        env_file: Optional[T_StrPath] = None,
        os_env: Optional[Sequence[str]] = None,
        kw_env: Optional[T_Environ] = None,
        allow_empty: bool = False,
    ):
        os_env = os_env or []
        kw_env = kw_env or {}
        self.load_vars(
            *os_env,
            env_file=env_file,
            allow_empty=allow_empty,
            **kw_env,
        )
        self.replacements: T_Environ = self.tag_keys(self.environment)

    def _replace(self) -> Generator:
        for source_file, target_file in self.paths:
            log.debug(f"reading file: {source_file}")
            content: str = _read_file_content(source_file)
            for key, value in self.replacements.items():
                if key in content:
                    replacement = value if value is not None else self.none_str
                    log.debug(
                        f"replacing all occurrences of '{key}' w/ "
                        f"'{mask_secret(key, replacement, secrets=self._secrets)}'"
                    )
                    content = content.replace(key, replacement)
            yield target_file, content

    def write_contents(self) -> None:
        if not self.paths:
            log.warning("No files found to write to!")
            return
        for target_file, content in self._replace():
            if not target_file.parent.exists():
                target_file.parent.mkdir(0o775, True, True)
            log.debug(f"writing replacements text to file: '{target_file}'")
            with open(target_file, self.write_mode) as fc:
                fc.write(content)
            if self.chmod is not None:
                log.debug(f"updating target file permissions <{oct(self.chmod)}>")
                os.chmod(target_file, self.chmod)


def run(*args: str) -> Configure:
    """Main function that handles a list of arguments and runs the main program.

    Can be used for testing or importing.

    Args:
        args (str): Sequence of `str` arguments.

    Returns:
        Configure: configuration class instance
    """
    parser = ParseCLIArgs(args)
    verbosity_logger(parser.args.get("verbose"))
    configure = Configure(**parser.args)
    configure.write_contents()
    log.info("Template configuration complete.")
    return configure


def cli() -> None:
    """Calls this program, passing along the cli arguments extracted from `sys.argv`.

    This function can be used as entry point to create console scripts.

    Raises:
        SystemExit: End of program execution
    """
    run(*sys.argv[1:])
    raise SystemExit


if __name__ == "__main__":
    cli()
