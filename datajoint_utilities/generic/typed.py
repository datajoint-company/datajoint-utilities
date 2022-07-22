import datajoint_utilities.typing as djt  # isort: skip

import datetime as dt
import hashlib
import importlib
import inspect
import itertools
import json
import os
import typing as typ
from dataclasses import dataclass, field
from importlib import import_module
from importlib.metadata import PackageNotFoundError, packages_distributions
from importlib.util import find_spec
from pathlib import Path
from uuid import UUID

import datajoint as dj
import typing_extensions as typx
from datajoint.errors import MissingTableError, QueryError


# frame stack functions ----------------------------------------------------------------
def select_stack_frame(stack: djt.FrameInfoList, back: int = 1) -> djt.FrameInfo | None:
    return stack[back] if len(stack) > back else None


def calling_frame(frame: djt.FrameStack) -> djt.FrameType | None | typ.NoReturn:
    if djt.is_frame(frame):
        return frame.f_back or None
    if djt.is_frame_info_list(frame):
        f_back = select_stack_frame(frame, 1)
        return f_back.frame if f_back else None
    raise TypeError(
        f"'frame' must be of type `list[inspect.FrameInfo] | types.FrameType`"
        f", not '{type(frame).__name__}'."
    )


def calling_frame_locals(frame: djt.FrameStack) -> djt.DictObj:
    cf = calling_frame(frame)
    return cf.f_locals if cf else {}


def calling_frame_globals(frame: djt.FrameStack) -> djt.DictObj:
    cf = calling_frame(frame)
    return cf.f_globals if cf else {}


def calling_frame_file(frame: djt.FrameStack) -> Path | None:
    cf = calling_frame(frame)
    return Path(cf.f_code.co_filename) if cf else None


def get_module_objects(
    module: str | djt.ModuleType, must_work: bool = True
) -> djt.DictObj | None | typ.NoReturn:
    if isinstance(module, str):
        module = import_module(module)
    if inspect.ismodule(module):
        return vars(module)
    if must_work:
        raise TypeError(
            f"The argument 'module'<{type(module).__name__}> must be "
            "a module's name as a string or ModuleType object."
        )


def module_name_from_frame(source: djt.FrameStack) -> str:
    frame = calling_frame(source)
    module = inspect.getmodule(frame)
    if module is None:
        return ""
    module_name = getattr(module.__spec__, "name", "")
    if not module_name and module.__name__ == "__main__":
        module_name = Path(module.__file__).stem if module.__file__ else ""
    return module_name


def pkg_name_from_frame(source: djt.FrameStack, fallback: str = "") -> str:
    module_root = module_name_from_frame(source).split(".", maxsplit=1)[0]
    return (
        module_root
        if module_root and module_root in packages_distributions()
        else fallback
    )


# path functions -----------------------------------------------------------------------
@typ.overload
def pkg_abspath(
    import_name: str | None = None,
    must_exist: typ.Literal[True] = True,
) -> Path | typ.NoReturn:
    ...


@typ.overload
def pkg_abspath(
    import_name: str | None = None,
    must_exist: typ.Literal[False] = False,
) -> Path | None:
    ...


def pkg_abspath(
    import_name: str | None = None,
    must_exist: bool = True,
) -> Path | None | typ.NoReturn:
    import_name = import_name or pkg_name_from_frame(inspect.stack())
    if pkg_spec := find_spec(import_name):
        return Path(str(pkg_spec.origin)).parent
    if must_exist:
        raise PackageNotFoundError(import_name)
    return None


def pkg_relpath(
    file: str | Path,
    *,
    package_import_name: str | None = None,
    package_root_path: str | Path | None = None,
    append_to_package_root_path: str | Path | None = None,
) -> Path | None:
    src_path = Path(file)
    root_path = Path(package_root_path or pkg_abspath(package_import_name))
    if append_to_package_root_path:
        root_path /= append_to_package_root_path
    return (
        src_path.relative_to(root_path) if src_path.is_relative_to(root_path) else None
    )


def as_module_parts(
    file_or_parts: str | Path | typ.Sequence[str],
) -> tuple[str, ...] | typ.NoReturn:
    """Like `Path().parts` but with no extension at end for building module names.

    Args:
        file_or_parts (str | Path | Sequence[str]): A sequence of path parts or a path string to get parts from.

    Raises:
        TypeError: Incorrect type of argument.

    Returns:
        tuple[str, ...]: A sequence of path parts.
    """
    if file_or_parts is None:
        return tuple()

    if isinstance(file_or_parts, typ.Sequence):
        return tuple(p for p in file_or_parts if p)

    if djt.is_pathstr(file_or_parts):
        file = Path(file_or_parts)
        return tuple(p for p in (*file.parts[:-1], file.stem) if p)

    raise TypeError(
        f"'file_or_parts' must be of type <{str | Path | typ.Sequence}>, "
        f"not '{type(file_or_parts).__name__}'"
    )


def chain_path_parts(*args: str | Path | typ.Sequence[str], sep: str = ".") -> str:
    part_gen = (as_module_parts(parts) for parts in args)
    return sep.join(itertools.chain.from_iterable(part_gen))


def path_maybe_from_frame(source: str | Path | djt.FrameStack | None) -> Path | None:
    if djt.is_frame_stack(source):
        return calling_frame_file(source)
    if djt.is_pathstr(source) and source:
        return Path(source)


@dataclass
class SourceFileInfo:
    file: Path = field(default_factory=Path)
    pkg_name: str = ""
    pkg_relpath: Path = field(default_factory=Path)


# datetime functions -------------------------------------------------------------------
def datetime_to_utcaware(
    datetime: dt.datetime, tzinfo: dt.tzinfo | None = None
) -> dt.datetime:
    local_date = datetime.astimezone(tz=tzinfo)
    dt_diff = local_date - (local_date.utcoffset() or dt.timedelta(0))
    return dt_diff.replace(tzinfo=dt.timezone.utc)


def date_to_utcaware(date: dt.date, tzinfo: dt.tzinfo | None = None) -> dt.datetime:
    return datetime_to_utcaware(dt.datetime(date.year, date.month, date.day), tzinfo)


def from_utc_timestamp(timestamp: int, *, precision: float = 1e6) -> dt.datetime:
    return dt.datetime.fromtimestamp(timestamp / precision, dt.timezone.utc)


def utc_timestamp(
    datetime: djt.TimeStampable | None = None,
    *,
    precision: float = 1e6,
    tzinfo: dt.tzinfo | None = None,
) -> int:
    if isinstance(datetime, dt.date):
        datetime = date_to_utcaware(datetime, tzinfo)
    elif isinstance(datetime, dt.datetime):
        datetime = datetime_to_utcaware(datetime, tzinfo)
    elif isinstance(datetime, dt.timedelta):
        datetime = from_utc_timestamp(0, precision=precision) + datetime
    else:
        datetime = dt.datetime.now(tz=dt.timezone.utc)
    return int(round(datetime.timestamp() * precision))


# misc functions -----------------------------------------------------------------------
def _extract(
    dict_: djt.MapObj,
    keys: typ.Iterable[str],
    *,
    prefix: str = "",
    suffix: str = "",
) -> djt.DictObj:
    return {f"{prefix}{key}{suffix}": dict_[key] for key in keys if key in dict_}


@typ.overload
def subset(
    dict_or_seq_of: typ.Sequence[djt.MapObj],
    *dict_keys: str,
    **options: str,
) -> list[djt.DictObj]:
    ...


@typ.overload
def subset(
    dict_or_seq_of: djt.MapObj,
    *dict_keys: str,
    **options: str,
) -> djt.DictObj:
    ...


def subset(
    dict_or_seq_of: djt.MapObj | typ.Sequence[djt.MapObj],
    *dict_keys: str,
    **options: str,
) -> list[djt.DictObj] | djt.DictObj:
    if isinstance(dict_or_seq_of, typ.Sequence):
        return [_extract(map_, dict_keys, **options) for map_ in dict_or_seq_of]
    if djt.is_strmap(dict_or_seq_of):
        return _extract(dict_or_seq_of, dict_keys, **options)
    raise TypeError(f"'dict_or_seq_of' must be {djt.MapObj | typ.Sequence[djt.MapObj]}")


class EnumNAError(Exception):
    def __init__(self, enum: str | None = "NA") -> None:
        txt = f": '{enum}'" if enum else ""
        self.message = f"Invalid enum value{txt}."
        super().__init__(self.message)


_Tstr = typ.TypeVar("_Tstr", bound=str)


def assert_enum(enum: _Tstr | None, enums: typ.Container[str]) -> _Tstr | typ.NoReturn:
    if not enum or enum == "NA" or enum not in enums:
        raise EnumNAError(enum)
    return enum


# keyword arguments functions ----------------------------------------------------------
def split_kwargs(
    keys_from: djt.MapObj | typ.Sequence[str] | str, kwargs: djt.MutMapObj
) -> tuple[djt.DictObj, djt.DictObj]:
    keys = [keys_from] if isinstance(keys_from, str) else list(keys_from)
    extracted_kwargs = subset(kwargs, *keys)
    other_kwargs = {k: v for k, v in kwargs.items() if k not in extracted_kwargs}
    return extracted_kwargs, other_kwargs


def pop_kwargs(
    keys_from: djt.MapObj | typ.Sequence[str] | str, kwargs: djt.MutMapObj
) -> djt.DictObj:
    keys = [keys_from] if isinstance(keys_from, str) else list(keys_from)
    return {k: kwargs.pop(k) for k in keys if k in kwargs}


def get_part_tbl(cls: type[djt.T_Table], part: str) -> dj.Part:
    part_table = getattr(cls, part, None)
    if not djt.is_parttable(part_table):
        raise MissingTableError(f"Failed to get part table '{part}' from {cls}.")
    return part_table


# uuid functions -----------------------------------------------------------------------
def norm_uuid(uuid: object = None) -> UUID | None:
    return UUID(str(uuid)) if djt.is_uuid_str(uuid) else None


def json_defaults(obj: object) -> object | typ.NoReturn:
    if djt.is_timestampable(obj):
        return utc_timestamp(obj)
    if uuid_str := norm_uuid(obj):
        return str(uuid_str).replace("-", "")
    try:
        return str(obj)
    except Exception as err:
        raise TypeError(
            f"Object of type '{type(obj).__name__}' is not serializable."
        ) from err


def json_dumps(obj: object) -> str:
    return json.dumps(
        obj,
        default=json_defaults,
        ensure_ascii=False,
        sort_keys=True,
        indent=None,
        separators=(",", ":"),
    )


def to_bytes(obj: object) -> bytes:
    if obj is None or (isinstance(obj, str) and not obj):
        return b""
    if djt.is_ndarray(obj):
        return obj.tobytes()
    return json_dumps(obj).encode()


def to_hash(*args: object) -> str:
    hashed = hashlib.md5()
    for item in args:
        hashed.update(to_bytes(item))
    return hashed.hexdigest()


def to_uuid(*args: object) -> UUID:
    return UUID(to_hash(*args))


def get_uuid_from_table(tbl: djt.BaseTable, from_key: str) -> UUID | None:
    try:
        return norm_uuid(tbl.fetch1(from_key))  # type: ignore
    except Exception:
        return None


def get_uuid_from_mapping(mapping: djt.MapObj, from_key: str) -> UUID | None:
    return norm_uuid(mapping.get(from_key))


def get_uuid_from_keys(
    obj: djt.MapObj | djt.BaseTable, try_keys: str | typ.Sequence[str] | None = None
) -> UUID | None:
    if not try_keys:
        return None
    keys = [try_keys] if isinstance(try_keys, str) else list(try_keys)
    key = keys.pop(0)
    uuid = (
        get_uuid_from_table(obj, key)
        if djt.is_djtable(obj)
        else (get_uuid_from_mapping(obj, key) if isinstance(obj, typ.Mapping) else None)
    )
    return uuid or get_uuid_from_keys(obj, keys)


def get_uuid(
    obj: djt.ContainsUUID,
    from_keys: str | typ.Sequence[str] | None = None,
) -> UUID | None:
    return (
        norm_uuid(obj)
        if isinstance(obj, (str, UUID))
        else get_uuid_from_keys(obj, from_keys)
    )


# datajoint functions ------------------------------------------------------------------
def dj_config_custom_entry(
    key: str, *, fallback: object = None, envar: str = ""
) -> object:
    """Try to get a custom entry from the DataJoint configuration.

    Priority:
        1. DataJoint configuration's `custom` entry given `key`.
        2. DataJoint configuration's top-level entry given `key`.
        3. Environment variable specified by `envar`.
        4. Fallback value given by `fallback`.

    Args:
        key (str): Key to look up in the DataJoint configuration.
        fallback (object): Fallback value if `key` or `envar` is not found.
        envar (str): Environment variable to use as a fallback.

    Raises:
        TypeError: If `custom` entry in the DataJoint configuration is not of the correct type.

    Returns:
        object: Value of the configuration entry.

    Examples:
            dj_config_custom_entry("database.prefix")
    """
    value = os.getenv(envar, fallback) if envar else fallback
    custom: object = dj.config.get("custom", {})
    if not djt.is_strmap(custom):
        raise TypeError(f"Expected `custom` to be a dict, got {type(custom)}.")
    return custom.get(key, dj.config.get(key, value))


def get_prefix(fallback: str = "unknown_database", *, sep: str = "_") -> str:
    """Get the database prefix from the `datajoint.config` property.

    Args:
        fallback (str, optional): Fallback value if prefix is not found. Defaults to
            `"unknown_database"`.
        sep (str, optional): Separator to append at the end of the prefix string.

    Returns:
        string: The database prefix string.

    Examples:
        Using the default values.

            get_prefix()
            > 'my_pkg_name_'
    """
    entry = dj_config_custom_entry(
        "database.prefix", fallback=fallback, envar="DATABASE_PREFIX"
    )
    return f"{entry}{sep}" if entry else ""


def append_to_prefix(name: str, **kwargs: str) -> str:
    """Append a module name to the configured database prefix.

    Args:
        name (str): Name of module to append to database prefix.
        kwargs (str): Keyword arguments passed to `get_prefix`.

    Returns:
        string: Extended database prefix.

    Examples:
        Using the default values.

            append_to_prefix('core')
            > 'my_pkg_name_core'
    """
    if not name:
        raise ValueError("string to append cannot be empty.")
    return get_prefix(**kwargs) + name


def set_missing_configs(
    *,
    db_prefix: str | None = None,
    db_host: str = "localhost",
    db_user: str = "root",
    db_pwd: str = "simple",
    file: str | Path | None = None,
) -> None:
    """Set common DataJoint configuration values _only_ if they are missing from the
    existing config or configuration file.

    Args:
        db_prefix (str | None): Value to use for `"database.prefix"`.
        db_host (str): Value to use for `"database.host"`.
        db_user (str): Value to use for `"database.user"`.
        db_pwd (str): Value to use for `"database.password"`.
        file (Path | str | None): Path to initialize a datajoint `JSON` config
            file before setting the rest of the arguments.
    """
    if file:
        dj.config.load(file)

    dj.config["database.user"] = dj.config.get("database.user") or db_user
    dj.config["database.host"] = dj.config.get("database.host") or db_host
    dj.config["database.password"] = dj.config.get("database.password") or db_pwd
    dj.config["custom"] = {
        **{"database.prefix": db_prefix or get_prefix(sep="")},
        **dj.config.get("custom", {}),
    }


def insert_row(
    cls: type[djt.T_UserTable],
    attrs: djt.MapObj,
    **insert_opts: typx.Unpack[djt.InsertOpts],
) -> djt.T_UserTable:
    cls().insert1(attrs, **insert_opts)  # type: ignore
    pks = typ.cast(list[str], cls.primary_key)
    return typ.cast(djt.T_UserTable, cls & subset(attrs, *pks))


def split_insert_opts(kwargs: djt.MutMapObj) -> tuple[djt.InsertOpts, djt.MutMapObj]:
    insert_opts, kwargs_ = split_kwargs(inspect.get_annotations(djt.InsertOpts), kwargs)
    return typ.cast(djt.InsertOpts, insert_opts), kwargs_


def pop_insert_opts(kwargs: djt.MutMapObj) -> djt.InsertOpts:
    return typ.cast(
        djt.InsertOpts,
        pop_kwargs(inspect.get_annotations(djt.InsertOpts), kwargs),
    )


def dj_table_info(
    table: djt.UserTable, name_prefix: str = "", section_level: int = 2
) -> str:
    db_name: str = table.database  # type: ignore
    cls_name: str = table.__name__ or ""  # type: ignore
    if table.database is None or table.heading is None or not cls_name:
        raise dj.errors.DataJointError(
            f"Class {cls_name} is not properly declared "
            "(schema decorator not applied?)"
        )
    db_table_name: str = table.table_name  # type: ignore
    table_name = f"{name_prefix}.{cls_name}" if name_prefix else cls_name
    table_comment: str = (
        table.heading.table_status["comment"] if table.heading.table_status else ""
    )
    table_attrs = str(table.heading).splitlines()
    if table_attrs[0].startswith("#"):
        table_attrs = table_attrs[1:]
    table_attrs = "\n".join(table_attrs)
    nl = "\n\n"
    return (
        f"{nl}{'#' * section_level} {table_name}{nl}"
        f"_{table_comment}_{nl}"
        f"**Attributes**{nl}```\n{table_attrs}\n```{nl}"
        f"**Database**{nl}**_`{db_name}`_**{nl}"
        f"**Table**{nl}**_`{db_table_name}`_**{nl}"
    )


def markdown_dj_tables(
    schema_module: str | djt.ModuleType, section_start: int = 2
) -> str:
    if isinstance(schema_module, str):
        schema_module = importlib.import_module(schema_module)
    table_info = [f"{'#' * section_start} DataJoint Tables\n"]
    for table in [
        getattr(schema_module, name)
        for name, _ in inspect.getmembers(schema_module, djt.is_djtable)
    ]:
        table_info.append(dj_table_info(table, section_level=section_start + 1))
        table_info.extend(
            dj_table_info(
                part_table, name_prefix=table.__name__, section_level=section_start + 2
            )
            for part_table in [
                getattr(table, name)
                for name, _ in inspect.getmembers(table, djt.is_parttable)
            ]
        )
    return "\n".join(table_info)
