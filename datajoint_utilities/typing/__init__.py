from datajoint_utilities.generic.pkgutil import detect_min_python_version

detect_min_python_version(3, 10)

import datetime
import inspect
import types
import typing as typ
from pathlib import Path
from uuid import UUID

import datajoint as dj
from numpy import bool_, complex_, float_, int_, ndarray, str_
from numpy.typing import NDArray

# Misc. typing -------------------------------------------------------------------------
MapObj: typ.TypeAlias = typ.Mapping[str, object]
MutMapObj: typ.TypeAlias = typ.MutableMapping[str, object]
DictObj: typ.TypeAlias = dict[str, object]
DictAny: typ.TypeAlias = dict[str, typ.Any]
FrameType: typ.TypeAlias = types.FrameType
FrameInfo: typ.TypeAlias = inspect.FrameInfo
FrameInfoList: typ.TypeAlias = list[FrameInfo]
FrameStack: typ.TypeAlias = FrameInfoList | FrameType
TimeStampable: typ.TypeAlias = datetime.datetime | datetime.date | datetime.timedelta
ContainsUUID: typ.TypeAlias = (
    str | UUID | typ.Mapping[str, str | UUID] | dj.user_tables.UserTable
)
ModuleType: typ.TypeAlias = types.ModuleType


def is_pathstr(obj: object) -> typ.TypeGuard[str | Path]:
    return isinstance(obj, (str, Path))


def is_frame(obj: object) -> typ.TypeGuard[FrameType]:
    """Determines whether and object is a frame from inspect.currentframe."""
    return isinstance(obj, types.FrameType)


def is_frame_info(obj: object) -> typ.TypeGuard[FrameInfo]:
    """Determines whether and object is a frame from inspect.stack list."""
    return isinstance(obj, inspect.FrameInfo)


def is_frame_info_list(obj: object) -> typ.TypeGuard[FrameInfoList]:
    """Determines whether and object is a stack list inspect.stack."""
    if not isinstance(obj, list):
        return False
    f: typ.Any
    for f in obj:
        if not is_frame_info(f):
            return False
    return True


def is_frame_stack(obj: object) -> typ.TypeGuard[FrameStack]:
    return is_frame(obj) or is_frame_info_list(obj)


def is_strmap(obj: object, allow_empty: bool = True) -> typ.TypeGuard[MapObj]:
    if not isinstance(obj, typ.Mapping):
        return False
    if len(typ.cast(typ.Sized, obj)) == 0:
        return allow_empty
    return all(
        isinstance(k, str)
        for k, _ in typ.cast(typ.ItemsView[typ.Any, typ.Any], obj.items())
    )


def is_uuid_str(obj: object) -> typ.TypeGuard[str | UUID]:
    if isinstance(obj, UUID):
        return True
    elif isinstance(obj, str):
        obj = obj.replace("urn:", "").replace("uuid:", "")
        obj = obj.strip("{}").replace("-", "")
        return len(obj) == 32
    else:
        return False


def is_timestampable(obj: object) -> typ.TypeGuard[TimeStampable]:
    return isinstance(obj, (datetime.datetime, datetime.date, datetime.timedelta))


# DataJoint typing ---------------------------------------------------------------------
T_Table = typ.TypeVar("T_Table", bound=dj.expression.QueryExpression)
T_UserTable = typ.TypeVar("T_UserTable", bound=dj.user_tables.UserTable)
BaseTable: typ.TypeAlias = dj.expression.QueryExpression
UserTable: typ.TypeAlias = dj.user_tables.UserTable
AnyTable: typ.TypeAlias = (
    dj.expression.QueryExpression
    | dj.table.Table
    | dj.user_tables.UserTable
    | dj.user_tables.TableMeta
    | dj.user_tables.Lookup
    | dj.user_tables.Manual
    | dj.user_tables.Imported
    | dj.user_tables.Computed
    | dj.user_tables.Part
)
HeadingProp: typ.TypeAlias = typ.Literal["names", "primary_key", "secondary_attributes"]
DBConnection: typ.TypeAlias = dj.connection.Connection
ContextLike: typ.TypeAlias = MapObj | FrameStack | types.ModuleType | str


class InsertOpts(typ.TypedDict, total=False):
    replace: bool
    skip_duplicates: bool
    ignore_extra_fields: bool
    allow_direct_insert: bool | None


class InsertCallArgs(typ.TypedDict):
    row: DictObj
    insert_opts: InsertOpts


def is_djtable(obj: object) -> typ.TypeGuard[dj.user_tables.UserTable]:
    return isinstance(obj, BaseTable) or (
        inspect.isclass(obj) and issubclass(obj, BaseTable)
    )


def is_parttable(obj: object) -> typ.TypeGuard[dj.user_tables.Part]:
    return inspect.isclass(obj) and issubclass(obj, dj.user_tables.Part)


# Numpy typing -------------------------------------------------------------------------
ArrayTypes: typ.TypeAlias = bool_ | complex_ | float_ | int_ | str_
VecTypes: typ.TypeAlias = bool | complex | float | int | str
NPArray: typ.TypeAlias = NDArray[ArrayTypes]
StrArray: typ.TypeAlias = NDArray[str_]
AnyArray: typ.TypeAlias = NDArray[typ.Any]
Vector: typ.TypeAlias = typ.Sequence[VecTypes]
ArrayOrVec: typ.TypeAlias = (
    NPArray
    | Vector
    | typ.Sequence[Vector]
    | typ.Sequence[typ.Sequence[Vector]]
    | typ.Sequence[typ.Sequence[typ.Sequence[Vector]]]
    | typ.Sequence[typ.Sequence[typ.Sequence[typ.Sequence[Vector]]]]
)


def is_ndarray(obj: object) -> typ.TypeGuard[AnyArray]:
    return isinstance(obj, ndarray)
