from datajoint_utilities.generic.pkgutil import detect_min_python_version

detect_min_python_version(3, 10)

import datetime
import inspect
import types
from pathlib import Path
from typing import (
    Any,
    ItemsView,
    Literal,
    Mapping,
    MutableMapping,
    Protocol,
    Sequence,
    Sized,
    TypeAlias,
    TypedDict,
    TypeGuard,
    TypeVar,
    cast,
    runtime_checkable,
)
from uuid import UUID

import datajoint as dj
from numpy import bool_, complex_, float_, int_, ndarray, str_
from numpy.typing import NDArray

# Misc. typing -------------------------------------------------------------------------
T = TypeVar("T")
DictObj: TypeAlias = dict[str, object]
DictAny: TypeAlias = dict[str, Any]
AnyMap: TypeAlias = Mapping[str, object]
AnyMutMap: TypeAlias = MutableMapping[str, object]
StrNone: TypeAlias = str | None
StrPath: TypeAlias = str | Path
StrPathNone: TypeAlias = str | Path | None
StrTuple: TypeAlias = tuple[str, ...]
StrPathParts: TypeAlias = str | Path | StrTuple
FrameType: TypeAlias = types.FrameType
FrameInfo: TypeAlias = inspect.FrameInfo
FrameInfoList: TypeAlias = list[FrameInfo]
FrameStack: TypeAlias = FrameInfoList | FrameType
FrameFile: TypeAlias = str | Path | FrameInfo
FrameFileNone: TypeAlias = str | Path | FrameInfo | None
AnyMapMapSeq: TypeAlias = AnyMap | Sequence[AnyMap]
AnyMapStrSeq: TypeAlias = AnyMap | Sequence[str] | str
TimeStampable: TypeAlias = datetime.datetime | datetime.date | datetime.timedelta
UUIDStr: TypeAlias = str | UUID
UUIDLike: TypeAlias = (
    str | UUID | MutableMapping[str, str | UUID] | dj.expression.QueryExpression
)
ModuleType: TypeAlias = types.ModuleType


def is_pathstr(obj: object) -> TypeGuard[StrPath]:
    return isinstance(obj, (Path, str))


def is_frame(obj: object) -> TypeGuard[FrameType]:
    """Determines whether and object is a frame from inspect.currentframe."""
    return isinstance(obj, types.FrameType)


def is_frame_info(obj: object) -> TypeGuard[FrameInfo]:
    """Determines whether and object is a frame from inspect.stack list."""
    return isinstance(obj, inspect.FrameInfo)


def is_frame_info_list(obj: object) -> TypeGuard[FrameInfoList]:
    """Determines whether and object is a stack list inspect.stack."""
    if not isinstance(obj, list):
        return False
    f: Any
    for f in obj:
        if not is_frame_info(f):
            return False
    return True


def is_frame_stack(obj: object) -> TypeGuard[FrameStack]:
    return is_frame(obj) or is_frame_info_list(obj)


def is_strmap(obj: object, allow_empty: bool = True) -> TypeGuard[AnyMap]:
    if not isinstance(obj, Mapping):
        return False
    if len(cast(Sized, obj)) == 0:
        return allow_empty
    return all(
        isinstance(k, str) and isinstance(v, object)
        for k, v in cast(ItemsView[Any, Any], obj.items())
    )


def is_uuid_str(obj: object) -> TypeGuard[UUIDStr]:
    if isinstance(obj, UUID):
        return True
    elif isinstance(obj, str):
        obj = obj.replace("urn:", "").replace("uuid:", "")
        obj = obj.strip("{}").replace("-", "")
        return len(obj) == 32
    else:
        return False


def is_timestampable(obj: object) -> TypeGuard[TimeStampable]:
    return isinstance(obj, (datetime.datetime, datetime.date, datetime.timedelta))


@runtime_checkable
class SupportsBool(Protocol):
    def __bool__(self) -> bool:
        ...


# DataJoint typing ---------------------------------------------------------------------
T_Table = TypeVar("T_Table", bound=dj.expression.QueryExpression)
T_UserTable = TypeVar("T_UserTable", bound=dj.user_tables.UserTable)
BaseTable: TypeAlias = dj.expression.QueryExpression
AnyTable: TypeAlias = (
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
HeadingProp: TypeAlias = Literal["names", "primary_key", "secondary_attributes"]
DBConnection: TypeAlias = dj.connection.Connection
ContextLike: TypeAlias = AnyMap | FrameStack | types.ModuleType | str


class InsertOpts(TypedDict, total=False):
    replace: bool
    skip_duplicates: bool
    ignore_extra_fields: bool
    allow_direct_insert: bool | None


class InsertCallArgs(TypedDict):
    row: DictObj
    insert_opts: InsertOpts


def is_djtable(obj: object) -> TypeGuard[dj.user_tables.UserTable]:
    return isinstance(obj, BaseTable) or (
        inspect.isclass(obj) and issubclass(obj, BaseTable)
    )


def is_parttable(obj: object) -> TypeGuard[dj.user_tables.Part]:
    return inspect.isclass(obj) and issubclass(obj, dj.user_tables.Part)


# Numpy typing -------------------------------------------------------------------------
ArrayTypes: TypeAlias = bool_ | complex_ | float_ | int_ | str_
VecTypes: TypeAlias = bool | complex | float | int | str
NPArray: TypeAlias = NDArray[ArrayTypes]
StrArray: TypeAlias = NDArray[str_]
AnyArray: TypeAlias = NDArray[Any]
Vector: TypeAlias = Sequence[VecTypes]
ArrayOrVec: TypeAlias = (
    NPArray
    | Vector
    | Sequence[Vector]
    | Sequence[Sequence[Vector]]
    | Sequence[Sequence[Sequence[Vector]]]
    | Sequence[Sequence[Sequence[Sequence[Vector]]]]
)


def is_ndarray(obj: object) -> TypeGuard[AnyArray]:
    return isinstance(obj, ndarray)
