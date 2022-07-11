import inspect
from pathlib import Path
from typing import Any, Mapping, NoReturn

import datajoint as dj
import datajoint_utilities.dj_lazy_schema.generic as lsg
import datajoint_utilities.dj_lazy_schema.typing as djt


def _src_file_relative_to_pkg(
    source: djt.FrameStack, package: djt.StrNone = None
) -> lsg.SourceFileInfo | None:
    src_file = lsg.filepath_from_frame(source)
    if not (src_file and src_file.is_absolute()):
        return None
    pkg_name = package or lsg.pkg_name_from_frame(source)
    src_file_pkg_relpath = lsg.pkg_relpath(src_file, package_import_name=pkg_name)
    if not src_file_pkg_relpath:
        return None
    return lsg.SourceFileInfo(src_file, pkg_name, src_file_pkg_relpath)


def make_lazy_schema_name(
    source: djt.FrameStack | None,
    package: djt.StrNone = None,
    *,
    schema_dirname_root: djt.StrPathNone = None,
) -> djt.StrNone:
    src = _src_file_relative_to_pkg(source or inspect.stack(), package)
    if src is None:
        return None
    pkg_relparts = src.pkg_relpath.parts if src.pkg_relpath else (src.file.name,)
    if src.pkg_relpath and schema_dirname_root:
        root_schema_idx = [
            i for i, x in enumerate(pkg_relparts) if x == schema_dirname_root
        ]
        if root_schema_idx and len(pkg_relparts) > 1:
            pkg_relparts = tuple(pkg_relparts[root_schema_idx[0] + 1 :])

    schema_suffix = lsg.chain_path_parts(Path(*pkg_relparts), sep="_")
    return lsg.append_to_prefix(schema_suffix)


class LazySchema(dj.Schema):
    """Always delay activation of a Schema instance using stored context info.

    Overrides the default behavior of the "activation" functionality from the
    `datajoint.Schema` class. If `schema_name` is left blank during initialization, this
    uses the config's database prefix and also the call stack to generate a default
    schema name based on the calling file. Unlike the default behavior for Schema,
    activation will never occur even if a schema name is provided during initialization.
    The method `activate` must be called at least once at a later point in the pipeline.
    """

    def __init__(
        self,
        schema_name: djt.StrNone = None,
        context: djt.ContextLike | None = None,
        *,
        connection: djt.DBConnection | None = None,
        create_schema: bool = True,
        create_tables: bool = True,
        add_objects: djt.ContextLike | None = None,
    ):
        super().__init__(
            schema_name=None,
            connection=connection,
            create_schema=create_schema,
            create_tables=create_tables,
        )
        self.context = context
        self.add_objects = add_objects
        self._lazy_schema_name = schema_name or make_lazy_schema_name(
            inspect.currentframe(), schema_dirname_root="pipeline"
        )

    def __call__(
        self, cls: type[djt.T_UserTable], *, context: djt.ContextLike | None = None
    ) -> type[djt.T_UserTable]:
        context = context or self.context or inspect.currentframe()
        if context is None:
            raise ValueError("No context provided.")
        if djt.is_parttable(cls):
            raise dj.errors.DataJointError(
                "The schema decorator should not be applied to Part relations"
            )
        if self.is_activated():
            self._decorate_master(cls, self._context_info(context))  # type: ignore
        else:
            self.declare_list.append((cls, context))  # type: ignore
        return cls

    def activate(
        self,
        schema_name: djt.StrNone = None,
        *,
        connection: djt.DBConnection | None = None,
        create_schema: bool | None = None,
        create_tables: bool | None = None,
        add_objects: djt.ContextLike | None = None,
    ) -> None:
        add_context = self._context_info(self.add_objects) or {}
        add_context |= self._context_info(add_objects) or {}
        if not self._is_active():
            self.declare_list: list[tuple[object, dict[str, Any]]] = [
                (cls, self._context_info(context) or {})
                for cls, context in self.declare_list
                if context is not None
            ]
        super().activate(  # type: ignore
            schema_name=schema_name or self._lazy_schema_name,
            connection=connection,
            create_schema=create_schema,
            create_tables=create_tables,
            add_objects=add_context,
        )

    def _context_info(
        self, context: djt.ContextLike | None
    ) -> dict[str, Any] | None | NoReturn:
        if context is None:
            return None
        if isinstance(context, Mapping):
            return dict(context)
        if djt.is_frame_stack(context):
            return lsg.calling_frame_locals(context)
        if isinstance(context, (str, djt.ModuleType)):
            return lsg.get_module_objects(context)
        raise TypeError(f"Invalid context type: {type(context).__name__}")

    def _is_active(self) -> bool:
        if not self.is_activated() or self.connection is None:  # type: ignore
            return False
        return bool(
            self.connection.query(  # type: ignore
                "SELECT schema_name FROM information_schema.schemata "
                f"WHERE schema_name = '{self.database}'"  # type: ignore
            ).rowcount
        )
