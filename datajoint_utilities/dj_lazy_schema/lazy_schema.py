import datajoint_utilities.typing as djt  # isort: skip

import inspect
from pathlib import Path
from typing import Any, Mapping, NoReturn

import datajoint as dj
import datajoint_utilities.generic.typed as gt


def _src_file_relative_to_pkg(
    source: djt.FrameStack, package: djt.StrNone = None
) -> gt.SourceFileInfo | None:
    src_file = gt.filepath_from_frame(source)
    if not (src_file and src_file.is_absolute()):
        return None
    pkg_name = package or gt.pkg_name_from_frame(source)
    src_file_pkg_relpath = gt.pkg_relpath(src_file, package_import_name=pkg_name)
    if not src_file_pkg_relpath:
        return None
    return gt.SourceFileInfo(src_file, pkg_name, src_file_pkg_relpath)


def make_lazy_schema_name(
    source: djt.FrameStack | None,
    package: djt.StrNone = None,
    *,
    start_dirname: djt.StrPathNone = None,
) -> djt.StrNone:
    src = _src_file_relative_to_pkg(source or inspect.stack(), package)
    if src is None:
        return None
    pkg_relparts = src.pkg_relpath.parts if src.pkg_relpath else (src.file.name,)
    if start_dirname is None and "pipeline" in pkg_relparts:
        start_dirname = "pipeline"
    if src.pkg_relpath and start_dirname:
        root_schema_idx = [i for i, x in enumerate(pkg_relparts) if x == start_dirname]
        if root_schema_idx and len(pkg_relparts) > 1:
            pkg_relparts = tuple(pkg_relparts[root_schema_idx[0] + 1 :])
    schema_suffix = gt.chain_path_parts(Path(*pkg_relparts), sep="_")
    to_append = {"name": schema_suffix}
    if src.pkg_name:
        to_append["fallback"] = src.pkg_name
    return gt.append_to_prefix(**to_append)


class LazySchema(dj.Schema):
    def __init__(
        self,
        schema_name: djt.StrNone = None,
        context: djt.ContextLike | None = None,
        *,
        connection: djt.DBConnection | None = None,
        create_schema: bool = True,
        create_tables: bool = True,
        add_objects: djt.ContextLike | None = None,
        **kwargs: Any,
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
            kwargs.pop("source", None) or inspect.currentframe(), **kwargs
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
            return gt.calling_frame_locals(context)
        if isinstance(context, (str, djt.ModuleType)):
            return gt.get_module_objects(context)
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
