# `dj_lazy_schema`

> **Note**: This subdirectory is fully typed using python inline typing. It requires `python>=3.10` for some of the functionality.

## Problem

Your package may be organized such that the directory tree corresponds to the schema names, or you may want to specify a schema name during initialization but not require activation.

`LazySchema` ([`datajoint_utilities.dj_lazy_schema.lazy_schema:LazySchema`](./lazy_schema.py)) modifies the DataJoint `Schema` class to always delay activation of a `Schema` instance, later using stored context information for activation as well as for automatically generating schema names.

## Usage

If the code snippet below is taken from a file located at `my_pkg/pipeline/sources/subjects.py` within the package `my_pkg`:

```python
import datajoint as dj
from datajoint_utilities.dj_lazy_schema import LazySchema

dj.config["custom"] = {"database.prefix": "my_db"}

# no schema name provided
schema = LazySchema() # -> schema_name = 'my_db_sources_subjects'
schema.is_activated()  # -> False

# schema name provided
another_schema = LazySchema("some_db") # -> schema_name = "some_db"
another_schema.is_activated()  # -> False

... # subjects tables

# activate can be in the same file or from another file
schema.activate()
another_schema.activate()
```

The schema suffix can be truncated to start at a subdirectory name other than `'pipeline'`:

```python
schema = LazySchema(start_dirname="sources") # -> schema_name = 'my_db_subjects'
```

## `__init__.py`

Shortcut to import the `LazySchema` class from `lazy_schema.py`.

## `lazy_schema.py`

`LazySchema` Overrides the default behavior of the "activation" functionality from the `datajoint.Schema` class. If the `schema_name` argument is left blank during initialization, it will try to use the `dj.config`'s database prefix, as well as the call stack to generate a default schema name based on the calling source file. Unlike the default behavior for `Schema`, activation will never happen during initialization, even if a schema name is provided. The method `activate` must be called at least once at a later point in the pipeline.
