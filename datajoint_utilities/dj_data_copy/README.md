# Data copy/migration
DataJoint utility to copy data from DataJoint tables across different schemas (from the same or different database servers)

Identical table definition is required for this data copy utilities to work

# Usage

```
import datajoint as dj
from datajoint_utilities.dj_data_copy import db_migration
```

## Copy data on the same database server

```
source_schema = dj.create_virtual_module('schema', 'source_schema_name')
target_schema = dj.create_virtual_module('schema', 'target_schema_name')

# Copy the data on a per-table level

db_migration.migrate_table(source_schema.Session, target_schema.Session)

# Copy the data for the entire schema - copy all tables from this schema in topologically sorted order

db_migration.migrate_schema(source_schema, target_schema

```

## Copy data across different database servers

```
source_conn = dj.connection.Connection('source_host', 'username', 'password', host_input='')
target_conn = dj.connection.Connection('target_host', 'username', 'password', host_input='')

source_schema = dj.create_virtual_module('schema', 'source_schema_name', connection=source_conn)
target_schema = dj.create_virtual_module('schema', 'target_schema_name', connection=target_conn)

# Copy the data on a per-table level

db_migration.migrate_table(source_schema.Session, target_schema.Session)

# Copy the data for the entire schema - copy all tables from this schema in topologically sorted order

db_migration.migrate_schema(source_schema, target_schema

```