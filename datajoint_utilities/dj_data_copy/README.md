# Data copy/migration
Utility to copy data from DataJoint tables across different schemas (from the same or different database servers)

Identical table definition is required for this data copy utilities to work

## Usage

```
import datajoint as dj
from datajoint_utilities.dj_data_copy import db_migration
```

### Copy data on the same database server

```
source_schema = dj.create_virtual_module('schema', 'source_schema_name')
target_schema = dj.create_virtual_module('schema', 'target_schema_name')

# Copy the data on a per-table level

db_migration.migrate_table(source_schema.Session, target_schema.Session)

# Copy the data for the entire schema - copy all tables from this schema in topologically sorted order

db_migration.migrate_schema(source_schema, target_schema)

```

### Copy data across different database servers

```
source_conn = dj.connection.Connection('source_host', 'username', 'password', host_input='')
target_conn = dj.connection.Connection('target_host', 'username', 'password', host_input='')

source_schema = dj.create_virtual_module('schema', 'source_schema_name', connection=source_conn)
target_schema = dj.create_virtual_module('schema', 'target_schema_name', connection=target_conn)

# Copy the data on a per-table level

db_migration.migrate_table(source_schema.Session, target_schema.Session)

# Copy the data for the entire schema - copy all tables from this schema in topologically sorted order

db_migration.migrate_schema(source_schema, target_schema)
```

# Pipeline diagram restriction

Utility to retrieve all ancestors and descendants associated with a given set of DataJoint tables, including part-tables and their respective ancestors.

## Usage

```
import datajoint as dj
from datajoint_utilities.dj_data_copy.pipeline_cloning import get_restricted_diagram_tables
```

Retrieve all ancestors and descendants of the tables `subject.Subject` and `ephys.Unit`, excluding those from the schema named `pipeline_analysis`

```
from my_pipeline import subject, session, ephys, analysis


restriction_tables = [subject.Subject, ephys.Unit]

restricted_tables = get_restricted_diagram_tables(
    restriction_tables,
    schema_allow_list=None,
    schema_block_list=['pipeline_analysis'])

list(restricted_tables)
```


# Pipeline cloning

Utility to generate schema/table source code to instantiate a cloned DataJoint pipeline from a given set of existing DataJoint tables. 

An example use case is when you need to make a clone of a working pipeline, say to a different schema name(s), and would like to further specify only a subset of tables to be cloned.

It is particularly useful to use in conjunction with the ***diagram restriction*** utility above.


## Usage

```
import datajoint as dj
from datajoint_utilities.dj_data_copy.pipeline_cloning import get_restricted_diagram_tables, generate_schemas_definition_code
```

Retrieve all ancestors and descendants of the tables `subject.Subject` and `ephys.Unit`, excluding those from the schema named `pipeline_analysis`

Generate a python script with schema/table defintion for these retrieved tables, under different schema names. This can be used to instantiate a cloned version of the original pipeline.

```
from my_pipeline import subject, session, ephys, analysis


# retrieve diagram restricted tables
restriction_tables = [subject.Subject, ephys.Unit]

restricted_tables = get_restricted_diagram_tables(
    restriction_tables,
    schema_allow_list=None,
    schema_block_list=['pipeline_analysis'])

list(restricted_tables)

# generate source code to instantiate clone pipeline
sorted_tables = list(restricted_tables)
schema_name_mapper = {'pipeline_lab': 'cloned_lab',
                               'pipeline_session': 'cloned_session',
                               'pipeline_ephys': 'cloned_ephys'}

schemas_code, tables_def = generate_schemas_definition_code(sorted_tables, schema_name_mapper, save_dir='.')

print(schemas_code['cloned_ephys'])
```

Or using the class `ClonedPipeline` for all of the steps above

```
from datajoint_utilities.dj_data_copy.pipeline_cloning import ClonedPipeline

diagram = dj.Diagram(subject.Subject) + dj.Diagram(ephys.Unit)
schema_name_mapper = {'pipeline_lab': 'cloned_lab',
                      'pipeline_session': 'cloned_session',
                      'pipeline_ephys': 'cloned_ephys'}

cloned_pipeline = ClonedPipeline(diagram, schema_name_mapper, verbose=True)

cloned_pipeline.restricted_tables
cloned_pipeline.restricted_diagram
cloned_pipeline.code
cloned_pipeline.tables_definition

cloned_pipeline.save_code(save_dir='.')

cloned_pipeline.instantiate_pipeline()
```


As next steps, you can also use the data copy utility above to migrate some data from the original pipeline to the new cloned pipeline.


