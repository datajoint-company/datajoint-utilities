# dj-search
DataJoint utility to facilitate schema and text search on a DataJoint pipeline


# Usage

## Schema search 
```
from datajoint_utilities.dj_search import list_schemas_prefix, drop_schemas, list_drop_order

# list schemas with a given prefix
list_schemas_prefix('dbprefix1')

# list schemas in an order that they could be dropped, to avoid foreign key constraints
list_drop_order('dbprefix1')

# drop schemas with a given prefix. Dry run ordered listing is slow.
drop_schemas(prefix='dbprefix1', dry_run=<bool>, ordered=<bool>, force_drop=<bool>)
```

## Full search
```
from datajoint_utilities.dj_search import DJSearch

# limit the search scope to schemas with database prefixes of interest
djsearch = DJSearch(['dbprefix1_', 'dbprefix2_])

# several ways to do search:

# search all schemas
djmatch = djsearch.search('Scan')

# or search by table-name only
djmatch = djsearch.search('ScanInfo', level='table')

# or search in comment only
djmatch = djsearch.search('depth', level='comment')

# or search by attribute-name only
djmatch = djsearch.search('field', level='attribute')
```

***djmatch.matches*** returns a dictionary with:
+ key: <schema_name>.<table_name>
+ value: a dictionary with:
    + ***definition***: string of the table definition
    + ***table***: DJ table for this table
    + ***tier***: tier of this table

# Local Testing

- Create a `.env` file within a local copy of this repo with appropriate values based on your system. For instance:
  ```shell
  HOST_UID=1000
  PY_VER=3.8
  ```
- Run the test workflow by:
  ```shell
  docker-compose -f LNX-docker-compose.yaml up --build --exit-code-from app
  ```
- Note: Make sure to `docker-compose -f LNX-docker-compose.yaml down` between runs.
