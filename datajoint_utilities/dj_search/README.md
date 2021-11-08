# dj-search
DataJoint utility to facilitate text search on a DataJoint pipeline

# Installation
Install using pip from GitHub repository:

    
    pip install git+https://github.com/datajoint/dj-search.git

# Usage

```
from dj_search import DJSearch

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
