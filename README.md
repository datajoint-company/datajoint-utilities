# datajoint-utilities

A general purpose repository containing all generic tools/utilities surrounding the DataJoint ecosystem. These can be candidates for DataJoint plugins

# Utilities/Tools

- [dj_search](./datajoint_utilities/dj_search) - text search to explore DataJoint pipeline
- [dj_data_copy](./datajoint_utilities/dj_data_copy) - copy data across tables, schemas, database servers
- [dj_worker](./datajoint_utilities/dj_worker) - set up workers to operate a DataJoint pipeline - i.e. manage and monitor `.populate()` calls, orchestrate different workers, etc.
- [cmdline](./datajoint_utilities/cmdline) - general utilities for creating command-line interfaces.

# Installation

```
pip install git+https://github.com/datajoint-company/datajoint-utilities.git
```

# Level of support

The tools/utilities here are meant as reference or example implementation for other DataJoint
users to explore, modify, adopt, etc.

We cannot guarantee:

- repository maintenance
- bugfix, issue resolution
- CI/CD
- documentation

Of course any form of contribution (e.g. pull requests) are greatly welcomed.
