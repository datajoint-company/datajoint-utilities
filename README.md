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

# Releases

A release is an annotated git tag `vX.Y.Z` (semantic versioning). The tag message is the release
notes, and the package version is read from the tag at build time, so no file in the repository
carries the version.

To release from an up-to-date `main`:

```
git tag -a vX.Y.Z      # the editor opens; write the release notes as the tag message
git push origin vX.Y.Z
gh release create vX.Y.Z --notes-from-tag
```

Install a specific release with
`pip install git+https://github.com/datajoint-company/datajoint-utilities.git@vX.Y.Z`.
Notes for every release are on the [releases page](https://github.com/datajoint-company/datajoint-utilities/releases).

# Level of support

The tools/utilities here are meant as reference or example implementation for other DataJoint
users to explore, modify, adopt, etc.

We cannot guarantee:

- repository maintenance
- bugfix, issue resolution
- CI/CD
- documentation

Of course any form of contribution (e.g. pull requests) are greatly welcomed.
