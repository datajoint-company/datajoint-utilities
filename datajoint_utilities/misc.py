import datajoint as dj
from pymysql import  OperationalError

"""
Misc helper functions to support dev. Chris Brozdowski <CBroz@datajoint.com>
- list_schemas_prefix: returns a list of schemas with a specific prefix
- drop_schemas: Cycles through schemas on a given prefix until all are dropped
"""


def list_schemas_prefix(prefix):
    """ Returns list of schemas with a specific prefix """
    return [s for s in dj.list_schemas() if s.startswith(prefix)]


def drop_schemas(prefix=None, dry_run=True):
    """
    Cycles through schemas with specific prefix. If not dry_run, drops the schemas from the database.
    Useful for dev, to save time figuring out the correct order in which to drop schemas

    :param prefix: Optional. If not specified, uses dj.config prefix
    :param dry_run: Optional. If True, returns list would attempt to drop
    """
    if not prefix:
        try:
            prefix = dj.config["custom"]["database.prefix"]
        except KeyError:
            raise NameError("Please specify a prefix with function or in dj.config")

    schemas_with_prefix = list_schemas_prefix(prefix)

    if dry_run:
        print("\n".join(schemas_with_prefix))

    else:
        while schemas_with_prefix:
            for i in schemas_with_prefix:
                try:
                    dj.schema(i).drop()
                    schemas_with_prefix.remove(i)
                    print(i)
                except OperationalError:
                    pass
