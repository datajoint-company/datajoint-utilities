import datajoint as dj
from pymysql import  OperationalError

"""
Dev helper functions. Chris Brozdowski <CBroz@datajoint.com>
- list_schemas_prefix: returns a list of schemas with a specific prefix
- drop_schemas: Cycles through schemas on a given prefix until all are dropped
"""


def list_schemas_prefix(prefix):
    """ Returns list of schemas with a specific prefix """
    return [s for s in dj.list_schemas() if s.startswith(prefix)]


def drop_schemas(prefix=None, dry_run=True, force_drop=False, max_attempts=10):
    """
    Cycles through schemas with specific prefix. If not dry_run, drops the schemas 
    from the database. Saves time figuring out the correct order for dropping schemas.

    :param prefix: Optional. If not specified, uses dj.config prefix
    :param dry_run: Optional, default True. If True, returns list would attempt to drop
    :param force_drop: Optional, default False. Passed to `schema.drop()`. 
                       If True, skips the standard confirmation prompt.
    :param max_attempts: Optional, default 10. Max attempts to drop relevant schemas
    """
    if not prefix:
        try:
            prefix = dj.config["custom"]["database.prefix"]
        except KeyError:
            raise NameError("No prefix found in dj.config[\"custom\"]"
                           + "[\"database.prefix\"]\n"
                           + "Please add a prefix with drop_schemas(prefix=<prefix>)")

    schemas_with_prefix = list_schemas_prefix(prefix)

    if dry_run:
        print("\n".join(schemas_with_prefix))

    else:
        while schemas_with_prefix and max_attempts > 0:
            for schema_name in schemas_with_prefix:
                try:
                    dj.schema(schema_name).drop(force=force_drop)
                except OperationalError:
                    pass
                else:
                    schemas_with_prefix.remove(schema_name)
                    print(schema_name)
            max_attempts -= 1
