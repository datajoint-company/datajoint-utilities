import datajoint as dj
from pymysql import IntegrityError, OperationalError

"""
Development helper functions.
- list_schemas_prefix: returns a list of schemas with a specific prefix
- drop_schemas: Cycles through schemas on a given prefix until all are dropped
- list_drop_order: Cycles though schemas with a given prefix. List schemas in an order
                   that they could be dropped, to avoid foreign key constraints
"""


def list_schemas_prefix(prefix):
    """Returns list of schemas with a specific prefix"""
    return [s for s in dj.list_schemas() if s.startswith(prefix)]


def list_drop_order(prefix):
    """Returns schema order from bottom-up"""
    schema_list = list_schemas_prefix(prefix=prefix)
    # schemas as dictionary of empty lists
    depends_on = {s: [] for s in schema_list}
    for schema in depends_on.keys():
        # make a list of foreign key references
        upstreams = [
            vmod.split("'")[-2]
            for vmod in dj.Schema(schema).code.split("\n")
            if "VirtualModule" in vmod
        ]
        for upstream in upstreams:
            # add schema to the list of schema dependents
            depends_on[upstream] = [*depends_on[upstream], schema]
    drop_list = []  # ordered list to drop
    while len(depends_on):
        drop_list += [k for k, v in depends_on.items() if not v]  # empty is dropable
        depends_on = {k: v for k, v in depends_on.items() if v}  # remove from dict
        for schema in depends_on.keys():  # remove dropable from other values
            # Filter out items already in drop list
            depends_on[schema] = [s for s in depends_on[schema] if s not in drop_list]

    return drop_list


def drop_schemas(prefix, dry_run=True, ordered=False, force_drop=False):
    """
    Cycles through schemas with specific prefix. If not dry_run, drops the schemas
    from the database. Saves time figuring out the correct order for dropping schemas.

    :param prefix: If None, uses dj.config prefix
    :param dry_run: Optional, default True. If True, prints list of schemas with prefix.
    :param ordered: Optional, default False. If True, takes time to decide drop order
                    before printing or printing & dropping. Slow process.
    :param force_drop: Optional, default False. Passed to `schema.drop()`.
                       If True, skips the standard confirmation prompt.
    """
    if not prefix:
        try:
            prefix = dj.config["custom"]["database.prefix"]
        except KeyError:
            raise NameError(
                'No prefix found in dj.config["custom"]'
                + '["database.prefix"]\n'
                + "Please add a prefix with drop_schemas(prefix=<prefix>)"
            )

    if ordered:
        schemas_with_prefix = list_drop_order(prefix)
    else:
        schemas_with_prefix = list_schemas_prefix(prefix)

    if dry_run:
        print("Ordered schemas:" if ordered else "Schemas:")
        print("\n\t".join([""] + schemas_with_prefix))  # indent names

    elif not dry_run:
        while schemas_with_prefix:
            recent_errs = ["\t"]
            n_schemas_initial = len(schemas_with_prefix)
            for schema_name in schemas_with_prefix:
                try:
                    dj.schema(schema_name).drop(force=force_drop)
                except (OperationalError, IntegrityError) as e:
                    recent_errs = [*recent_errs, str(e)]
                else:
                    schemas_with_prefix.remove(schema_name)
                    if force_drop:
                        print(schema_name)
            recent_errs_concat = "\n\t".join(recent_errs)
            assert n_schemas_initial != len(schemas_with_prefix), (
                f"Could not drop any of the following schemas:\n\t"
                + "\n\t".join(schemas_with_prefix)
                + f"\Recent errors:{recent_errs_concat}"
            )
