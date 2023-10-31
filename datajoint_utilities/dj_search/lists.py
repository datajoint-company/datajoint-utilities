import datajoint as dj
from pymysql import IntegrityError, OperationalError

"""
Development helper functions.
- _list_schemas_prefix: returns a list of schemas with a specific prefix
- drop_schemas: Cycles through schemas on a given prefix until all are dropped
- list_drop_order: Cycles though schemas with a given prefix. List schemas in an order
                   that they could be dropped, to avoid foreign key constraints
"""

BUILT_INS = ["information_schema", "performance_schema", "sys", "mysql"]


def _list_schemas_prefix(prefix=None, connection=None):
    """Returns list of schemas with a given prefix. Helper function, Not for common use.

    Parameters
    ----------
    prefix: when retruning a list of schemas, restrict by this prefix
    connection: Optional parameter passed to datajoint.list_schemas
    """
    if prefix is None:
        prefix = ""
    return [
        s
        for s in dj.list_schemas(connection=connection)
        if s.startswith(prefix) and s not in BUILT_INS
    ]


def list_drop_order(prefix):
    """Returns schema order from bottom-up"""
    schema_list = _list_schemas_prefix(prefix=prefix)
    # schemas as dictionary of empty lists
    depends_on = {s: [] for s in schema_list}
    for schema in depends_on:
        # make a list of foreign key references
        upstreams = [
            vmod.split("'")[-2]
            for vmod in dj.Schema(schema).code.split("\n")
            if "VirtualModule" in vmod
        ]
        for upstream in upstreams:
            # add schema to the list of schema dependents
            depends_on[upstream].append(schema)
    drop_list = []  # ordered list to drop
    while depends_on:
        drop_list.extend(k for k, v in depends_on.items() if not v)  # empty is dropable
        depends_on = {k: v for k, v in depends_on.items() if v}  # remove from dict
        for schema in depends_on:
            # Filter out items already in drop list
            depends_on[schema] = [s for s in depends_on[schema] if s not in drop_list]

    return drop_list


def list_tables(
    schema_prefix="",
    table_prefix="",
    ordered=False,
    attribute=None,
    connection=None,
):
    """Return list of full table names given schema/table prefixes.

    :param schema_prefix: Optional, default all schemas.
    :param table_prefix: Optional, default all tables.
    :param ordered: Optional, default False. If True, returns in schema drop
        order to approx dependency order.
    :param attribute: Optional, default no restriction. Optionally filters
        tables by those that include the attribute.
    """
    if connection is None:
        connection = dj.conn()

    if ordered:
        schemas_with_prefix = list_drop_order(schema_prefix)
    else:
        schemas_with_prefix = _list_schemas_prefix(schema_prefix)

    table_list = [
        f"`{s}`.`{t}`"
        for s in schemas_with_prefix
        for t in dj.Schema(s).list_tables()
        if t.startswith(table_prefix)
    ]

    if attribute:
        table_list = [
            t
            for t in table_list
            if attribute in dj.FreeTable(connection, t).heading.attributes
        ]

    return table_list


def drop_schemas(prefix=None, dry_run=True, ordered=False, force_drop=False):
    """
    Cycles through schemas with specific prefix. If not dry_run, drops the schemas
    from the database. Saves time figuring out the correct order for dropping schemas.

    :param prefix: If None, uses dj.config prefix. For all, use prefix=''.
    :param dry_run: Optional, default True. If True, prints list of schemas with prefix.
    :param ordered: Optional, default False. If True, takes time to decide drop order
                    before printing or printing & dropping. Slow process.
    :param force_drop: Optional, default False. Passed to `schema.drop()`.
                       If True, skips the standard confirmation prompt.

    Example:
        from datajoint_utilities.dj_search import drop_schemas;
        prefix='lfp_mer';
        drop_schemas(prefix, force_drop=True, dry_run=False)
    """
    if prefix is None:
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
        schemas_with_prefix = _list_schemas_prefix(prefix)

    if dry_run:
        print("Ordered schemas:" if ordered else "Schemas:")
        print("\n\t".join([""] + schemas_with_prefix))  # indent names

    elif not dry_run:
        while schemas_with_prefix:
            recent_errs = []  # Refresh recent_errs at start of while loop
            n_schemas_initial = len(schemas_with_prefix)
            for schema_name in schemas_with_prefix:
                try:
                    dj.schema(schema_name).drop(force=force_drop)
                except (OperationalError, IntegrityError) as e:
                    recent_errs.append(str(e))  # Add to list for current loop
                else:
                    schemas_with_prefix.remove(schema_name)
                    if force_drop:
                        print(schema_name)
            assert n_schemas_initial != len(schemas_with_prefix), (
                f"Could not drop any of the following schemas:\n\t"
                + "\n\t".join(schemas_with_prefix)
                + f"\Recent errors:\n\t"
                + "\n\t".join(recent_errs)
            )
