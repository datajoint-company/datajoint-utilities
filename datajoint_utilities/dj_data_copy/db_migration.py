import sys
import datajoint as dj
from tqdm import tqdm


"""
Utility for data copy/migration between schemas and tables
"""

_bypass_serialization = dj.blob.bypass_serialization


def migrate_schema(
    origin_schema,
    destination_schema,
    restriction={},
    table_block_list=[],
    allow_missing_destination_tables=True,
    force_fetch=False,
    batch_size=None
):
    """
    Data migration from all tables from `origin_schema` to `destination_schema`, in topologically sorted order

    :param origin_schema - schema to transfer the data from
    :param destination_schema - schema to transfer the data to
    :param restriction - DataJoint restriction to apply to the tables in origin_schema for restricted data transfer
    :param table_block_list - skip data transfer for these tables
    :param allow_missing_destination_tables - allow for missing tables in the destination_schema compared to the origin_schema
    :param force_fetch - bool - force the fetch and reinsert instead of server side transfer
    :param batch_size: int - do the data transfer in batch - with specified batch_size
        (if None - transfer all at once)
    """
    total_to_transfer_count = 0
    total_transferred_count = 0

    tbl_names = [
        tbl_name.split(".")[-1]
        for tbl_name in dj.Diagram(origin_schema).topological_sort()
    ]
    tbl_names = [
        ".".join(
            [dj.utils.to_camel_case(s) for s in tbl_name.strip("`").split("__") if s]
        )
        for tbl_name in tbl_names
    ]

    print(f"Data migration for schema: {origin_schema.schema.database}")

    for tbl_name in tbl_names:
        if (tbl_name in table_block_list) or ("." in tbl_name and tbl_name.split('.')[0] in table_block_list):
            continue

        orig_tbl = get_table(origin_schema, tbl_name)

        try:
            dest_tbl = get_table(destination_schema, tbl_name)
        except AttributeError as e:
            if allow_missing_destination_tables:
                continue
            else:
                raise e

        transferred_count, to_transfer_count = migrate_table(
            orig_tbl, dest_tbl, restriction=restriction, force_fetch=force_fetch, batch_size=batch_size
        )
        total_transferred_count += transferred_count
        total_to_transfer_count += to_transfer_count

    print(
        f"--- Total records migrated: {total_transferred_count}/{total_to_transfer_count} records ---"
    )
    return total_transferred_count, total_to_transfer_count


def migrate_table(orig_tbl, dest_tbl, restriction={}, force_fetch=True, batch_size=None):
    """
    Migrate data from `orig_tbl` to `dest_tbl`

    :param orig_tbl: datajoint table to copy data from
    :param dest_tbl: datajoint table to copy data to
    :param restriction - DataJoint restriction to apply to the orig_tbl for restricted data transfer
    :param force_fetch: bool - force the fetch and reinsert instead of server side transfer
    :param batch_size: int - do the data transfer in batch - with specified batch_size
        (if None - transfer all at once)
    """
    table_name = ".".join(
        [
            dj.utils.to_camel_case(s)
            for s in orig_tbl.table_name.strip("`").split("__")
            if s
        ]
    )
    print(f"\tData migration for table {table_name}: ", end="")

    # check if the transfer is between different database servers (different db connections)
    is_different_server = (
        orig_tbl.connection.conn_info["host"] != dest_tbl.connection.conn_info["host"]
    )

    # apply restriction
    orig_tbl &= restriction
    dest_tbl &= restriction

    # check if there's external datatype to be transferred
    has_external = any("@" in attr.type for attr in orig_tbl.heading.attributes.values())

    if is_different_server:
        records_to_transfer = (
            orig_tbl.proj() - (orig_tbl & dest_tbl.fetch("KEY")).proj()
        ).fetch('KEY')
    else:
        records_to_transfer = orig_tbl.proj() - dest_tbl.proj()

    must_fetch = has_external or is_different_server or force_fetch

    to_transfer_count = len(records_to_transfer)
    transferred_count = 0

    if to_transfer_count:
        dj.blob.bypass_serialization = True
        try:
            if batch_size is not None and must_fetch:
                for i in tqdm(range(0, to_transfer_count, batch_size), file=sys.stdout, desc=f'\tBatch migration for table {table_name}'):
                    entries = (orig_tbl & records_to_transfer).fetch(as_dict=True, offset=i, limit=batch_size)
                    dest_tbl.insert(entries, skip_duplicates=True, allow_direct_insert=True)
                    transferred_count += len(entries)
            else:
                entries = ((orig_tbl & records_to_transfer).fetch(as_dict=True)
                           if must_fetch
                           else (orig_tbl & records_to_transfer))
                dest_tbl.insert(entries, skip_duplicates=True, allow_direct_insert=True)
                transferred_count = to_transfer_count
        except dj.DataJointError as e:
            print(f'\n\tData copy error: {str(e)}')
        dj.blob.bypass_serialization = _bypass_serialization

    print(f"{transferred_count}/{to_transfer_count} records")
    return transferred_count, to_transfer_count


def get_table(pipeline_module, table_name):
    """
    Given a "pipeline_module" (e.g. from dj.VirtualModule) - return the DataJoint table with the name specified in "table_name" 

    :param pipeline_module: pipeline module to retrieve the table from (e.g. from dj.VirtualModule)
    :param table_name: name of the table (or part table), in PascalCase, e.g. `Session` or `Probe.Electrode`
    :return: DataJoint table from "pipeline_module" with "table_name"
    """

    if "." in table_name:
        master_name, part_name = table_name.split(".")
        return getattr(getattr(pipeline_module, master_name), part_name)
    else:
        return getattr(pipeline_module, table_name)