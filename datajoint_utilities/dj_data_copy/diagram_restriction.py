import datajoint as dj
import re
import pathlib


def find_part_table_ancestors(table, ancestors={}, verbose=False):
    """
    A recursive search routine to find all ancestors of the part tables for the given "table"
    """

    free_table = dj.FreeTable(dj.conn(), table.full_table_name)
    ancestors[table.full_table_name] = free_table
    for part_table in free_table.parts(as_objects=True):
        ancestors_diagram = dj.Diagram(part_table) - 999
        for full_table_name in ancestors_diagram.topological_sort():
            if full_table_name.isdigit():
                continue
            if full_table_name == part_table.full_table_name or full_table_name in ancestors:
                continue
            if verbose:
                print(f'\t\tstep into {full_table_name}')
            free_tbl = dj.FreeTable(dj.conn(), full_table_name)
            ancestors = find_part_table_ancestors(free_tbl, ancestors=ancestors)
        ancestors[part_table.full_table_name] = part_table
    # unite master-part below the parts' ancestors
    ancestors[table.full_table_name] = free_table
    ancestors.update({p.full_table_name: p for p in free_table.parts(as_objects=True)})
    return ancestors


def get_restricted_diagram_tables(restriction_tables,
                                  schema_allow_list=None,
                                  schema_block_list=None,
                                  ancestors_only=False,
                                  verbose=False):
    """
    Search the full pipeline diagram to find the set of ancestor and descendant
        tables for the given "restriction_tables"
    :param restriction_tables: list of datajoint tables to restrict the diagram
    :param schema_allow_list: list of schema names to allow in the search
    :param schema_block_list: list of schema names to ignore in the search
    :param ancestors_only: bool - search for ancestors only
    :param verbose:
    :return: dictionary of all ancestor and descendant tables
    """

    diagram = None
    for restriction_table in restriction_tables:
        try:
            restriction_table().definition
        except NotImplementedError:
            raise NotImplementedError('Unable to handle virtual table')

        if diagram is None:
            diagram = dj.Diagram(restriction_table)
        else:
            diagram += dj.Diagram(restriction_table)

    ancestors, descendants = {}, {}

    # walk up to search for all ancestors
    ancestors_diagram = diagram - 999
    for ancestor_table_name in ancestors_diagram.topological_sort():
        if ancestor_table_name.isdigit():
            continue
        if verbose:
            print(f'\tstep into to {ancestor_table_name}')
        ancestor_table = dj.FreeTable(dj.conn(), ancestor_table_name)

        # check for allow-list and block-list
        if (schema_allow_list and ancestor_table.database not in schema_allow_list)\
                or (schema_block_list and ancestor_table.database in schema_block_list):
            continue

        ancestors = find_part_table_ancestors(ancestor_table, ancestors, verbose=verbose)

    # walk down to find all descendants
    if not ancestors_only:
        descendants_diagram = diagram + 999
        for descendant_table_name in descendants_diagram.topological_sort():
            if descendant_table_name.isdigit():
                continue
            if verbose:
                print(f'\tstep into to {descendant_table_name}')
            descendant_table = dj.FreeTable(dj.conn(), descendant_table_name)

            # check for allow-list and block-list
            if (schema_allow_list and descendant_table.database not in schema_allow_list)\
                    or (schema_block_list and descendant_table.database in schema_block_list):
                continue

            descendants = find_part_table_ancestors(descendant_table, descendants, verbose=verbose)

    return {**ancestors, **descendants}


def generate_schemas_definition_code(sorted_tables, schema_prefix_update_mapper={},
                                     verbose=False, save_dir=None):
    """
    Generate a .py string containing the code to instantiate DataJoint tables
        from the given list of "sorted_tables", with schema names modification provided in
        "schema_prefix_update_mapper"

    :param sorted_tables: List - list of tables in topologically sorted order
    :param schema_prefix_update_mapper: Dict - mapper to update schema name, e.g.:
        schema_prefix_update_mapper = {'main_ephys': 'cloned_ephys',
                                       'main_analysis': 'clone_analysis'}
    :return: dict
    """
    table_names = [dj.utils.to_camel_case(t.split('.')[-1].strip('`')) for t in sorted_tables]

    sorted_schemas = {}
    sorted_schemas = list({t.split('.')[0].strip('`'): None for t in sorted_tables
                           if t.split('.')[0].strip('`') not in sorted_schemas})

    schemas_code = {}
    for schema_name in sorted_schemas:
        if verbose:
            print(f'\tProcessing {schema_name}')

        definition_str = 'import datajoint as dj\n\n\n'

        cloned_schema_name = schema_prefix_update_mapper.get(schema_name, schema_name)

        definition_str += f'# -------------- {cloned_schema_name} -------------- \n\n\n'

        schema_definition = dj.create_virtual_module(schema_name, schema_name).schema.save()

        schema_str = re.search(r'schema = .*', schema_definition).group()
        schema_str = schema_str.replace(schema_name, cloned_schema_name)
        definition_str += f'{schema_str}\n\n'

        # update schema names for virtual modules
        vmods_str = re.findall(r'vmod.*VirtualModule.*', schema_definition)
        for vmod_str in vmods_str:
            vmod_name = re.search(r"VirtualModule\('(\w+)', '(\w+)'\)", vmod_str).groups()[-1]
            vmod_str = vmod_str.replace(vmod_name, schema_prefix_update_mapper.get(vmod_name, vmod_name))
            definition_str += f'{vmod_str}\n'

        definition_str += '\n\n'

        # add table definitions
        tables_definition = [table_definition.replace('\n\n\n', '')
                             for table_definition in re.findall(r'@schema.*?\n\n\n', schema_definition, re.DOTALL)]
        tables_definition.append(re.search(r'.*(@schema.*$)', schema_definition, re.DOTALL).groups()[0])
        for table_definition in tables_definition:
            table_name = re.search(r'class\s(\w+)\((.+)\):', table_definition).groups()[0]
            if table_name in table_names:
                definition_str += f'{table_definition}\n\n\n'

        schemas_code[cloned_schema_name] = definition_str

    if save_dir:
        save_dir = pathlib.Path(save_dir)
        save_dir.mkdir(exist_ok=True, parents=True)
        for cloned_schema_name, schema_definition_str in schemas_code.items():
            with open(save_dir / f'{cloned_schema_name}.py', 'wt') as f:
                f.write(schema_definition_str)

    return schemas_code

