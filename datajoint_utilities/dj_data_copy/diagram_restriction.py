import datajoint as dj
import re


def find_part_table_ancestors(table, ancestors={}, verbose=False):
    """
    A recursive search routine to find all ancestors of the part tables for the given "table"
    """

    free_table = dj.FreeTable(dj.conn(), table.full_table_name)
    ancestors[table.full_table_name] = free_table
    for part_table in free_table.parts(as_objects=True):
        ancestors_diagram = dj.Diagram(part_table) - 999
        ancestors[part_table.full_table_name] = part_table
        for full_table_name in ancestors_diagram.topological_sort():
            if full_table_name == part_table.full_table_name or full_table_name in ancestors:
                continue
            if verbose:
                print(f'\t...stepping to {full_table_name}')
            free_tbl = dj.FreeTable(dj.conn(), full_table_name)
            ancestors = find_part_table_ancestors(free_tbl, ancestors=ancestors)
    return ancestors


def get_restricted_diagram_tables(restriction_tables,
                                  schema_allow_list=None,
                                  schema_block_list=None,
                                  verbose=False):
    """
    Search the full pipeline diagram to find the set of ancestor and descendants
        tables of the given "restriction_tables"
    :param restriction_tables: list of datajoint tables to restrict the diagram
    :param schema_allow_list: list of schema names to allow in the search
    :param schema_block_list: list of schema names to ignore in the search
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

    # walk up to search for all ancestors
    ancestors_diagram = diagram - 999
    ancestors = {}
    for ancestor_table_name in ancestors_diagram.topological_sort():
        if verbose:
            print(f'\t...stepping to {ancestor_table_name}')
        ancestor_table = dj.FreeTable(dj.conn(), ancestor_table_name)

        # check for allow-list and block-list
        if (schema_allow_list and ancestor_table.database not in schema_allow_list)\
                or (schema_block_list and ancestor_table.database in schema_block_list):
            continue

        ancestors = find_part_table_ancestors(ancestor_table, ancestors)

        # ancestors[full_table_name] = free_table
        # for part_table in free_table.parts(as_objects=True):
        #     ancestors[part_table.full_table_name] = part_table

    # walk down to find all descendants
    descendants_diagram = diagram + 999
    descendants = {}
    for descendant_table_name in descendants_diagram.topological_sort():
        if verbose:
            print(f'\t...stepping to {descendant_table_name}')
        descendant_table = dj.FreeTable(dj.conn(), descendant_table_name)

        # check for allow-list and block-list
        if (schema_allow_list and descendant_table.database not in schema_allow_list)\
                or (schema_block_list and descendant_table.database in schema_block_list):
            continue

        descendants = find_part_table_ancestors(descendant_table, descendants)

    # descendants = {tbl.full_table_name: tbl
    #                for tbl in restriction_table.descendants(as_objects=True)
    #                if ((schema_allow_list and tbl.database in schema_allow_list)
    #                    and (schema_block_list and tbl.database not in schema_block_list))}

    return {**ancestors, **descendants}


def generate_schemas_definition_code(sorted_tables, schema_prefix_update_mapper={}):
    """
    Generate a .py string containing the code to instantiate DataJoint tables
        from the given list of "sorted_tables", with schema names modification provided in
        "schema_prefix_update_mapper"

    :param sorted_tables: List - list of tables in topologically sorted order
    :param schema_prefix_update_mapper: Dict - mapper to update schema name, e.g.:
        schema_prefix_update_mapper = {'main_ephys': 'cloned_ephys',
                                       'main_analysis': 'clone_analysis'}
    :return: str
    """
    table_names = [dj.utils.to_camel_case(t.split('.')[-1].strip('`')) for t in sorted_tables]

    # FIXME: confirm topological sort of schemas
    sorted_schemas = {}
    sorted_schemas = list({t.split('.')[0].strip('`'): None for t in sorted_tables
                      if t.split('.')[0].strip('`') not in sorted_schemas})

    definition_str = 'import datajoint as dj\n\n\n'
    for schema_name in sorted_schemas:
        definition_str += f'-------------- {schema_prefix_update_mapper.get(schema_name, schema_name)} -------------- \n\n\n'

        schema_definition = dj.create_virtual_module(schema_name, schema_name).schema.save()

        schema_str = re.search(r'schema = .*', schema_definition).group()
        schema_str = schema_str.replace(schema_name,
                                    schema_prefix_update_mapper.get(schema_name, schema_name))
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

    return definition_str
