import datajoint as dj
import re
import inspect
from termcolor import colored


class DJSearch:

    def __init__(self, db_prefixes=[''], context=None):
        db_prefixes = [db_prefixes] if isinstance(db_prefixes, str) else db_prefixes
        self.context = context or inspect.currentframe().f_back.f_locals
        self.virtual_modules = {}

        self.schema_names = set(s for s in iter(dj.list_schemas())
                                if any(p in s for p in db_prefixes))

        tables_definitions = []
        for schema_name in self.schema_names:  # add progress bar
            self.virtual_modules[schema_name] = dj.create_virtual_module(schema_name,
                                                                         schema_name)
            schema_definition = self.virtual_modules[schema_name].schema.save()
            schema_definition = re.sub(r'(@)?schema(.*)\n', fr'\g<1>{schema_name}\g<2>\n',
                                       schema_definition)[len(schema_name) + 93:]

            for match in re.finditer(r"VirtualModule\('(\w+)', '(\w+)'\)", schema_definition):
                vmod, vmod_rep = match.groups()
                schema_definition = schema_definition.replace(vmod, vmod_rep)

            tables_definitions.append('\n\n\n' + schema_definition)

        # join definitions from all schema, remove INDEX and UNIQUE lines
        defi = r'\n'.join(tables_definitions)
        defi = re.sub(r'\s+?INDEX.+?\n|\s+?UNIQUE.+?\n', '', defi, flags=re.MULTILINE)
        defi = re.sub(r'([\(\)\[\]\w])( *)"""', r'\g<1>\n\g<2>"""', defi)

        self.definition_string = defi

    def search(self, search_str, level=None, method='_do_search'):
        """
        :param search_str: string to search for
        :param level: 'table', 'attribute', 'comment'
        :return:
        """
        if level is not None and level not in ('table', 'attribute', 'comment'):
            raise ValueError('Argument "level" must be ("table", "attribute", "comment")')
        m = DJMatch(search_str, self.definition_string, self.virtual_modules, level=level,
                    method=method)
        m.print()
        return m


class DJMatch:

    def __init__(self, search_str, definition_string, virtual_modules, level=None,
                 method='_do_search'):
        self.search_str = search_str
        self._definition_string = definition_string
        self._virtual_modules = virtual_modules
        self.matches = {}
        getattr(self, method)(level)

    def _do_search(self, level=None):
        for match in re.finditer(r' *(class\s\w*?)?({})'.format(self.search_str),
                                 self._definition_string, re.I):
            is_class = bool(match.groups()[0])

            if level == 'table':
                if not is_class:
                    continue
                else:
                    # safeguard against partial class_name match - e.g. "Unit" in "UnitSpikes"
                    if re.match(r'(\w+)\(', self._definition_string[match.span(2)[-1]:]):
                        continue

            # extract the whole line this matched string is on
            # from the last "\n" right before the match to the first "\n" right after
            for line_start in re.finditer(r'\n', self._definition_string[:match.span()[-1]]):
                pass
            line_end = re.search(r'\n', self._definition_string[match.span()[-1]:])
            line = self._definition_string[line_start.span()[0]:line_end.span()[-1] +
                                           match.span()[-1]]

            if ('dj.VirtualModule' in line
                    or 'dj.Schema' in line
                    or line.strip() in [f'@{vm}' for vm in self._virtual_modules]):
                continue

            if is_class:
                is_attr, is_comment = False, False
            elif ':' in line and '#' not in line:
                is_attr, is_comment = True, False
            elif ':' not in line and '#' in line:
                is_attr, is_comment = False, True
            elif ':' in line and '#' in line:
                mstr_start = match.span(2)[0] - line_start.span()[0]
                if mstr_start > line.index('#'):
                    is_attr, is_comment = False, True
                elif mstr_start < line.index(':'):
                    is_attr, is_comment = True, False
            else:  # neither ':' nor '#' are present
                is_attr, is_comment = False, False

            if level == 'attribute' and (is_class or not is_attr):
                continue
            if level == 'comment' and (is_class or not is_comment):
                continue

            # extract the table this matched string belongs to
            # from the
            if is_class:
                class_start = match
            else:
                for class_start in re.finditer(r' *class\s(\w+)\((.+)\):',
                                               self._definition_string[:match.span()[-1]]):
                    pass
            # non-greedy search for the end of the class definition
            class_end = next(re.finditer('definition = """.*?"""' if is_class else '"""',
                                         self._definition_string[match.span()[-1]:],
                                         re.DOTALL))

            tbl_defi = self._definition_string[class_start.span()[0]:class_end.span()[-1] +
                                               match.span()[-1]]
            tbl_name, tbl_tier = re.search(r'class\s(\w+)\((.+)\):', tbl_defi).groups()

            # extract schema and master table - search from the beginning to the end of the
            # class-definition string containing the match
            for schema_match in re.finditer(r'@(\w+)\nclass\s(\w+)\((.+)\):',
                                            self._definition_string[:class_end.span()[-1] +
                                                                    match.span()[-1]]):
                pass
            schema_name, master_name, master_tier = schema_match.groups()

            if tbl_tier == 'dj.Part':
                master_prepend = '@{}\nclass {}({}):\n\n\t...\n\n'.format(schema_name,
                                                                          master_name,
                                                                          master_tier)
                key = '{}.{}.{}'.format(schema_name, master_name, tbl_name)
                try:
                    table = getattr(getattr(self._virtual_modules[schema_name], master_name), tbl_name)
                except KeyError as e:
                    raise Warning(f'{str(e)}\nSuspecting DataJoint incompatible table definition')
            else:
                master_prepend = '@{}\n'.format(schema_name)
                key = '{}.{}'.format(schema_name, tbl_name)
                try:
                    table = getattr(self._virtual_modules[schema_name], tbl_name)
                except KeyError as e:
                    raise Warning(f'{str(e)}\nSuspecting DataJoint incompatible table definition')

            tbl_defi = master_prepend + tbl_defi

            if key in self.matches:
                tbl_defi = self.matches[key]['definition']

            matched_str = match.groups()[1]

            color_shift = len(re.findall(r'\x1b\[31m{}\x1b\[0m'.format(self.search_str),
                                         tbl_defi, re.I)) * len(colored('', 'red'))
            tbl_defi = ''.join([tbl_defi[:match.span(2)[0] - class_start.span()[0] +
                                         color_shift + len(master_prepend)],
                                colored(matched_str, 'red'),
                                tbl_defi[match.span(2)[-1] - class_start.span()[0] +
                                         color_shift + len(master_prepend):]])

            if key in self.matches:
                self.matches[key]['definition'] = tbl_defi
            else:
                self.matches[key] = {'definition': tbl_defi, 'table': table, 'tier': tbl_tier}

    def _do_search2(self, level=None):
        # regex constants
        s = r'[\t ]*'        # horizontal whitespace
        w = r'\w*'           # words valid for variable names
        e = r'[\w()."\'=]*'  # valid python expression
        n = '\n\r\v\f'       # vertical whitespace
        a = fr'[^{n}]*'      # all characters except vertical whitespace
        # determine appropriate regex based on level
        if level is None:  # anywhere
            level_regex = self.search_str
        elif level == 'table':
            level_regex = fr'(?<=[{n}]){s}class{s}{w}{self.search_str}{w}{e}:{s}(?=[#{n}])'
        elif level == 'attribute':
            level_regex = (f'(?<=[{n}]){s}(->{s}{e}{self.search_str}|'
                           f'{w}{self.search_str}{w}{s}(={s}{e}{s})?:){a}(?=[#{n}])')
        elif level == 'comment':
            level_regex = f'#{a}{self.search_str}{a}(?=[{n}])'
        # split full definition into class blocks + iterate over those that match regex once
        for match_definition in [b[2:] if i == 2 else b
                                 for i, b in enumerate(self._definition_string.split('\n\n\n'))
                                 if (i > 1 and 'dj.Schema' not in b and
                                     next(re.finditer(level_regex, b, re.I), False))]:
            schema_name, table_name, table_tier = re.compile(
                fr'@(?P<schema_name>{w}){s}[{n}]'
                fr'{s}class{s}(?P<table_name>{w}){s}\((?P<table_tier>{e})\):{s}(?=[#{n}])'
                ).match(match_definition).groups()
            self.matches[f'{schema_name}.{table_name}'] = {
                'definition': match_definition, 'pretty_definition': match_definition,
                'schema_name': schema_name, 'table': table_name, 'tier': table_tier,
                'matches': [],
            }
            color_shift = 9  # shift for red color
            # iterate over matches within class block to store match and adjust color
            for i, match in enumerate([m for m in re.finditer(level_regex,
                                                              match_definition,
                                                              re.I)]):
                self.matches[f'{schema_name}.{table_name}']['matches'].append(match)
                L, R = match.span()
                # print(f'debug: {repr(match_definition[L+i*color_shift:R+i*color_shift])}')
                L, R = tuple(map(sum, zip((L, L), next(re.finditer(
                    self.search_str,
                    self.matches[f'{schema_name}.{table_name}']['pretty_definition'][
                        L+i*color_shift:R+i*color_shift],
                    re.I)).span())))
                self.matches[f'{schema_name}.{table_name}']['pretty_definition'] = (
                    self.matches[f'{schema_name}.{table_name}']['pretty_definition'][
                        :L+i*color_shift] +
                    colored(self.matches[f'{schema_name}.{table_name}']['pretty_definition'][
                        L+i*color_shift:R+i*color_shift], 'red') +
                    self.matches[f'{schema_name}.{table_name}']['pretty_definition'][
                        R+i*color_shift:])

    def print(self):
        if not self.matches:
            print('No match found!')
        else:
            try:
                matched_str = '\n-------------------------------------\n'.join(
                    [m['pretty_definition'] for m in self.matches.values()])
            except KeyError:
                matched_str = '\n-------------------------------------\n'.join(
                    [m['definition'] for m in self.matches.values()])
            print(matched_str)
