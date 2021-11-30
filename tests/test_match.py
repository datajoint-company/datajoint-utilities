import pytest
from ruamel.yaml import YAML
from dj_search.search import DJMatch


@pytest.fixture
def sample_data(filepath='./tests/sample_data.yaml'):
    yaml = YAML()
    try:
        with open(filepath) as fp:
            sample_data = yaml.load(fp)
    except FileNotFoundError:
        sample_data = {}
    return sample_data


def test_match(sample_data):
    def verify(mask, definition, vms, level, expected):
        m = DJMatch(search_str=mask,
                    definition_string=definition,
                    virtual_modules=vms,
                    level=level,
                    method='_do_search2')
        # print('\n======================================' +
        #       ''.join([f'\n--------- {t} ---------\n' + m['pretty_definition']
        #                for t, m in m.matches.items()]))
        # verify specific tables are matched
        assert set(m.matches) == set(expected)
        # verify correct match per table
        assert all([len(v['matches']) == expected[t] for t, v in m.matches.items()])
    # ------ IBL, Allele
    mask = 'allele'
    sample_key = 'ibl_ingest'
    verify(mask=mask,
           definition=sample_data[sample_key]['definition'],
           vms=sample_data[sample_key]['virtual_modules'],
           level=None,
           expected={'ibl_ingest_subject.AlleleSequence': 5,
                     'ibl_ingest_subject.LineAllele': 3,
                     'ibl_ingest_subject.Allele': 6,
                     'ibl_ingest_subject.Zygosity': 2}
           )
    verify(mask=mask,
           definition=sample_data[sample_key]['definition'],
           vms=sample_data[sample_key]['virtual_modules'],
           level='table',
           expected={'ibl_ingest_subject.AlleleSequence': 1,
                     'ibl_ingest_subject.LineAllele': 1,
                     'ibl_ingest_subject.Allele': 1}
           )
    verify(mask=mask,
           definition=sample_data[sample_key]['definition'],
           vms=sample_data[sample_key]['virtual_modules'],
           level='comment',
           expected={'ibl_ingest_subject.AlleleSequence': 1,
                     'ibl_ingest_subject.Allele': 1,
                     'ibl_ingest_subject.Zygosity': 1}
           )
    verify(mask=mask,
           definition=sample_data[sample_key]['definition'],
           vms=sample_data[sample_key]['virtual_modules'],
           level='attribute',
           expected={'ibl_ingest_subject.AlleleSequence': 2,
                     'ibl_ingest_subject.LineAllele': 2,
                     'ibl_ingest_subject.Allele': 4,
                     'ibl_ingest_subject.Zygosity': 1}
           )
    # ------ STRF, Mov
    mask = 'Mov'
    sample_key = 'strf'
    verify(mask=mask,
           definition=sample_data[sample_key]['definition'],
           vms=sample_data[sample_key]['virtual_modules'],
           level=None,
           expected={'rguzman_STRF_pyPL.STRFCalcs': 1,
                     'rguzman_STRF_pyPL.Stimuli': 1,
                     'rguzman_STRF_pyPL.Movies': 5,
                     'rguzman_STRF_pyPL.MovieFiles': 3}
           )
    verify(mask=mask,
           definition=sample_data[sample_key]['definition'],
           vms=sample_data[sample_key]['virtual_modules'],
           level='table',
           expected={'rguzman_STRF_pyPL.Movies': 1,
                     'rguzman_STRF_pyPL.MovieFiles': 1}
           )
    verify(mask=mask,
           definition=sample_data[sample_key]['definition'],
           vms=sample_data[sample_key]['virtual_modules'],
           level='comment',
           expected={'rguzman_STRF_pyPL.Movies': 2,
                     'rguzman_STRF_pyPL.MovieFiles': 2}
           )
    verify(mask=mask,
           definition=sample_data[sample_key]['definition'],
           vms=sample_data[sample_key]['virtual_modules'],
           level='attribute',
           expected={'rguzman_STRF_pyPL.STRFCalcs': 1,
                     'rguzman_STRF_pyPL.Stimuli': 1,
                     'rguzman_STRF_pyPL.Movies': 2}
           )
    # ------ DJMON, mysql
    mask = 'mysql'
    sample_key = 'djmon'
    verify(mask=mask,
           definition=sample_data[sample_key]['definition'],
           vms=sample_data[sample_key]['virtual_modules'],
           level=None,
           expected={'datajoint_monitoring.MySQLProcessList': 3}
           )
    verify(mask=mask,
           definition=sample_data[sample_key]['definition'],
           vms=sample_data[sample_key]['virtual_modules'],
           level='table',
           expected={'datajoint_monitoring.MySQLProcessList': 2}
           )
    verify(mask=mask,
           definition=sample_data[sample_key]['definition'],
           vms=sample_data[sample_key]['virtual_modules'],
           level='comment',
           expected={}
           )
    verify(mask=mask,
           definition=sample_data[sample_key]['definition'],
           vms=sample_data[sample_key]['virtual_modules'],
           level='attribute',
           expected={'datajoint_monitoring.MySQLProcessList': 1}
           )
    # ------ DJMON, host
    mask = 'host'
    sample_key = 'djmon'
    verify(mask=mask,
           definition=sample_data[sample_key]['definition'],
           vms=sample_data[sample_key]['virtual_modules'],
           level=None,
           expected={'datajoint_monitoring.Host': 2,
                     'datajoint_monitoring.MySQLProcessList': 2,
                     'datajoint_monitoring.HostMetric': 4}
           )
    verify(mask=mask,
           definition=sample_data[sample_key]['definition'],
           vms=sample_data[sample_key]['virtual_modules'],
           level='table',
           expected={'datajoint_monitoring.Host': 1,
                     'datajoint_monitoring.HostMetric': 2}
           )
    verify(mask=mask,
           definition=sample_data[sample_key]['definition'],
           vms=sample_data[sample_key]['virtual_modules'],
           level='comment',
           expected={}
           )
    verify(mask=mask,
           definition=sample_data[sample_key]['definition'],
           vms=sample_data[sample_key]['virtual_modules'],
           level='attribute',
           expected={'datajoint_monitoring.Host': 1,
                     'datajoint_monitoring.MySQLProcessList': 2,
                     'datajoint_monitoring.HostMetric': 2}
           )
    # ------ DJMON, mpl
    mask = 'mpl'
    sample_key = 'djmon'
    verify(mask=mask,
           definition=sample_data[sample_key]['definition'],
           vms=sample_data[sample_key]['virtual_modules'],
           level=None,
           expected={'datajoint_monitoring.MySQLProcessList': 11}
           )
    verify(mask=mask,
           definition=sample_data[sample_key]['definition'],
           vms=sample_data[sample_key]['virtual_modules'],
           level='table',
           expected={}
           )
    verify(mask=mask,
           definition=sample_data[sample_key]['definition'],
           vms=sample_data[sample_key]['virtual_modules'],
           level='comment',
           expected={}
           )
    verify(mask=mask,
           definition=sample_data[sample_key]['definition'],
           vms=sample_data[sample_key]['virtual_modules'],
           level='attribute',
           expected={'datajoint_monitoring.MySQLProcessList': 11}
           )
