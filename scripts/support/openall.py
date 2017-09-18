import csv
import os.path
import pprint
import time
import traceback

from cate.core.ds import DATA_STORE_REGISTRY
from cate.ds.esa_cci_odp import ESA_CCI_ODP_DATA_STORE_ID
from cate.ops.io import open_dataset
from util import ConsoleMonitor


def get_data_store_ids():
    data_store = DATA_STORE_REGISTRY.get_data_store(ESA_CCI_ODP_DATA_STORE_ID)
    data_sources = data_store.query()
    return data_sources


def test_data_source(data_source, out_dir, result):
    # pprint.pprint(data_source.meta_info)
    data_source_id = data_source.id

    dataset = None
    t0 = time.clock()
    try:
        dataset = open_dataset(data_source_id,
                               force_local=True,
                               monitor=ConsoleMonitor(stay_in_line=True,
                                                      progress_bar_size=60))
        t1 = time.clock()
        result.update(dict(result='OK', time=t1 - t0))
    except Exception as e:
        t1 = time.clock()
        result.update(dict(result='Error', time=t1 - t0, details=str(e)))
        with open(os.path.join(out_dir, 'Error-%s.txt' % data_source_id), 'w') as fp:
            traceback.print_exc(file=fp)
    finally:
        if dataset:
            dataset.close()


def main():
    # pprint.pprint(data_sources)
    # exit()

    out_dir = 'out'
    os.makedirs(out_dir, exist_ok=True)

    field_names = ['id', 'result', 'time', 'details']

    t0 = time.clock()

    with open(os.path.join(out_dir, 'test-results.csv'), 'w', newline='') as results_file:
        writer = csv.DictWriter(results_file, field_names, delimiter=';')
        writer.writeheader()
        data_sources = get_data_store_ids()
        for data_source in data_sources:
            print('Testing %s...' % data_source.id)
            result = {key: '' for key in field_names}
            result['id'] = data_source.id
            test_data_source(data_source, out_dir, result)
            writer.writerow(result)
            pprint.pprint(result)

    t1 = time.clock()
    print('%s tests performed after %s secs' % (len(data_sources), t1 - t0))


if __name__ == '__main__':
    main()
