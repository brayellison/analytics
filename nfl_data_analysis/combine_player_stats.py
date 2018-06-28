import re
from pickle import load
#import pandas as pd
import numpy as np
from tqdm import tqdm
from itertools import repeat
from pickle import dumps
from joblib import Parallel, delayed
from multiprocessing import current_process

def main(table_types, data):
    process_num = current_process().name
    appended = dict(zip(table_types, repeat(None)))
    failed = []
    desc = f'{process_num}\u2013Appending player tables:'.ljust(40, '.')
    for name, name_dict in tqdm(data, desc=desc):
        for key, val in name_dict.items():
            try:
                if isinstance(appended[key], type(None)):
                    appended[key] = val
                else:
                    appended[key] = appended[key].append(val)
            except:
                failed.append((name, key, val))
    return appended, failed

if __name__ == '__main__':
    print('Loading player data.')
    open_path = '/home/bray/Documents/nfl_data_analysis/player_data_dict.pkl'
    with open(open_path, 'rb') as f:
        data = load(f)

    table_types = set()

    for name in data:
        table_types.update(set(data[name].keys()))
        for key in data[name]:
            data[name][key]['name'] = name
    print('Creating splits.')
    data_splits = np.array_split(list(data.items()), 8)
    print('Beginning parallel processsing...\n\n')
    parallel = Parallel(n_jobs=8)
    returned = parallel(delayed(main)(x) for x in data_splits)

    appended = dict(zip(table_types, repeat(None)))
    failed = []

    desc = 'Appending returned results:'.ljust(30, '.')
    for a, f in returned:
        for key, val in a.items():
            if isinstance(appended[key], type(None)):
                appended[key] = val
            else:
                appended[key] = appended[key].append(val)
        failed.extend(f)


    out_path = '/home/bray/Documents/nfl_data_analysis/player_data_dict_appended-2.pkl'
    with open(out_path, 'wb') as f:
        f.write(dumps(appended))

    out_path = '/home/bray/Documents/nfl_data_analysis/player_data_dict_failed-2.pkl'
    with open(out_path, 'wb') as f:
        f.write(dumps(failed))


    