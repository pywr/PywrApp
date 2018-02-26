import pandas as pd
import  tables
from numpy import array


def writr_data_to_csv(data, url):
    data.df.to_csv(url)


def writr_data_to_hdf(data, attribute, url):
    data.to_hdf(url, attribute)

def write_data_to_excel(data, url, attribute=None):
    if attribute is not None:
        data.to_excel('foo.xlsx', sheet_name=attribute)
    else:
        data.to_excel('foo.xlsx')


def write_tablesarray_to_hdf(url, root, dataset_name, data):
    print "================>", url
    if '/' in root:
        root = root.replace('/', '')
    h5file = tables.open_file(url, mode="w", title="data file")
    group = h5file.create_group("/", root, 'root information')
    data = array(data)
    h5file.create_array(group, dataset_name, data, "data selection")
    h5file.close()