import os
import pandas as pd
from HydraLib.hydra_dateutil import get_datetime
import datetime
import tables
from numpy import array


from HydraLib.HydraException import HydraPluginError

def check_file(url):
    if os.path.exists(url) == False:
        raise HydraPluginError("File: " + url + ' is not found !!!')

def get_the_res_data(url, attribute, res, data):
    if res==None:
        res='Data'
    if res in data.columns:
        ss=[]
        values={}
        for k in data[res].to_dict():
            ss.append(str(k)+','+str(data[res].to_dict()[k]))
            values[str(get_datetime(k))]=str(data[res].to_dict()[k])
        return values
    else:
        error = attribute + ' for resource: ' + res + ' is not found in the file: ' + url
        raise HydraPluginError(error)


def read_data_file_column(url, attribute, res, sheetname, index_=0):
    extension = os.path.splitext(url)[1][1:]
    if(extension.lower().strip()=='csv'):
        return read_csv_file_column(url, attribute, res, index_)
    elif (extension.lower().strip() == 'xlsx' or extension.lower().strip() == 'xls'):
        return read_excel_file_column(url, attribute, res,sheetname, index_)
    elif (extension.lower().strip() == 'h5'):
        return read_hdf_file_column(url, attribute, res, index_)
    else:
        raise HydraPluginError("Reading file: " + url + ' is not supported !!!')

def read_hdf_file_column(url, attribute, res, index_=0):
    check_file(url)
    try:
        data = pd.read_hdf(url, attribute,parse_dates=False, index_col=index_)
    except:
        data = pd.read_hdf(url,parse_dates=False)

    return get_the_res_data(url, attribute, res, data)

def read_csv_file_column(url, attribute, res, index_=0):
    check_file(url)
    try:
        data=pd.read_csv(url, index_col=index_,parse_dates=False )
    except:
        raise HydraPluginError("Error while reading: "+url+' to get attribute: '+attribute)
    return  get_the_res_data(url, attribute, res, data)

def read_excel_file_column(url, attribute, res, sheetname, index_=0):
    check_file(url)
    try:
        if sheetname!=None:
            data = pd.read_excel(url, sheetname=sheetname, index_col=index_,parse_dates=False )

        else:
            data = pd.read_excel(url, attribute, index_col=index_, parse_dates=False)

    except:
        data = pd.read_excel(url,index_col=index_ ,parse_dates=False)
        #raise HydraPluginError("Error while reading: "+url+' to get attribute: '+attribute)
    return get_the_res_data(url, attribute, res, data)


def get_h5DF_store (file_name):
    store=pd.HDFStore(file_name)
    return store

def get_node_attr_values(store, node_name, root=None):
    if root==None:
        data_ = store.get_node(node_name).read()
    else:
        tmp=root+'/'+node_name
        data_ = store.get_node(tmp).read()
    print "data_", data_
    try:
        if (data_!=None):
            return data_
        else:
             return None
    except:
        return data_

def read_tables_recoder (store, node_name, address=None):
    time=store.get_node('/time')
    data_ = store.get_node(node_name).read()
    if (time!=None):
        data = {}
        for i in range (0, len(time)):
            cur_time_step=time[i]
            date_=datetime.date(day=cur_time_step['day'],month=cur_time_step['month'],year=cur_time_step['year'])
            data[str(data)]=data_[i]
        return data
    else:
        error = 'Error whole reading table recorder'
        raise HydraPluginError(error)
