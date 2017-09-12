import pywr
import sys
from pywr.core import Model, Input, Output, Link

import numpy as np
import pandas
from IPython.core.display import display
import time
pandas.set_option("precision", 3)


def get_dict(obj):
    if not  hasattr(obj,"__dict__"):
        return obj
    result = {}
    for key, val in obj.__dict__.items():

        if key.startswith("_"):
            continue
        if isinstance(val, list):
            element = []
            for item in val:
                element.append(get_dict(item))
        else:
            element = get_dict(obj.__dict__[key])
        result[key] = element
    return result


def load_model(file_name):
    model = Model.load(file_name)
    #start= pandas.to_datetime(model.timestepper.start)
    #end= pandas.to_datetime(model.timestepper.end)
    #timeStep= ((model.timestepper.delta))
    # check the model is OK
    model.check()
    model.run()
    start_date= pandas.to_datetime(model.timestepper.start)
    end_date= pandas.to_datetime(model.timestepper.end)
    timeStep=model.timestepper.delta
    with open(file_name+".csv", 'w') as file:
        file.write("start_date,"+str(start_date)+'\n')
        file.write("end_date," + str(end_date)+ '\n')
        file.write("timeStep," + str(timeStep) + '\n')
        file.write("rec_name, rec_type, res name, type, value/s\n")
        for record in model.recorders:
            #print ("===============================================>")
            print ("Rec ===>",get_dict(record))
            #print ("===============================================>")
            if hasattr(record, "csvfile"):
                '''
                if hasattr(record, 'node_names'):
                    line = record.name+ ",csvrecorder"
                    for node_name in record.node_names:
                        line = line + ',' + node_name
                    file.write(line)
                '''
                file.write(record.name+ ",csvrecorder,"+record.csvfile+"\n")
            elif hasattr(record, "h5file"):

                file.write(record.name + ",tablesrecorder," + record.h5file + "\n")
            else:
                if hasattr(record, 'node'):
                    file.write (record.name+',single_recorder,'+record.node.name)
                if hasattr(record, 'data'):
                    for items in record.data:
                        _type=items.__class__.__name__
                        if _type=="ndarray":
                            for item in items:
                                file.write ("," + str(item))
                        else:
                            file.write("," + str(items))
                file.write('\n')
                #scenario = 0
                #timestep = 0
    file.close()


if __name__ == '__main__':
    #filename = "mean_flow_recorder.json"
    args=sys.argv
    if len(args)>1:
        filename=str(args[1])
        print ("====>", filename)
        if filename != None:
            load_model(filename)
