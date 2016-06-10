'''''
from pywr.core import Model, Input, Output
from pywr.recorders import NumpyArrayNodeRecorder
model = Model()
node = Input(model, 'supply')
recorder = NumpyArrayNodeRecorder(model, node)
node.recorders
print ("=============================================", (node.recorders[0]).data)
print(node.recorder[0].data[0])
'''


from pywr.core import Model, Input, Output
import pandas
# create a model (including an empty network)
model = Model()

# create two nodes: a supply, and a demand
supply = Input(model, name='supply')
demand = Output(model, name='demand')

# create a connection from the supply to the demand
supply.connect(demand)

# set maximum flows
supply.max_flow = 10.0
demand.max_flow = 6.0

# set cost (+ve) or benefit (-ve)
supply.cost = 3.0
demand.cost = -100.0

import datetime

from pywr.core import Timestepper

model.timestepper = Timestepper(
    pandas.to_datetime('2015-01-01'),  # first day
    pandas.to_datetime('2015-12-31'),  # last day
    datetime.timedelta(31)  # interval
)

from pywr.recorders import NumpyArrayNodeRecorder

recorder = NumpyArrayNodeRecorder(model, supply)

#supply.recorder =   NumpyArrayNodeRecorder(len(model.timestepper))
# lets get this party started!
model.run()
cou=len(supply.recorders[0].data)
print (len(supply.recorders[0].data))

print(supply.recorders[0].data[cou-1])  # prints 6.0

import json
import jsonpickle

tt= json.dumps(json.loads(jsonpickle.encode(model)), indent=2)
text_file = open("c:\\temp\model_New.dat", "w")
text_file.write(tt)

text_file.close()