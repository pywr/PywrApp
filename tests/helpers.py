from hydra_base.lib.objects import JSONObject, Dataset

def convert_network_to_json_object(network):

    json_network = JSONObject(network)
    for scenario in json_network['scenarios']:
        for rs in scenario['resourcescenarios']:
            rs.value = Dataset(rs['value'])

    return json_network
