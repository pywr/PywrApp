from hydra_base.lib.objects import JSONObject, Dataset


# TODO this can be removed in latest refactor branch
def convert_network_to_json_object(network):

    json_network = JSONObject(network)
    for scenario in json_network['scenarios']:
        for rs in scenario['resourcescenarios']:
            rs.dataset = Dataset(rs['dataset'])

    return json_network
