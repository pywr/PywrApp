from Exporter.PywrJsonWriter import get_resourcescenarios_ids
import json
from collections import OrderedDict

def sort_value(value):
    try:
        return (sorted(value.items(), key=lambda t: int(t[0]))).values()
    except:
        return OrderedDict(sorted(value.items(), key=lambda t: t[0])).values()

def check_resources_attribute(res, res_scenario_value, attr, variables, objectives, constraints):
    metadata = json.loads(res_scenario_value.value.metadata)
    if 'attr_rule' in metadata:
        value = json.loads(res_scenario_value.value.value)['0']
        ord_value=sort_value(value)
        main_key=check_for_special_characters(attr.name+'_'+res.name)
        if metadata['attr_rule'] == 'variable':
            variables["DEC_XXX_"+main_key]=ord_value
        elif metadata['attr_rule'] == 'objective':
            objectives["MO_MIN"+main_key]=ord_value
        elif metadata['attr_rule'] == 'constraint':
            constraints["MO_MIN"+main_key]=ord_value

def write_polyviz_file(network, resourcescenarios, attrlist):
    constraints={}
    objectives={}
    variables={}
    resourcescenarios_ids = get_resourcescenarios_ids(resourcescenarios.resourcescenarios)
    attributes_ids = {}
    for attr in attrlist:
        attributes_ids[attr.id] = attr
    nodes_id_name = {}
    for attr_ in network.attributes:
        attr = attributes_ids[attr_.attr_id]
        if attr_.id not in resourcescenarios_ids.keys():
            continue
        res = resourcescenarios_ids[attr_.id]
        check_resources_attribute(network, res, attr, variables, objectives, constraints)
    for node_ in network.nodes:
        nodes_id_name[node_.id] = node_.name
        for attr_ in node_.attributes:
            attr = attributes_ids[attr_.attr_id]
            if attr_.id not in resourcescenarios_ids.keys():
                continue
            res = resourcescenarios_ids[attr_.id]
            check_resources_attribute(node_, res, attr, variables, objectives, constraints)
    for link_ in network.links:
        for attr_ in link_.attributes:
            attr = attributes_ids[attr_.attr_id]
            if attr_.id not in resourcescenarios_ids.keys():
                continue
            res = resourcescenarios_ids[attr_.id]
            check_resources_attribute(link_, res, attr, variables, objectives, constraints)

    main_contenets = {}
    main_contenets.update(variables)
    main_contenets.update(constraints)
    main_contenets.update(objectives)
    import pandas as pd
    df = pd.DataFrame.from_dict(main_contenets)
    df.to_csv(r"f:\\temp\\network"+str(network.id)+".csv")


def check_for_special_characters(strng):
    if "," in strng:
        strng=strng.replace(',','_')
    if '/' in strng:
        strng = strng.replace('/', '_')
    sp=["\\", "\%" , "\$", "\^" , "\*", "\@", "\!", "\(", "\)", "\:", "\;",  "\"", "\{", "\}", "\[", "\]"]
    for chr in sp:
        if chr in strng:
            strng=strng.replace (chr, "\\"+chr)
    return strng

