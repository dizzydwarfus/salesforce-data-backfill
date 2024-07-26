import pandas as pd
import requests


def flatten_dictionary(dict_to_flatten):
    new_dict = {}
    values_data_type = []

    for _, value_type in enumerate(dict_to_flatten):
        values_data_type.append(isinstance(dict_to_flatten[value_type], dict))

    if True not in values_data_type:
        return dict_to_flatten

    for _, lvl1 in enumerate(dict_to_flatten):
        if isinstance(dict_to_flatten[lvl1], dict):
            for _, lvl2 in enumerate(dict_to_flatten[lvl1]):
                new_dict[lvl1 + "." + lvl2] = dict_to_flatten[lvl1][lvl2]
        else:
            new_dict[lvl1] = dict_to_flatten[lvl1]

    return flatten_dictionary(new_dict)


def format_query(query: str) -> str:
    query = query.replace("\n", "").replace(" ", "+").strip()
    return query


def get_data(
    domain: str, api_endpoint: str, query: str, auth_header: dict, **kwargs
) -> pd.DataFrame:
    all_data = []

    response = requests.get(url=api_endpoint + query, headers=auth_header)
    data = response.json()
    all_data.extend(data["records"])

    while "nextRecordsUrl" in data:
        response = requests.get(
            url=domain + data["nextRecordsUrl"], headers=auth_header
        )
        data = response.json()

        all_data.extend(data["records"])

    for i, record in enumerate(all_data):
        del record["attributes"]

        if len(kwargs) == 0:
            pass

        for _, v in kwargs.items():
            if record.get(v):
                del record[v]["attributes"]

        all_data[i] = flatten_dictionary(record)

    all_data = pd.DataFrame(all_data)
    return all_data
