import requests
from dotenv import load_dotenv
import pandas as pd

load_dotenv(".env")


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


def get_data(api_endpoint, query, auth_header) -> pd.DataFrame:
    response = requests.get(url=api_endpoint + query, headers=auth_header)

    data = response.json()

    raw_history = data["records"]

    for i, record in enumerate(raw_history):
        del record["attributes"]
        del record["Lead"]["attributes"]
        del record["Lead"]["Owner"]["attributes"]
        raw_history[i] = flatten_dictionary(record)

    raw_history = pd.DataFrame(data["records"])
    return raw_history
