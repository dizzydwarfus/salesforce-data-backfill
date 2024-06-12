# Built-in Imports
import sys
import os

sys.path.append(os.path.abspath(".."))
sys.path.append(os.path.abspath("."))

# Third-Party Imports
import pandas as pd
import requests

# Internal Imports
from utils.access_token import AccessToken
from utils._constants import (
    PAYLOAD,
    DOMAIN,
    SALES_MEMBERS_FILE,
    OUTPUT_FILE_LEAD_MEMBERS,
    API_VERSION,
)

from utils.utils import flatten_dictionary

# Auth Setup
auth = AccessToken(domain=DOMAIN, payload=PAYLOAD)
auth.generate_access_token()
auth_header = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {auth.access_token}",
}

# Query Salesforce
api_endpoint = DOMAIN + f"/services/data/v{API_VERSION}/query/?q="
query = "SELECT+LeadId,+Field,+OldValue,+NewValue,+DataType,+CreatedById,+CreatedDate,+Id,+Lead.SDROwner__c,+Lead.SalesOwner__c,+Lead.OwnerId,+Lead.Owner.Name+FROM+LeadHistory+WHERE+(Field='ownerAssignment'+or+Field='Owner')+and+DataType='EntityId'"


# Get Lead History Data
def get_lead_data(api_endpoint, query, auth_header) -> pd.DataFrame:
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


raw_history = get_lead_data(api_endpoint, query, auth_header)

# Get Sales Members Data
sales_members = pd.read_excel(
    io=SALES_MEMBERS_FILE,
    sheet_name="Sales Members",
)


# Clean raw_history
raw_history["CreatedDate"] = pd.to_datetime(
    raw_history["CreatedDate"], format="%Y-%m-%dT%H:%M:%S.000+0000"
)
raw_history = raw_history.sort_values(by=["LeadId", "CreatedDate"])

processed_history = raw_history.merge(
    sales_members,
    how="left",
    left_on="NewValue",
    right_on="Id",
    suffixes=("", "_merged"),
).drop(["_", "Id_merged", "Name"], axis=1)

processed_history = processed_history.merge(
    sales_members,
    how="left",
    left_on="OldValue",
    right_on="Id",
    suffixes=("", "_merged"),
).drop(["_", "Id_merged", "Name"], axis=1)
processed_history = processed_history.rename(
    columns={"Team": "NewValueTeam", "Team_merged": "OldValueTeam"}
)

# loop through each lead
# for each lead, loop through each row and reassign Sales Owner or SDR Owner based on New or Old Value
# since dataframe is sorted ascendingly based on CreatedDate of Lead History record, the last record will be the latest owner assignment

final_dict_list = {}

for lead_id, group in processed_history.groupby("LeadId"):
    try:
        final_dict_list[lead_id]
    except KeyError:
        final_dict_list[lead_id] = {}

    for index, row in group.iterrows():
        final_dict_list[lead_id][row["NewValueTeam"]] = row["NewValue"]
        final_dict_list[lead_id][row["OldValueTeam"]] = row["OldValue"]

final_output = (
    pd.DataFrame(final_dict_list)
    .T.reset_index()
    .rename(
        columns={
            "index": "LeadId",
            "SDR Owner": "SDROwner__c",
            "Sales Owner": "SalesOwner__c",
        }
    )
)

sales_members_dict = sales_members.set_index("Id")["Name"].to_dict()

final_output["SDR Owner Name"] = final_output["SDROwner__c"].map(sales_members_dict)
final_output["Sales Owner Name"] = final_output["SalesOwner__c"].map(sales_members_dict)
final_output["Admin Name"] = final_output["Admin"].map(sales_members_dict)

with pd.ExcelWriter(
    path=OUTPUT_FILE_LEAD_MEMBERS,
    mode="a",
    if_sheet_exists="replace",
) as writer:
    final_output.to_excel(writer, sheet_name="Final Output", index=False)
