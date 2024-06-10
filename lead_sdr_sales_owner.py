import pandas as pd
from dotenv import load_dotenv
import os
from access_token import AccessToken
from data_query import get_data

load_dotenv()

argument_dict = {
    "url": os.environ.get("PROD_DOMAIN"),
    "client_id": os.environ.get("PROD_CONSUMER_KEY"),
    "client_secret": os.environ.get("PROD_CONSUMER_SECRET"),
    "username": os.environ.get("PROD_USERNAME"),
    "password": os.environ.get("PROD_PASSWORD"),
    "security_token": os.environ.get("PROD_SECURITY_TOKEN"),
}

payload = {
    "grant_type": "client_credentials",
    "client_id": argument_dict.get("client_id"),
    "client_secret": argument_dict.get("client_secret"),
}


auth = AccessToken(domain=os.getenv("PROD_DOMAIN"), payload=payload)
auth.generate_access_token()

api_version = "60.0"
api_endpoint = os.getenv("PROD_DOMAIN") + f"/services/data/v{api_version}/query/?q="
query = "SELECT+LeadId,+Field,+OldValue,+NewValue,+DataType,+CreatedById,+CreatedDate,+Id,+Lead.SDROwner__c,+Lead.SalesOwner__c,+Lead.OwnerId,+Lead.Owner.Name+FROM+LeadHistory+WHERE+(Field='ownerAssignment'+or+Field='Owner')+and+DataType='EntityId'"
auth_header = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {auth.access_token}",
}

raw_history = get_data(api_endpoint, query, auth_header)

sales_members = pd.read_excel(
    io=os.getenv("SALES_MEMBERS_FILE"),
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

final_output.to_excel(
    os.getenv("OUTPUT_FILE"),
    sheet_name="Final Output",
    index=False,
)
