# Built-in Imports
import sys
import os

sys.path.append(os.path.abspath(".."))
sys.path.append(os.path.abspath("."))

# Third-Party Imports
import pandas as pd

# Internal Imports
from utils.access_token import AccessToken
from utils._constants import (
    PAYLOAD,
    DOMAIN,
    API_VERSION,
    OUTPUT_FILE_WON_OPPO_FC,
)
from utils.utils import get_data

# Auth Setup
auth = AccessToken(domain=DOMAIN, payload=PAYLOAD)
auth.generate_access_token()
auth_header = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {auth.access_token}",
}

# Salesforce Queries
api_endpoint = DOMAIN + f"/services/data/v{API_VERSION}/query/?q="
oppo_query = """SELECT 
Id, AccountId, Account.Name, Name, StageName, CreatedDate, CloseDate, Amount, OwnerId, Owner.Name, Owner.UserRegion__c, CurrencyIsoCode, Type 
FROM 
Opportunity 
WHERE 
RecordType.Name = 'Main Opportunity' 
and StageName = 'Closed Won' 
and CloseDate = THIS_FISCAL_YEAR
"""
oppo_query = oppo_query.replace("\n", "").replace(" ", "+").strip()

forecast_query = """SELECT 
Id, Account__c, Account__r.Name, CreatedDate, Date__c, Amount__c, CreatedById, CreatedBy.Name, Account__r.Region__c, CurrencyIsoCode, Business_line__c, Product_Family__c 
FROM Forecast__c 
WHERE Account__c != null
"""
forecast_query = forecast_query.replace("\n", "").replace(" ", "+").strip()

# Get Opportunity Data
raw_oppo_data = get_data(
    domain=DOMAIN,
    api_endpoint=api_endpoint,
    query=oppo_query,
    auth_header=auth_header,
    val1="Account",
    val2="Owner",
)

raw_oppo_data["CreatedDate"] = pd.to_datetime(
    raw_oppo_data["CreatedDate"], format="%Y-%m-%dT%H:%M:%S.000+0000"
)
raw_oppo_data["CloseDate"] = pd.to_datetime(
    raw_oppo_data["CloseDate"], format="%Y-%m-%d"
)

# Get Forecast Data
raw_forecast_data = get_data(
    domain=DOMAIN,
    api_endpoint=api_endpoint,
    query=forecast_query,
    auth_header=auth_header,
    val1="Account__r",
    val2="CreatedBy",
)

raw_forecast_data["CreatedDate"] = pd.to_datetime(
    raw_forecast_data["CreatedDate"], format="%Y-%m-%dT%H:%M:%S.000+0000"
)

raw_forecast_data["Date__c"] = pd.to_datetime(
    raw_forecast_data["Date__c"], format="%Y-%m-%d"
)

# Join forecast data to opportunity data based on accountid, closedate of oppo, and created date of forecast
# to determine if forecast was created after won oppo
# Steps:
# 1. For each row of won oppo, find all forecast rows with the same AccountId with created date after the closedate of the won oppo

oppo_w_fc_type_dict = []
attributable_forecasts = []
not_attributable_forecasts = []
for _, row in raw_oppo_data.iterrows():
    forecasts_attributable = raw_forecast_data.loc[
        (raw_forecast_data["CreatedDate"] >= row["CloseDate"])
        & (raw_forecast_data["Account__c"] == row["AccountId"])
    ].copy()
    forecast_not_attributable = raw_forecast_data.loc[
        (raw_forecast_data["CreatedDate"] < row["CloseDate"])
        & (raw_forecast_data["Account__c"] == row["AccountId"])
    ].copy()

    if not forecast_not_attributable.empty:
        forecast_not_attributable.loc[:, "OpportunityId"] = row["Id"]

    if not forecasts_attributable.empty:
        forecasts_attributable.loc[:, "OpportunityId"] = row["Id"]

    if forecasts_attributable.empty:
        row["Forecast Type"] = "No Forecast Created"

    elif forecast_not_attributable.empty and len(forecasts_attributable) > 0:
        row["Forecast Type"] = "New Forecast Created After Won Oppo"
    elif len(forecast_not_attributable) > 0 and len(forecasts_attributable) > 0:
        row["Forecast Type"] = "Could be from Recurring Forecast"

    oppo_w_fc_type_dict.append(row.to_dict())
    attributable_forecasts.extend(forecasts_attributable.to_dict("records"))
    not_attributable_forecasts.extend(forecast_not_attributable.to_dict("records"))

oppo_w_fc_type = pd.DataFrame.from_records(oppo_w_fc_type_dict)
attributable_forecasts = pd.DataFrame.from_records(attributable_forecasts)
not_attributable_forecasts = pd.DataFrame.from_records(not_attributable_forecasts)
# Merge oppo df and forecast w oppo id df
final_df = oppo_w_fc_type.merge(
    attributable_forecasts.groupby(["OpportunityId", "CurrencyIsoCode"])
    .agg({"Amount__c": "sum"})
    .reset_index(),
    how="left",
    left_on="Id",
    right_on="OpportunityId",
    suffixes=("", "_forecast_attributable"),
)

final_df = final_df.merge(
    not_attributable_forecasts.groupby(["OpportunityId", "CurrencyIsoCode"])
    .agg({"Amount__c": "sum"})
    .reset_index(),
    how="left",
    left_on="Id",
    right_on="OpportunityId",
    suffixes=("", "_forecast_not_attributable"),
)
final_df = final_df.drop(
    [
        "Owner.UserRegion__c",
        "CurrencyIsoCode_forecast_attributable",
        "CurrencyIsoCode_forecast_not_attributable",
        "OpportunityId",
        "OpportunityId_forecast_not_attributable",
    ],
    axis=1,
).rename(
    columns={
        "Amount__c": "Attributable Forecasts",
        "Amount__c_forecast_not_attributable": "Not Attributable Forecasts",
        "Amount": "Oppo Revenue",
        "Name": "Oppo Name",
        "CreatedDate": "Oppo Created Date",
        "CloseDate": "Oppo Close Date",
        "Owner.Name": "Oppo Owner",
        "OwnerId": "Oppo Owner Id",
        "Type": "Oppo Type",
        "Id": "Oppo Id",
    }
)

with pd.ExcelWriter(
    path=OUTPUT_FILE_WON_OPPO_FC,
    mode="a",
    if_sheet_exists="replace",
) as writer:
    raw_oppo_data.to_excel(writer, sheet_name="Opportunity Data", index=False)
    raw_forecast_data.to_excel(writer, sheet_name="Forecast Data", index=False)
    # attributable_forecasts.to_excel(
    #     writer, sheet_name="Attributable Forecasts", index=False
    # )
    final_df.to_excel(writer, sheet_name="Final_raw", index=False)
