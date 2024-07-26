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
Id, AccountId, Account.Name, Name, StageName, CreatedDate, CloseDate, Amount, OwnerId, Owner.Name, Owner.UserRegion__c, CurrencyIsoCode, Type, Ramp_up_time_years__c, BusinessLine__c 
FROM 
Opportunity 
WHERE 
RecordType.Name = 'Main Opportunity' 
and StageName != 'Closed Won'
and (CloseDate = THIS_FISCAL_YEAR)
"""

oppo_query = oppo_query.replace("\n", "").replace(" ", "+").strip()

forecast_query = """SELECT 
Id, Account__c, Account__r.Name, CreatedDate, Date__c, Amount__c, CreatedById, CreatedBy.Name, Account__r.Region__c, CurrencyIsoCode, Business_line__c, Product_Family__c 
FROM Forecast__c 
WHERE Account__c != null and Account__c != '0011t00000W6CaTAAV'
"""

forecast_query = forecast_query.replace("\n", "").replace(" ", "+").strip()

raw_oppo_data = get_data(
    domain=DOMAIN,
    api_endpoint=api_endpoint,
    query=oppo_query,
    auth_header=auth_header,
    val1="Account",
    val2="Owner",
)

raw_forecast_data = get_data(
    domain=DOMAIN,
    api_endpoint=api_endpoint,
    query=forecast_query,
    auth_header=auth_header,
    val1="Account__r",
    val2="CreatedBy",
)

raw_oppo_data["CreatedDate"] = pd.to_datetime(
    raw_oppo_data["CreatedDate"], format="%Y-%m-%dT%H:%M:%S.000+0000"
)
raw_oppo_data["CloseDate"] = pd.to_datetime(
    raw_oppo_data["CloseDate"], format="%Y-%m-%d"
)

raw_forecast_data["CreatedDate"] = pd.to_datetime(
    raw_forecast_data["CreatedDate"], format="%Y-%m-%dT%H:%M:%S.000+0000"
)

raw_forecast_data["Date__c"] = pd.to_datetime(
    raw_forecast_data["Date__c"], format="%Y-%m-%d"
)
