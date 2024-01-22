#########################################################################################
# Install modules etc.
#########################################################################################
!pip install deltalake
import requests
import adal
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from datetime import date
import os
from delta.tables import *
from deltalake.writer import write_deltalake
activityDate = date.today()

#########################################################################################
# Read secrets from Azure Key Vault
#########################################################################################
key_vault = "your_keyvault_here"
tenant_id = mssparkutils.credentials.getSecret(key_vault , "FabricTenant")
client_id = mssparkutils.credentials.getSecret(key_vault , "AdminApiClientId")
client_secret = mssparkutils.credentials.getSecret(key_vault , "AdminApiClientSecret")

#########################################################################################
# Authentication - Replace string variables with your relevant values       
#########################################################################################

try:
    from azure.identity import ClientSecretCredential
except Exception:
     !pip install azure.identity
     from azure.identity import ClientSecretCredential

#########################################################################################
# Define the power bi admin api endpoint
powerbi_url = "https://api.fabric.microsoft.com/v1/admin/tenantsettings"

# Define the scope for power bi service
scope = "https://analysis.windows.net/powerbi/api/.default"

# Define the oauth2 token endpoint
token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"

# Define the payload for requesting the access token
payload = {
    "grant_type": "client_credentials",
    "client_id": client_id,
    "client_secret": client_secret,
    "scope": scope
}
# Make a post request to get the access token
response = requests.post(token_url, data=payload)

# Check if the request was successful
if response.status_code == 200:
    # Get the access token from the response
    access_token = response.json()["access_token"]

    # Define the headers for the power bi admin api request
    headers = {
        "Authorization": f"Bearer {access_token}"
    }

    # Make a get request to get the data from the power bi admin api
    response = requests.get(powerbi_url, headers=headers)

    # Check if the request was successful
    if response.status_code == 200:
        # Get the data from the response
        data = response.json()

        # Convert the data to a spark dataframe
        dfSettings = spark.createDataFrame(data["tenantSettings"])

    else:
        # Print the error message
        print(f"Power BI Admin API request failed: {response.text}")
else:
    # Print the error message
    print(f"Token request failed: {response.text}")


# Add new column ExportedDate to df 
dfSettings = dfSettings.withColumn("ExportedDate", lit(activityDate))
dfSettings.show()

# Write to table
dfSettings.write.mode("overwrite").format("delta").saveAsTable("TenantSettings-initials")