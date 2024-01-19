###################################################################################################
# Import required libraries
###################################################################################################
import requests
import adal
import json
import pandas as pd
from datetime import date

#location where export file will be saves- use File API path
path = '/lakehouse/default/Files/TenantSettings/'
activityDate = date.today().strftime("%Y-%m-%d")

###################################################################################################
# Authenticate for owner of notebook - Tenant settings API does not support service principal
###################################################################################################

pbi_access_token = mssparkutils.credentials.getToken("https://analysis.windows.net/powerbi/api")


############################################################################################################
# Call API with GET request to TenantSettingsURL (call endpoint and pass token as header for authentication)
############################################################################################################

TenantSettingsURL = 'https://api.fabric.microsoft.com/v1/admin/tenantsettings'
header = {'Authorization': f'Bearer {pbi_access_token}'}
TenantSettingsJSON = requests.get(TenantSettingsURL , headers=header)

#Parse the response as a JSON object and extract the tenantSettings key
TenantSettingsJSONContent = json.loads(TenantSettingsJSON.content)
TenantSettingsJSONContentExplode = TenantSettingsJSONContent.get('tenantSettings')

# Convert list of dictionaries into pandas dataframe 
df = pd.DataFrame(TenantSettingsJSONContentExplode)

# Add new column ExportedDate to df and save df as csv
df['ExportedDate'] = activityDate
df.to_csv(path + activityDate + '_TenantSettings.csv', index=False) 

# When running for the first time - use "overwrite" - change to "append" later
df = spark.read.format("csv").option("header","true").load("Files/TenantSettings/*.csv")
df.write.mode("append").format("delta").saveAsTable("TenantSettings")