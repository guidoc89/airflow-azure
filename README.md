
## Configure Gen2 read/mount to Databricks
Check all of the [available options](https://learn.microsoft.com/en-us/azure/databricks/connect/storage/azure-storage) for connection.

First, follow the [official tutorial](https://learn.microsoft.com/en-us/azure/databricks/connect/storage/tutorial-azure-storage), in order to create:
1. A Microsoft Entra ID service principal 
1. A client secret for it
1. Grant the service principal access to our Azure Data Lake Storage Gen2


[Configure Key Vault for Databricks](https://learn.microsoft.com/en-us/azure/databricks/security/secrets/secret-scopes): 
    - remember to change in Settings/Access configuration the "Permission model"  to "Vault access policies"
    - then in the "Networking" tab can change to "Allow public access from specific virtual networks and IP addresses" + enabling
                "Allow trusted Microsoft services to bypass the firewall", or just leave "Allow access from all networks".


Mount a volume following the [example Python code](https://learn.microsoft.com/en-us/azure/databricks/dbfs/mounts).


## Give ADF access to the key vault

In order to create a connection between Databricks and ADF, we need to create a linked service between them, and authenticate the connection between them.
There are various ways to authenticate, we are going to be using the `Access Token` one, in which we create a Databricks token (the same way that we'd create one 
when connecting Databricks with its VSCode extension) and use it to authenticate both services. 

Obviously we want to manage all of our private and sensible values via the Azure Key Vault, and give access to the services that require them.

In this case, after creating the Databricks token we are going to store it as a secret value, and when connecting `ADF -> Databricks via a Linked Service`, 
use that key vault secret value to authenticate.

Bear in mind that we also need to create yet another Linked Service in ADF in order to be able to access our Key Vault secrets.
But not only that, we also need to make sure that our `ADF has the right Access Policies / permissions` to be able to access the values, otherwise we are going
to get a 403 error. So in our Key Vault page, in the "Access Policies" tab, we need to create a new policy for our ADF to read them, adding the "Secret Permissions"
boxes in the UI.

So the steps would be:
1. Create a Linked Service between ADF and the Key Vault
2. Create a Databricks token
3. Save the value and store as a secret in our Key Vault
4. Give the **Secret Permissions** Access Policies to our ADF (look up the Data Factory name that was created in our "Resource Group"").
5. In the ADF UI, create a new Linked Service with Databricks using our Key Vault connection

IMPORTANT: remember to `Publish the changes`.

## Configure Airflow Gen2 Data Lake connection

In order to be able to communicate with the Storage Account, since we are going to be checkign for the existance of data files to see if we need to
run the data extraction flow or not, we need to add an Airflow connection to the service.
There are a lot of builtins (once you install the [Microsoft Provider](https://airflow.apache.org/docs/apache-airflow-providers-microsoft-azure/stable/index.html)),
I've tried a lot of them, Data Lake Storage V2, Azure Data Lake, Container Instance, etc, but the one that worked for me was `Azure Blob Storage`, adding the **connection string** as parameter for the connection.

The connection string is visible inside of the Storage Account secrets, Azure automatically generates 2 keys and 2 connection strings when creating the service, use any of them as paramter for the connection.



## Workflow

1. See if the data exists in the Data Lake Gen2 storage
1. If it doesnt, import it 
1. If it does, can move on to the processing part
