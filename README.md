
Test project that integrates Azure Data Factory + Azure Gen2 Storage + Azure Databricks, orchestrated with Airflow.  

Will get [Tennis CSV data](https://github.com/JeffSackmann/tennis_atp/tree/master) (players data and singles matches data from 2000-2024) from a GitHub repo in order to calculate relevant metrics like:
- Win %
- Tournaments played
- Tournament %: the prob of winning a tournament they participate in
- Grand Slams appearances 
- Grand Slams win %
- Grand Slams %: the prob of winning a GS they participate in
- Finals played
- Finals win %

The **analysis part is not the focus** (the Databricks notebooks can be found in notebooks/), but the ***integration between the Azure services 
(Service Principals, Key Vault and secrets, Storage Account, authentication methods, Databricks, etc) and their orchestration using Airflow***.

Hardest part? The [Airflow connections](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/connections.html) setup in the UI, had to hand-try them, since there are a various ways to configure them, also depending on 
the type of Airflow Connectors / Sensors and the protocols to use  (ABFSS, recommended, WASBS).

## Configure Gen2 read/mount to Databricks

Check how to connect to  [Azure Data Lake and Blob Storage](https://learn.microsoft.com/en-us/azure/databricks/connect/storage/azure-storage).

First, follow the [official tutorial](https://learn.microsoft.com/en-us/azure/databricks/connect/storage/tutorial-azure-storage), in order to create:
1. A Microsoft Entra ID service principal 
1. A client secret for it
1. Grant the service principal access to our Azure Data Lake Storage Gen2


[Configure Key Vault for Databricks](https://learn.microsoft.com/en-us/azure/databricks/security/secrets/secret-scopes): 
- remember to change in `Settings/Access` configuration the "Permission model"  to "Vault access policies"
- then in the `Networking` tab can change to `Allow public access from specific virtual networks and IP addresses` + enabling
            `Allow trusted Microsoft services to bypass the firewall`, or just leave `Allow access from all networks`.


Mount a volume following the [example Python code](https://learn.microsoft.com/en-us/azure/databricks/dbfs/mounts).


## Give ADF access to the key vault

In order to create a connection between Databricks and ADF, you need to create a **linked service between them**, and authenticate its connection.
There are various ways to authenticate, I'm going to go with `Access Token`, in which you create a Databricks token (the same way that you'd create one 
when connecting Databricks with its VSCode extension) and use it to authenticate both services. 

Obviously you'd want to manage all of your private and sensible values via the Azure Key Vault, and give access to the services that require them.

In this case, after creating the Databricks token I'm going to store it as a secret value, and when connecting `ADF -> Databricks via a Linked Service`, 
use that key vault secret value to authenticate.

Bear in mind that you also need to create yet another Linked Service in ADF in order to be able to access your Key Vault secrets.
But not only that, you also need to make sure that your ADF has the right `Access Policies / permissions` to be able to access the values, otherwise your are going
to get a 403 error. So in your Key Vault page, in the "Access Policies" tab, you need to create a new policy to let ADF to read them, adding the `Secret Permissions`
boxes in the UI.

So the steps would be:
1. Create a Linked Service between ADF and the Key Vault
2. Create a Databricks token
3. Save the value and store as a secret in our Key Vault
4. Give the `Secret Permissions Access Policies` to your ADF (look up the Data Factory name that was created in your `Resource Group`).
5. In the ADF UI, create a new Linked Service with Databricks using your **Key Vault connection**

IMPORTANT: remember to `Publish the changes`.

## Configure Airflow Gen2 Data Lake connection

In order to be able to communicate with the Storage Account, since I'm are going to be checking for the existence of data files to see if I need to
run the data extraction flow or not, I need to add an Airflow connection to the service.
There are a lot of builtins (once you install the [Microsoft Provider](https://airflow.apache.org/docs/apache-airflow-providers-microsoft-azure/stable/index.html)),
I've tried a lot of them, Data Lake Storage V2, Azure Data Lake, Container Instance, etc, but the one that worked for me was `Azure Blob Storage`, adding the **connection string** as parameter for the connection.

The connection string is visible inside of the Storage Account secrets, Azure automatically generates 2 keys and 2 connection strings when creating the service, use any of them as parameter for the connection.


## Workflow

1. See if the data exists in the Data Lake Gen2 storage
    * If it doesn't, import it into the Bronze layer
    * If it does, skip and move on to the processing part

1. Simple Bronze -> Silver layer processing: 
    - column names
    - parse date formats
    - try to complete some missing values using another table values (first, last names)
    - standardize naming conventions (could happen that a full name like "First-Second Last" appears as "First Second Last", without the "-" for example)
    - add some helper cols 
1. Simple Silver -> Gold layer processing: 
    - metrics calculations
    - eventual Gold CSV (TODO: delta files)
1. TODO: add Azure Synapse + Power BI 

