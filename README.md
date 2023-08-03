- [marine-eov-broker](#marine-eov-broker)
  * [Setup](#setup)
  * [Usage](#usage)
    + [With the demonstration notebook](#with-the-demonstration-notebook)
    + [From scratch](#from-scratch)
      - [Starting the Broker](#starting-the-broker)
      - [Querying datasets](#querying-datasets)
      - [Exploring the results](#exploring-the-results)
      - [Requesting data](#requesting-data)
  * [Software evolutions](#software-evolutions)

  
  
# marine-eov-broker
Essential Ocean Variables broker linking queries on EOVs to Marine RI data servers taking into account RI-specific vocabularies.

## Setup
To quickly setup the base environment, you can use conda to create an isolated python/jupyterlab environment by using the environment.yml file.
Assuming conda is already installed on your setup :
`conda env create -n <env_name> --file environment.yml`

Once the environment created, activate it & use the following command to install the marine-eov-broker package :
`python setup.py install`

## Usage

### With the demonstration notebook
The jupyter notebook at the root of the project can help you to quickly start using the marine-eov-broker module.
Make sure that the jupyter notebook is indeed using the conda environment in which you installed the package.

### From scratch

#### Starting the Broker
Import the module :
`from marine_eov_broker import MarineRiBroker`

Show the available EOVs :
`print(MarineRiBroker.EOV_LIST)`

Start the broker :
`broker = MarineRiBroker.MarineBroker()`

During this step, the broker will 
- query the NVS vocabulary server in order to get the EOVs correspondances with P01, P02, P09 & R03 parameters
- query the Erddap servers registered by default (Argo, SeaDataNet, ICOS, Lifewatch) to get datasets descriptions.

It is possible to override the default erddap servers and related datasets by providing the **erddap_servers** optional argument, such as the example below :
```
broker = MarineRiBroker.MarineBroker(
    erddap_servers={
        "https://www.ifremer.fr/erddap": ["ArgoFloats", "copernicus-fos"]
    }
)
```
  
The default erddap servers configured are the following (as of 2023-08-03):  
```
{
        "https://www.ifremer.fr/erddap": ["ArgoFloats", "ArgoFloats-synthetic-BGC", "SDC_BAL_CLIM_TS_V2_m", ...],
        "http://erddap.emso.eu/erddap": None,
        "https://erddap.icos-cp.eu/erddap": "[icos26na20170409SocatEnhanced", ...],
        "https://erddap.eurobis.org/erddap": ["obis_obis", ...],
}
```  
  
Specifying **None** instead of the datasets list will make the broker query all the datasets ; make sure it is a reasonable choice considering the number of datasets available in an erddap server.

The default SPARQL Endpoint configured are the following (as of 2023-08-03):  
```
{
        "Argo": "https://sparql.ifremer.fr/argo/query"
    }
...

#### Querying datasets
  
Once the broker is started, you can submit a query with the following :
```
start_date = "2002-10-01"
end_date = "2003-01-01"
min_lon = -180
min_lat = -90
max_lon = 180
max_lat = 90

response = broker.submit_request(["EV_SALIN", "EV_OXY", "EV_SEATEMP", "EV_CO2", "EV_CHLA"], 
                                 start_date,
                                 end_date,
                                 min_lon,
                                 min_lat,
                                 max_lon,
                                 max_lat,
                                 "nc"
                                 )
```
  
#### Exploring the results
  
The broker gathers the results in a pandas DataFrame containing for each found dataset :
- the dataset_id as the index value
- the dataset metadata found in erddap
- EOV/Variables name correspondance found
- a query string that reflects the user query constraints
  
#### Requesting data
  
Data access is made on dataset basis.  
Main data access methods are the following :
- `get_datasets_list()`: simply retrieves the dataset IDs list ; may be useful in order to loop over each dataset & get data regardless of the metadata
- `get_dataset_EOVs_list(dataset_id)`: gets a dictionnary with EOV/Variable name correspondance for the dataset_id provided
- `dataset_to_xarray(dataset_id)`: retrieves the data returned by the query stored in the response in an xarray dataset
- `dataset_to_pandas_dataframe(dataset_id)`: retrieves the data returned by the query stored in the response in a Pandas DataFrame
- `dataset_to_file_download(dataset_id, output_format)`: retrieves the data returned by the query stored and saves it in the output format with the following file naming **dataset_id-timestamp.output_format**

## Software evolutions

Feel free to register issues and submit pull requests.

To submit pull request, please first make sure that your code to merge is in a dedicated branch to avoid conflicts.  
  
  
<small><i><a href='http://ecotrust-canada.github.io/markdown-toc/'>Table of contents generated with markdown-toc</a></i></small>

