# marine-eov-broker
Essential Ocean Variables broker linking queries on EOVs to Marine RI data servers taking into account RI-specific vocabularies.

# Setup
To quickly setup the base environment, you can use conda to create an isolated python/jupyterlab environment by using the marine_eov.yml file.
Assuming conda is already installed on your setup :
`conda env create -n <env_name> --file marine_eov.yml`

Once the environment created, activate it & use the following command to install the marine-eov-broker package :
`pip install git+https://github.com/twnone/marine-eov-broker.git`

# Quickstart

## With the demonstration notebook
The jupyter notebook at the root of the project can help you to quickly start using the marine-eov-broker module.
Make sure that the jupyter notebook is indeed using the conda environment in which you installed the package.

## From scratch
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
Specifying **None** instead of the datasets list will make the broker query all the datasets ; make sure it is a reasonable choice considering the number of datasets available in an erddap server.


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

The broker gathers the results in a pandas DataFrame containing for each found dataset :
- the dataset_id as the index value
- the dataset metadata found in erddap
- the EOVs found in the dataset and the related effective variables name
- an ErddapRequest object that provides the user with convenience method to get the data with xarray/pandas or file download.

The notebook provides more complete examples about the convenience methods.

# Software evolutions

Feel free to register issues and pull requests.

To submit pull request, please first make sure that your code to merge is in a dedicated branch to avoid conflicts