from . import ErddapMarineRI
import pandas as pd
import requests
# from urllib.error import HTTPError
import io
import xarray as xr
import numpy as np
import time
import logging
import concurrent.futures
import datetime
from SPARQLWrapper import SPARQLWrapper, JSON

INPUT_DATE_FORMATS = ["%Y%m%dT%H%M%SZ", "%Y-%m-%dT%H:%M:%SZ", 
                      "%Y%m%dT%H:%M:%SZ", "%Y-%m-%dT%H%M%SZ", 
                      "%Y%m%d", "%Y-%m-%d"]

ERDDAP_OUTPUT_FORMATS = ["csv", "geoJson", "json", "nc", "ncCF", "odvTxt"]
EOV_LIST = ['EV_OXY', 'EV_SEATEMP', 'EV_SALIN', 'EV_CURR', 'EV_CHLA', 'EV_CO2', 'EV_NUTS']

logger = logging.getLogger(__name__)


class MarineBroker:
    
    vocabularies_server = "https://vocab.nerc.ac.uk/sparql/"
    DEFAULT_ERDDAP_SERVERS = {
        "https://www.ifremer.fr/erddap": ["ArgoFloats", "ArgoFloats-synthetic-BGC", 
                                          "SDC_BAL_CLIM_TS_V2_m", "SDC_BAL_CLIM_TS_V2_s",
                                          "SDC_GLO_AGG_V2", 
                                          "SDC_GLO_CLIM_TS_V2_1", "SDC_GLO_CLIM_TS_V2_2",
                                          "SDC_BLS_CLIM_TS_V2_m", "SDC_BLS_CLIM_TS_V2_s",
                                          "SDC_MED_CLIM_TS_V2_m_pre_post_emt",
                                          "SDC_MED_CLIM_TS_V2_m_whole_period",
                                          "SDC_MED_CLIM_TS_V2_s_decades",
                                          "SDC_MED_CLIM_TS_V2_s_pre_post_emt",
                                          "SDC_MED_CLIM_TS_V2_s_whole_period",
                                          "SDC_NAT_CLIM_TS_V2_050_m", "SDC_NAT_CLIM_TS_V2_050_s"],
        "http://erddap.emso.eu/erddap": None,
        "https://erddap.icos-cp.eu/erddap": ["icos11ss20211206"]
    }

    def __init__(self, erddap_servers=DEFAULT_ERDDAP_SERVERS):
        """Create a new broker and automatically scan Erddap servers provided.
        
        Keyword arguments:
        erddap_servers -- Dict containing Erddap servers URL as keys and lists of dataset IDs as values 
                          (if value is None, then all datasets will be collected by the broker)
        """
#         self.erddap_servers = erddap_servers        
        self.datasets_list = []
        self.datasets = []
        self.vocabularies = {}
        
        with concurrent.futures.ThreadPoolExecutor(10) as executor:
            futures = []
            for eov in EOV_LIST:
                futures.append(executor.submit(self.build_vocabularies, eov))
                
            for future in concurrent.futures.as_completed(futures):
                eov, eov_result = future.result()
                self.vocabularies[eov] = eov_result
#                 executor.submit(self.build_vocabularies, eov)

        for erddap_server in erddap_servers.keys():
            if erddap_servers[erddap_server] is not None:
                self.datasets_list.extend([(erddap_server, dataset_id) for dataset_id in erddap_servers[erddap_server]])
            else:
                self.find_datasets_in_erddap_server(erddap_server)
        
        with concurrent.futures.ThreadPoolExecutor(5) as executor:
            futures = []
            for erddap_server, dataset_id in self.datasets_list:
                futures.append(executor.submit(self.get_dataset, erddap_server, dataset_id))
                
            for future in concurrent.futures.as_completed(futures):
                try:
                    self.datasets.append(future.result())
                except:
                    pass
                    
    
            
    def find_datasets_in_erddap_server(self, erddap_server) -> None:
        """
        Find datasets hosted by an Erddap server. The ErddapDataset object is created if it is not already listed in datasets_list attribute.
        
        Argument :
        erddap_server : (str) base url for the Erddap server

        """
        start = time.time()
        erddap_datasets_list = []
        
        erddap_datasets_list = pd.read_csv(f"{erddap_server}/info/index.csv")
        erddap_datasets_list = erddap_datasets_list[erddap_datasets_list["Dataset ID"] != "allDatasets"]["Dataset ID"].to_list()
            
        for dataset_id in erddap_datasets_list:
            if (erddap_server, dataset_id) not in self.datasets_list:
#                 self.datasets.append(ErddapMarineRI.ErddapDataset(erddap_server, dataset_id))
                self.datasets_list.append((erddap_server, dataset_id))
        
        logger.debug(f"Took {time.time() - start} seconds to get datasets list for {erddap_server}")
        
    def get_dataset(self, erddap_server, dataset_id):
        """
        Creates an ErddapDataset object from an Erddap dataset.
        Arguments:
        erddap_server : (str) Erddap server URL
        dataset_id : (str) Erddap dataset ID
        """
        start = time.time()
        erddap_dataset = ErddapMarineRI.ErddapDataset(erddap_server, dataset_id)
        logger.debug(f"Loaded {dataset_id} in {time.time() - start} seconds.")
        return erddap_dataset  
    
    def build_vocabularies(self, eov) -> dict:
        """
        Wrapper for query_vocabularies()
        Argument :
        eov: (str) Essential Ocean Variable name
        """
        start = time.time()
        vocabularies = {}
        
        eov_result = self.query_vocabularies(eov)
        logger.debug(f"Gathering vocabularies took {time.time() - start} seconds.")
        return eov, eov_result
        
    
    def query_vocabularies(self, eov) -> dict:
        """
        Query vocabulary server and search for P01 parameters names corresponding to a A05 EOV term.
        
        Arguments :
        eov: variable string
        """
        sparql = SPARQLWrapper(self.vocabularies_server)

        logging.info(f"Querying vocabulary server for EOV : {eov}")
        sparql.setQuery("""
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX pav: <http://purl.org/pav/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>

select distinct  ?dt ?P01notation ?prefLabel (?R03notation as ?R03) (?P09notation as ?P09) (?P02notation as ?P02)where {<http://vocab.nerc.ac.uk/collection/A05/current/""" + eov + """/> 
#<https://w3id.org/env/puv#statistic> ?v ; 
    
<https://w3id.org/env/puv#matrix> ?v1  ;
  <https://w3id.org/env/puv#property> ?v2  .
    

<http://vocab.nerc.ac.uk/collection/P01/current/>  skos:member ?dt  .
  ?dt owl:deprecated ?depr . FILTER((str(?depr)="false"))
 
  #?dt ?rel ?v .  filter(!regex(str(?v),'S07') && regex(str(?rel),'related') ).
  NOT EXISTS {
  ?dt ?rel  ?v .  filter(regex(str(?v),'S07/current/'))
   }
  
     ?dt ?rel1 ?v1 .
       ?dt ?rel2 ?v2 .
  optional {    ?dt ?rel3 ?v3 . filter(regex(str(?v3),'R03/current/')) .  ?v3 skos:notation ?R03notation .}
   optional {    ?dt ?rel4 ?v4 . filter(regex(str(?v4),'P09/current/')) .  ?v4 skos:notation ?P09notation .}
     optional {    ?dt ?rel5 ?v5 . filter(regex(str(?v5),'P02/current/')) .  ?v5 skos:notation ?P02notation .}

?dt skos:prefLabel ?prefLabel .FILTER(langMatches(lang(?prefLabel), "en"))
    ?dt skos:notation ?P01notation .
  
  
} 
    """
    )
        sparql.setReturnFormat(JSON)
        response = sparql.query().convert()
        return response

    
    def find_eov_in_dataset(self, dataset, eov, eov_vocabs):
        """
        Looks through the dataset metadata to find a matching variable name.
        The match is made on the "sdn_parameter_urn" variable attribute.
        
        Arguments:
        dataset: ErddapDataset object
        eov_vocabs: dictionnary containing vocabulary server response.
        
        Returns : variable name if a match is made, otherwise False.
        """

        found_eov = ""
        P01 = [v['P01notation']['value'] for v in eov_vocabs["results"]["bindings"]]
        P02 = [v['P02']['value'] for v in eov_vocabs["results"]["bindings"]]
        
#         found_vars = [dataset.parameters[i] for i in P01 if i in dataset.parameters.keys()]
        found_vars = []
        for i in P01:
            if i in dataset.parameters.keys():
                found_vars.append(dataset.parameters[i])
                dataset.found_eovs[eov] = dataset.parameters[i]
        
        if len(found_vars) == 0:
#             found_vars = [dataset.parameters[i] for i in P02 if i in dataset.parameters.keys()]
            for i in P02:
                if i in dataset.parameters.keys():
                    found_vars.append(dataset.parameters[i])
                    dataset.found_eovs[eov] = dataset.parameters[i]
            
        if found_vars is not None and len(found_vars) > 0:
#             logging.debug(f"Found vars for dataset {dataset.name} : {np.unique(found_vars)}")
            return np.unique(found_vars)
        else:
#             logging.debug(f"No vars found for dataset {dataset.name}")
            return []
            
        # Nothing was found while looping through vocabularies:
        return None
    
    def validate_datetime(self, input_date):
        """
        Validates a string datetime and checks if it can be parsed in one of the following patterns : ["%Y%m%dT%H%M%SZ", "%Y-%m-%dT%H:%M:%SZ", 
                      "%Y%m%dT%H:%M:%SZ", "%Y-%m-%dT%H%M%SZ", 
                      "%Y%m%d", "%Y-%m-%d"]
        Arguments :
        input_date : string datetime
        """
        converted = None
        for date_format in INPUT_DATE_FORMATS:
            try:
                converted = datetime.datetime.strptime(input_date, date_format)
                break
            except ValueError:
                pass

        if converted is None:
            raise ValueError(f"Input value {input_date} did not match any of the following formats : {', '.join(INPUT_DATE_FORMATS)}")
        else:
            return True
                  
    def setup_request_for_dataset(self,
                                  dataset,
                                  eovs,
                                  query_start_date,
                                  query_end_date,
                                  query_min_lon, 
                                  query_min_lat, 
                                  query_max_lon, 
                                  query_max_lat,
                                  output_format):
        """
        Searches for EOVs in the provided ErddapDataset metadata.
        Checks if the query constraints is valid for the provided ErddapDataset object
        
        Arguments:
        dataset : ErddapDataset object
        eovs: (list(str)) : list of EOVs requested
        query_start_date / query_end_date : (str) start/end dates
        query_min/max_lon/lat : float
        output_format : (str)
        
        Returns a ErddapRequest object.
        """
        variables_found = []

        start = time.time()
        for eov in eovs:
            variables_found.extend(self.find_eov_in_dataset(dataset, eov, self.vocabularies[eov]))
        logger.debug(f"Looking for eovs in {dataset.name} took {time.time() - start} seconds with result : {variables_found}")

        # Filter out None values
        variables_found = [v for v in variables_found if v]

        if len(variables_found) == 0:
            logger.debug(f"Will discard dataset {dataset.name} because no variables found.")
            return None

        if dataset.covers_spatiotemporal_query(query_start_date, query_end_date, query_min_lon, query_min_lat, query_max_lon, query_max_lat):
            
            request = ErddapRequest(dataset,
                                    variables_found,
                                    query_min_lon,
                                    query_min_lat,
                                    query_max_lon,
                                    query_max_lat,
                                    query_start_date,
                                    query_end_date,
                                    output_format
                                   )
            return request
        else:
            return None
    
    def submit_request(self,
                       eovs,
                       query_start_date,
                       query_end_date,
                       query_min_lon, 
                       query_min_lat, 
                       query_max_lon, 
                       query_max_lat,
                       output_format) -> list:
        """
        Create Erddap queries according to arguments provided as input.
        Returns a list of ErddapRequest objects
        
        Arguments :
        eov: string or list of strings of the EOV(s) requested
        query_start_date: start datetime string
        query_end_date: end datetime string
        query_min_lon: min longitude float
        query_min_lat: min latitude float
        query_max_lon: max longitude float
        query_max_lat: max latitude float
        output_format: output format string
        """
        request_datasets = []
        
        if isinstance(eovs, str):
            eovs = [eovs]
        
        # Check inputs :
        # temporal :
        self.validate_datetime(query_start_date)
        self.validate_datetime(query_end_date)
        
        # Spatial inputs
        try:
            query_min_lon = float(query_min_lon)
            query_min_lat = float(query_min_lat)
            query_max_lon = float(query_max_lon)
            query_max_lat = float(query_max_lat)
        except ValueError:
            raise ValueError("At least one of input spatial coordinates provided is invalid. Provide float-convertible values.")
        
        # Output format requested
        if not output_format in ERDDAP_OUTPUT_FORMATS:
            raise ValueError(f"Provided output format \"{output_format}\" does not match any of available EOVs : {', '.join(ERDDAP_OUTPUT_FORMATS)}")
                
        # EOVs requested
        for eov in eovs:
            if not eov in EOV_LIST:
                raise ValueError(f"Provided eov \"{eov}\" does not match any of available EOVs : {', '.join(EOV_LIST)}")
        
#         query_variables = self.query_vocabularies(eov)


        response = BrokerResponse(eovs)

        with concurrent.futures.ThreadPoolExecutor(20) as executor:
            futures = []
            for dataset in self.datasets:
                futures.append(
                    executor.submit(self.setup_request_for_dataset,
                                    dataset,
                                    eovs,
                                    query_start_date,
                                    query_end_date,
                                    query_min_lon, 
                                    query_min_lat, 
                                    query_max_lon, 
                                    query_max_lat,
                                    output_format
                                   )
                )
                
            start = time.time()
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                if result is not None:
                    response.add_query(result)

            logger.debug(f"Handling dataset {dataset} took {time.time() - start} seconds")
        
        return response

            
class ErddapRequest:
    
    def __init__(self, 
                 dataset, 
                 query_variables, 
                 query_min_lon, 
                 query_min_lat, 
                 query_max_lon, 
                 query_max_lat, 
                 query_start_date, 
                 query_end_date,
                 output_format
                ):
        
        self.dataset = dataset
        self.query_variables = query_variables
        
        # If the protocol is griddap, we must adjust the query to fit the dataset bbox
        # otherwise, Erddap will return an error.
        # If the query is ok or the protocol is tabldap, we leave the min/max lon/lat values requested.
        if dataset.protocol == "griddap" and query_min_lon < dataset.min_lon:
            self.query_min_lon = dataset.min_lon
        else:
            self.query_min_lon = query_min_lon
        if dataset.protocol == "griddap" and query_min_lat < dataset.min_lat:
            self.query_min_lat = dataset.min_lat
        else:
            self.query_min_lat = query_min_lat
        if dataset.protocol == "griddap" and query_max_lon > dataset.max_lon:
            self.query_max_lon = dataset.max_lon
        else:
            self.query_max_lon = query_max_lon
        if dataset.protocol == "griddap" and query_max_lat > dataset.max_lat:
            self.query_max_lat = dataset.max_lat
        else:
            self.query_max_lat = query_max_lat
               
        self.query_start_date = query_start_date
        self.query_end_date = query_end_date
        self.output_format = output_format
        
        self.query_url = self.build_url(output_format=output_format)
            
        self.nc_data = None
    
    def build_url(self, output_format=""):
        """
        Build the data URL with defaults constraints provided at initialization.
        The output format chosen at request time can be modified.
        
        Keyword arguments:
        
        output_format -- string for an alternative output format.
        """
        query_string = ""
        
        if self.dataset.protocol == "tabledap":
            query_string = (f"{self.dataset.data_url}.{output_format}"
                            f"?time%2Clatitude%2Clongitude%2C{'%2C'.join(self.query_variables)}"
                            f"&time%3E={self.query_start_date}&time%3C={self.query_end_date}"
                            f"&latitude%3E={self.query_min_lat}&latitude%3C={self.query_max_lat}&longitude%3E={self.query_min_lon}&longitude%3C={self.query_max_lon}")

        else:            
            query_string = f"{self.dataset.data_url}.{output_format}?"
            for variable in self.query_variables:
                query_string += f'{variable}[({self.query_start_date}):1:({self.query_end_date})]'
                if len(self.dataset.wms_elevation_values) > 0:
                    query_string += f'[({self.dataset.wms_elevation_values[0]}):1:({self.dataset.wms_elevation_values[-1]})]'
                query_string += f'[({self.query_min_lat}):1:({self.query_max_lat})]'
                query_string += f'[({self.query_min_lon}):1:({self.query_max_lon})],'
                
            query_string = query_string.rstrip(',')
        return query_string

    def get_nc_data(self):
        if self.nc_data is None:
            # Tabledap offers ncCF output format which will provide with better dimensions :
            # if self.dataset.protocol == "tabledap":
            #     resp = requests.get(self.build_url(output_format="ncCF"))
            # Griddap will only offer nc output format :
            # else:
            #     resp = requests.get(self.build_url(output_format="nc"))
            resp = requests.get(self.build_url(output_format="nc"))
            self.nc_data = resp.content
        return io.BytesIO(self.nc_data)
    
    def to_pandas_dataframe(self):
        return self.to_xarray().to_dataframe()
    
    def to_xarray(self):
        return xr.open_dataset(self.get_nc_data())
    
    def download(self, output_format, filename=""):
        if filename == "":
            filename = f'{self.dataset.name}-{str(int(time.time()))}'
        with open(f"{filename}.{output_format}", 'wb') as out:
            resp = requests.get(self.build_url(output_format=output_format))
            out.write(resp.content)
        return True


class BrokerResponse():
    
    def __init__(self, eovs):
        self.eovs = eovs
        self.queries = None
        
    def __repr__(self):
        return f"BrokerResponse object with {len(self.queries)} results."
        
    def add_query(self, query):
        """
        Add a query to the response. The query is a Pandas DataFrame.
        It contains :
        - the query URL
        - the dataset global metadata
        - found variables in the dataset matching the EOVS
        - the ErddapRequest object containing the ErddapDataset object and the data access helpers
        """
        
        # Get the dataset url:
        query_line = pd.DataFrame(
            data={"query_url": [query.query_url]},
            index=[query.dataset.name]
        )
        # Add dataset global metadata to the dataframe
        dataset_global_attributes = query.dataset.metadata[
            query.dataset.metadata["Variable Name"] == "NC_GLOBAL"][
            ["Attribute Name", "Value"]
        ]
        for index, row in dataset_global_attributes.iterrows():
            query_line[row["Attribute Name"]] = row["Value"]
        
        # Add the ErddapRequest object:
        query_line["query_object"] = query
        
        # Add the EOVS columns with the variables found if any :
        for eov in EOV_LIST:
            if eov in query.dataset.found_eovs.keys():
                query_line[eov] = query.dataset.found_eovs[eov]
            else:
                query_line[eov] = ""
        
        if not isinstance(self.queries, pd.DataFrame):
            logger.debug(f"Creating DataFrame with dataset {query.dataset.name}")
            self.queries = query_line
        else:
            logger.debug(f"Adding dataset {query.dataset.name} to existing dataframe.")
            self.queries = self.queries.append(query_line)
    
    def get_dataset(self, dataset_id):
        '''
        Returns the ErddapRequest object for the provided dataset ID.
        
        Args:
        - dataset_id (str): the dataset_id as specified in BrokerResponse object
        '''
        if not dataset_id in self.queries.index:
            raise Exception(f"Dataset id {dataset_id} was not found in queries.")
            
        return self.queries.loc[dataset_id].query_object.dataset
    
    def get_dataset_EOVs_list(self, dataset_id):
        '''
        Returns a dict of the EOVs found for the request in the specified dataset ID.
        The dict structure is the following :
        {
            <EOV_NAME>: <Variable name>
        }
        Args:
        - dataset_id (str): the dataset_id as specified in BrokerResponse object
        '''
        if not dataset_id in self.queries.index:
            raise Exception(f"Dataset id {dataset_id} was not found in queries.")
        
        eov_dict = {}
        for eov in self.eovs:
            eov_dict[eov] = self.queries.loc[dataset_id][eov]
        
        return eov_dict
    
    def get_datasets_list(self):
        '''
        Returns a list of dataset IDs found for the specified query.
        '''
        return self.queries.index.tolist()
    
    def query_to_xarray(self, dataset_id, rename_vars=True, eov=""):
        """
        Get a query by the datasets ID & retrieve the result of the query in an xarray dataset.
        The resulting dataset will contain all the variables linked with the EOV(s) queried to the broker.
        The variables names are renamed by default with the EOV name, one can disable the renaming by switching rename_vars arg to False
        
        Args:
        - dataset_id (str): the dataset_id as specified in BrokerResponse object
        
        Optional args:
        - rename_vars (bool): rename original variables in the dataset by their P01 parameter; if False, keep the original names
        - eov (str): only retrieve one specific EOV in the xarray dataset.
        """
        if not dataset_id in self.queries.index:
            raise Exception(f"Dataset id {dataset_id} was not found in queries.")
        else:
            if eov != "":
                if eov not in EOV_LIST:
                    raise Exception(f"EOV {eov} not in allowed EOV list : {EOV_LIST}")
                eov_varname = self.queries.loc[dataset_id].query_object.dataset.found_eovs[eov]
                ds = self.queries.loc[dataset_id].query_object.to_xarray()[eov_varname]
            else:
                ds = self.queries.loc[dataset_id].query_object.to_xarray()
            # Todo : code rename_vars
            # if rename_vars:
            #     varname = 
        return ds
            
    def query_to_pandas_dataframe(self, dataset_id, eov=""):
        if not dataset_id in self.queries.index:
            raise Exception(f"Dataset id {dataset_id} was not found in queries.")
        else:
            if eov != "":
                if eov not in EOV_LIST:
                    raise Exception(f"EOV {eov} not in allowed EOV list : {EOV_LIST}")
                eov_varname = self.queries.loc[dataset_id].query_object.dataset.found_eovs[eov]
                return self.queries.loc[dataset_id].query_object.to_pandas_dataframe()[eov_varname]
            else:
                return self.queries.loc[dataset_id].query_object.to_pandas_dataframe()
            
    def query_to_file_download(self, dataset_id, output_format):
        if output_format not in ERDDAP_OUTPUT_FORMATS:
            raise Exception(f"Output format {output_format} not in available Erddap output formats :{ERDDAP_OUTPUT_FORMATS}")
        if not dataset_id in self.queries.index:
            raise Exception(f"Dataset id {dataset_id} was not found in queries.")
        else:
            return self.queries.loc[dataset_id].query_object.download(output_format)
            