from . import ErddapMarineRI
import pandas as pd
import requests
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

logger = logging.getLogger(__name__)


class MarineBroker:
    
    vocabularies_server = "https://vocab.nerc.ac.uk/sparql/"
    ERDDAP_OUTPUT_FORMATS = ["csv", "geoJson", "json", "nc", "ncCF", "odvTxt"]
    EOV_LIST = ['EV_OXY', 'EV_SEATEMP', 'EV_SALIN', 'EV_CURR', 'EV_CHLA', 'EV_CO2', 'EV_NUTS']
    DEFAULT_ERDDAP_SERVERS = {
        "https://www.ifremer.fr/erddap": ["ArgoFloats", "ArgoFloats-synthetic-BGC", "SDC_GLO_AGG_V2", "SDC_GLO_CLIM_TS_V2_1", "SDC_GLO_CLIM_TS_V2_2"],
        "http://erddap.emso.eu/erddap": None,
        "https://erddap.icos-cp.eu/erddap": None
    }

    def __init__(self, erddap_servers=DEFAULT_ERDDAP_SERVERS):
        """Create a new broker and automatically scan Erddap servers provided.
        
        Keyword arguments:
        erddap_servers -- List of url strings for each erddap server to query. (default ["https://www.ifremer.fr/erddap", "http://erddap.emso.eu/erddap", "https://erddap.icos-cp.eu/erddap"]
        
        """
#         self.erddap_servers = erddap_servers        
        self.datasets_list = []
        self.datasets = []
        self.vocabularies = {}

        
        with concurrent.futures.ThreadPoolExecutor(10) as executor:
            futures = []
            for eov in self.EOV_LIST:
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
        
#         for erddap_server in self.erddap_servers:
#             self.find_datasets_in_erddap_server(erddap_server)
        
        with concurrent.futures.ThreadPoolExecutor(5) as executor:
            futures = []
            for erddap_server, dataset_id in self.datasets_list:
                futures.append(executor.submit(self.get_dataset_metadata, erddap_server, dataset_id))
                
            for future in concurrent.futures.as_completed(futures):
                self.datasets.append(future.result())
    

            
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
        
    def get_dataset_metadata(self, erddap_server, dataset_id):
        start = time.time()
        erddap_dataset = ErddapMarineRI.ErddapDataset(erddap_server, dataset_id)
        logger.debug(f"Loaded {dataset_id} in {time.time() - start} seconds.")
        return erddap_dataset  
    
    def build_vocabularies(self, eov) -> dict:
        start = time.time()
        vocabularies = {}
        
#         for EOV in self.EOV_LIST:
        eov_result = self.query_vocabularies(eov)

#         self.vocabularies[eov] = eov_result
        
#         logger.debug(eov_result)
        logger.debug(f"Gathering vocabularies took {time.time() - start} seconds.")
        return eov, eov_result
#         self.vocabularies =  vocabularies
        
    
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

    
    def find_eov_in_dataset(self, dataset, eov_vocabs):
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
        
        found_vars = [dataset.parameters[i] for i in P01 if i in dataset.parameters.keys()]
        
        if len(found_vars) == 0:
            found_vars = [dataset.parameters[i] for i in P02 if i in dataset.parameters.keys()]
            
        if found_vars is not None:
            return np.unique(found_vars)
        else:
            return None
#         for v in eov_vocabs["results"]["bindings"]:
#             try:
#                 found_varname = dataset.metadata[
#                     (dataset.metadata["Attribute Name"] == "sdn_parameter_urn") &
#                     (dataset.metadata["Value"] == v["P01notation"]["value"])
#                 ]["Variable Name"].unique()
#                 if len(found_varname) > 0:
#                     logger.debug(f"Found {found_varname[0]} in {dataset.name}")
#                     logger.debug(f"sdn_parameter_urn is {v['P01notation']['value']}")
#                     return found_varname[0]
#             except KeyError:
#                 logger.debug(dataset.name)
            
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
        variables_found = []

        start = time.time()
        for eov in eovs:
            variables_found.extend(self.find_eov_in_dataset(dataset, self.vocabularies[eov]))
        logger.debug(f"Looking for eovs in {dataset.name} took {time.time() - start} seconds")

        # Filter out None values
        variables_found = [v for v in variables_found if v]

        if len(variables_found) == 0:
            return None

        if dataset.covers_spatiotemporal_query(query_start_date, query_end_date, query_min_lon, query_min_lat, query_max_lon, query_max_lat):
            request = ErddapRequest(dataset.name,
                                    dataset.metadata,
                                    dataset.data_url,
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
            float(query_min_lon)
            float(query_min_lat)
            float(query_max_lon)
            float(query_max_lat)
        except ValueError:
            raise ValueError("At least one of input spatial coordinates provided is invalid. Provide float-convertible values.")
        
        # Output format requested
        if not output_format in self.ERDDAP_OUTPUT_FORMATS:
            raise ValueError(f"Provided output format \"{output_format}\" does not match any of available EOVs : {', '.join(self.ERDDAP_OUTPUT_FORMATS)}")
                
        # EOVs requested
        for eov in eovs:
            if not eov in self.EOV_LIST:
                raise ValueError(f"Provided eov \"{eov}\" does not match any of available EOVs : {', '.join(self.EOV_LIST)}")
        
#         query_variables = self.query_vocabularies(eov)


        queries = []

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
                    queries.append(result)
#             variables_found = []
            
#             with concurrent.futures.ThreadPoolExecutor(20) as executor:
#                 futures = []
#                 for eov in eovs:
#                     futures.append(executor.submit(self.find_eov_in_dataset, dataset, self.vocabularies[eov]))
                    
#                 for future in concurrent.futures.as_completed(futures):
#                     variables_found.append(future.result())
#             logger.debug(f"Looking for eovs in {dataset.name} took {time.time() - start} seconds")
# #                 variables_found.append(self.find_eov_in_dataset(dataset, self.vocabularies[eov]))
                
            
#             variables_found = [v for v in variables_found if v]
            
#             if len(variables_found) == 0:
#                 continue
            
#             if dataset.covers_spatiotemporal_query(query_start_date, query_end_date, query_min_lon, query_min_lat, query_max_lon, query_max_lat):
#                 request = ErddapRequest(dataset.name,
#                                         dataset.metadata,
#                                         dataset.data_url,
#                                         variables_found,
#                                         query_min_lon,
#                                         query_min_lat,
#                                         query_max_lon,
#                                         query_max_lat,
#                                         query_start_date,
#                                         query_end_date,
#                                         output_format
#                                        )
#                 queries.append(request)
            logger.debug(f"Handling dataset {dataset} took {time.time() - start} seconds")
        
        return queries
                

            
            
class ErddapRequest:
    
    def __init__(self, 
                 dataset_name,
                 dataset_metadata,
                 data_url, 
                 query_variables, 
                 query_min_lon, 
                 query_min_lat, 
                 query_max_lon, 
                 query_max_lat, 
                 query_start_date, 
                 query_end_date,
                 output_format
                ):
        
        self.dataset_name = dataset_name
        self.dataset_metadata = dataset_metadata
        self.data_url = data_url
        self.query_variables = query_variables
        self.query_min_lon = query_min_lon
        self.query_min_lat = query_min_lat
        self.query_max_lon = query_max_lon
        self.query_max_lat = query_max_lat
        self.query_start_date = query_start_date
        self.query_end_date = query_end_date
        self.output_format = output_format
        try:
            self.query_url = self.build_url()
        except:
            logger.error(self.query_variables)
        
    
    def build_url(self, output_format=None):
        """
        Build the data URL with defaults constraints provided at initialization.
        The output format chosen at request time can be modified.
        
        Keyword arguments:
        
        output_format -- string for an alternative output format.
        """
        if output_format is None:
            output_format = self.output_format
        
        return (f"{self.data_url}.{output_format}"
                 f"?time%2Clatitude%2Clongitude%2C{'%2C'.join(self.query_variables)}"
                 f"&time%3E={self.query_start_date}&time%3C={self.query_end_date}"
                 f"&latitude%3E={self.query_min_lat}&latitude%3C={self.query_max_lat}&longitude%3E={self.query_min_lon}&longitude%3C={self.query_max_lon}")

    
    def to_pandas_dataframe(self):
        return pd.read_csv(self.build_url(output_format="csv"))
    
    def to_xarray(self):
        resp = requests.get(self.build_url(output_format="ncCF"))
        data = io.BytesIO(resp.content)
        return xr.open_dataset(data)
                                          