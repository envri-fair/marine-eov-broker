import time
import pandas as pd
import numpy as np
import asyncio
from shapely.geometry import box
import logging

from urllib.error import HTTPError

logger = logging.getLogger(__name__)
    

class ErddapDataset:
    def __init__(self, erddap_server, name):
        """
        Create a new Erddap dataset based on an existing dataset.
        The dataset metadata will be downloaded and extracted to provide spatio-temporal information on the dataset.
        
        Arguments:
        erddap_server: url string to the erddap server base
        name: dataset ID
        """
        self.server = erddap_server
        self.name = name
        self.metadata_url = f"{erddap_server}/info/{self.name}/index.csv"
        self.data_url = f"{erddap_server}/tabledap/{self.name}"
        
        self.start_date = None
        self.end_date = None
        
        self.min_lon = None
        self.max_lon = None
        self.min_lat = None
        self.max_lat = None
        self.metadata = self.get_metadata()
        self.parameters = {}
        
        for i, r in self.metadata[
                            (self.metadata["Attribute Name"] == "sdn_parameter_urn")
                        ].iterrows():
            self.parameters[r["Value"]] = r["Variable Name"]
        

    def __repr__(self):
        return f"{self.name} Erddap dataset at {self.server}"
        
    def get_metadata(self):
        start = time.time()
        metadata = pd.read_csv(self.metadata_url)
        
#         try:
#             self.min_lon = float(metadata[(metadata["Attribute Name"] == "geospatial_lon_min")]["Value"].item())
#         except ValueError:
#             pass
        
#         try:
#             self.min_lat = float(metadata[(metadata["Attribute Name"] == "geospatial_lat_min")]["Value"].item())
#         except ValueError:
#             pass
        
#         try:
#             self.max_lon = float(metadata[(metadata["Attribute Name"] == "geospatial_lon_max")]["Value"].item())
#         except ValueError:
#             pass
        
#         try:
#             self.max_lat = float(metadata[(metadata["Attribute Name"] == "geospatial_lat_max")]["Value"].item())
#         except ValueError:
#             pass
        
#         try:
#             self.start_date = np.datetime64(metadata[(metadata["Attribute Name"] == "time_coverage_start")]["Value"].item())
#         except ValueError:
#             pass
        
#         try:
#             self.end_date = np.datetime64(metadata[(metadata["Attribute Name"] == "time_coverage_end")]["Value"].item())
#         except ValueError:
#             pass
        
#         print(f"Took {time.time() - start} seconds to load {self.name} metadata.")
#         self.metadata = metadata
        return metadata

    
    def covers_geospatial_query(self, query_min_lon, query_min_lat, query_max_lon, query_max_lat):
        # If metadata does not provide geospatial information, the dataset will be queried anyway
        if None in [self.min_lon, self.min_lat, self.max_lon, self.max_lat]:
            return True
        dataset_bbox = box(self.min_lon, self.min_lat, self.max_lon, self.max_lat)
        query_bbox = box(query_min_lon, query_min_lat, query_max_lon, query_max_lat)
        
        if query_bbox.intersection(dataset_bbox) or query_bbox.contains(dataset_bbox):
            return True
        else:
#             print(f"Dataset {self.name} does not fit bbox {query_min_lon, query_min_lat} - {query_max_lon, query_max_lat}.")
            return False
    

    def covers_spatiotemporal_query(self, start, end, query_min_lon, query_min_lat, query_max_lon, query_max_lat) -> bool:
        """
        Checks if the dataset covers the time range between start and end.
        In order to avoid asking for the full period, this function splits the date range into a daily range.
        
        Returns:
        True if data is found for the time range.
        or False.
        """
        
#         date_range = [str(i) for i in np.arange(start, end,np.timedelta64(1,'D'), dtype='datetime64')]
#         for k, i in enumerate(date_range):
#             start = time.time()
#             time_query_order_by_max = f"{self.data_url}.csv?time&time%3E={i}&time%3C={date_range[k+1]}&latitude%3E={query_min_lat}&latitude%3C={query_max_lat}&longitude%3E={query_min_lon}&longitude%3C={query_max_lon}&orderByMin(%22time,time/1day,time%22)"
#             try:
#                 df = pd.read_csv(time_query_order_by_max)
#             except HTTPError:
#                 logger.warn(f"Issue with http request : {time_query_order_by_max}")
                
#             logger.info(f"query {time_query_order_by_max} :: {time.time() - start}")
#             if df.size > 0:
#                 return True
#             else:
#                 return False
        time_query_order_by_limit = f"{self.data_url}.csv?time&time%3E={start}&time%3C={end}&latitude%3E={query_min_lat}&latitude%3C={query_max_lat}&longitude%3E={query_min_lon}&longitude%3C={query_max_lon}&orderByLimit(%22time/6months,1%22)"
        logger.debug(f"Will check spatiotemporal constraints from query {time_query_order_by_limit}")
        try:
            df = pd.read_csv(time_query_order_by_limit)
        except HTTPError as e:
            logger.debug(f"Query failed with exception {str(e)}")
            return False
        
        if df.size > 0:
            return True
        else:
            return False
