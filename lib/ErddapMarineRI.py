import time
import pandas as pd
import numpy as np
import asyncio
from shapely.geometry import box
import logging

import requests
import xml.etree.ElementTree as ET
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
        self.data_url = None
        self.metadata_url = f"{erddap_server}/info/{self.name}/index.csv"
        self.protocol = None
        
        self.start_date = None
        self.end_date = None
        
        self.min_lon = None
        self.max_lon = None
        self.min_lat = None
        self.max_lat = None
        self.metadata = self.get_metadata()
        
        # Only used for griddap datasets
        self.wms_capabilities = None
        self.wms_time_values = []
        self.wms_elevation_values = []
        
        # Griddap datasets do not behave the same way as tabledap.
        # - Query syntax is different
        # - Querying outside of bounding box & timeframe will fail
        # - Query must include the elevation
        if self.metadata[self.metadata["Attribute Name"] == "cdm_data_type"].Value.iloc[0] == "Grid":
            self.protocol = "griddap"
            self.data_url = f"{erddap_server}/griddap/{self.name}"
            wms_capabilities = f"{erddap_server}/wms/{self.name}/request?service=WMS&request=GetCapabilities&version=1.3.0"
            data = requests.get(wms_capabilities).content
            root = ET.fromstring(data)
            
            # Erddap wms xml is not parseable with owslib, so we expect the dimensions to be available in the matrix below :
            for i in root[1][2][2]:
                if i.tag.endswith("Dimension"):
                    if i.attrib.get("name", "") == "time":
                        wms_time_values = [str(time_value)[0:10] for time_value in i.text.split(',')]
                        self.wms_time_values = np.unique(wms_time_values)
                        logger.debug(f"Griddap wms for {self.name} returned {len(self.wms_time_values)} time values.")
                    elif i.attrib.get("name", "") == "elevation":
                        wms_elevation_values = [float(elevation_value) for elevation_value in i.text.split(',')]
                        self.wms_elevation_values = [abs(ev) if ev < 0 else ev for ev in wms_elevation_values]
                        logger.debug(f"Griddap wms for {self.name} returned {len(self.wms_elevation_values)} elevation values.")
            # Also retrieve the bounding box for the dataset
            for i in root[1][2][2]:
                if i.tag == "{http://www.opengis.net/wms}BoundingBox":
                    self.min_lon = float(i.attrib.get("minx", -180.0))
                    self.min_lat = float(i.attrib.get("miny", -90.0))
                    self.max_lon = float(i.attrib.get("maxx", 180.0))
                    self.max_lat = float(i.attrib.get("maxy", 90.0))
                    
        else:
            self.protocol = "tabledap"
            self.data_url = f"{erddap_server}/tabledap/{self.name}"
        
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
            return False
    

    def covers_spatiotemporal_query(self, start, end, query_min_lon, query_min_lat, query_max_lon, query_max_lat) -> bool:
        """
        Checks if the dataset covers the time range between start and end.
        In order to avoid asking for the full period, this function splits the date range into a daily range.
        
        Returns:
        True if data is found for the time range.
        or False.
        """

        if self.protocol == "tabledap":
            time_query_order_by_limit = f"{self.data_url}.csv?time&time%3E={start}&time%3C={end}&latitude%3E={query_min_lat}&latitude%3C={query_max_lat}&longitude%3E={query_min_lon}&longitude%3C={query_max_lon}&orderByLimit(%22time/6months,1%22)"
            logger.debug(f"Will check spatiotemporal constraints from query {time_query_order_by_limit}")
            try:
                df = pd.read_csv(time_query_order_by_limit)
            except HTTPError as e:
                logger.debug(f"Query failed with exception {str(e)}")
                logger.debug(f"Failed query is : {time_query_order_by_limit}")
                return False

            if df.size > 0:
                return True
            else:
                return False
        # Griddap
        # Informations about time values are available in griddap WMS, we just check the information extracted from WMS getcapabilities
        else:
            # First check time values
            date_range = [str(i)[0:10] for i in np.arange(start, end,np.timedelta64(1,'D'), dtype='datetime64')]
            # find common dates between available time values and date range in query/
            # If not, abort querying the dataset
            if len(list(set(self.wms_time_values).intersection(date_range))) == 0:
                return False
            
            # Check spatial coverage of the query:
            if self.covers_geospatial_query(query_min_lon, query_min_lat, query_max_lon, query_max_lat):
                return True
