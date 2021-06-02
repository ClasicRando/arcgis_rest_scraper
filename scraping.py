import json
from typing import List, Tuple, Any
from numpy import format_float_positional
from requests import Session
from math import ceil
from json import dumps


def fetch(session: Session, url: str) -> dict:
    """ Function used to get json response from url GET request """
    with session.get(url) as response:
        data = response.json()
        if response.status_code != 200:
            raise ConnectionError(f"Failed to retrieve using url, {url}")
        return data


def max_min_query(oid_field: str) -> str:
    """
    Helper query postfix to get max and min Oid values. Given an Oid field name, can be added to the
    end of a base url
    """
    return f'/query?outStatistics=%5B%0D%0A+%7B%0D%0A++++"statisticType"%3A+"max"%2C%0D%0A' \
           f'++++"onStatisticField' \
           f'"%3A+"{oid_field}"%2C+++++%0D%0A++++"outStatisticFieldName"%3A+"MAX_VALUE"%0D%0A' \
           f'++%7D%2C%0D%0A++%7B%0D%0A++++"statisticType"%3A+"min"%2C%0D%0A++++"onStatisticField' \
           f'"%3A+"{oid_field}"%2C+++++%0D%0A++++"outStatisticFieldName"%3A+"MIN_VALUE"%0D%0A' \
           f'++%7D%0D%0A%5D&f=json'


def convert_json_value(x: Any) -> str:
    """
    Function used to transform elements of a DataFrame to string based upon the type of object

    This is the default implementation of the function that a FileLoader will use if the user does
    not supply their own function

    Parameters
    ----------
    x : Any
        an object of Any type stored in a DataFrame
    Returns
    -------
     string conversion of the values based upon it's type
    """
    if isinstance(x, str):
        return x
    elif isinstance(x, int):
        return str(x)
    # Note that for JSON numbers, some truncation might occur during json load into python dict
    elif isinstance(x, float):
        return format_float_positional(x).rstrip(".")
    elif isinstance(x, bool):
        return "TRUE" if x else "FALSE"
    elif x is None:
        return ""
    elif x is list:
        return json.dumps(x)
    else:
        return str(x)


def handle_record(geo_type: str, feature: dict) -> List[str]:
    """
    Parameters
    ----------
    geo_type : str
        geometry type from the RestMetadata object
    feature : str
        json object from the query's feature json array
    Return
    ------
    feature object converted to List[str] with geometry is applicable
    """
    # collect all values from the attributes key and convert them to string
    record = [
        convert_json_value(value).strip()
        for value in feature["attributes"].values()
    ]
    # If geometry is point, get X and Y and add to the record. If no geometry present, default to a
    # blank X and Y
    if geo_type == "esriGeometryPoint":
        record += [
            convert_json_value(point).strip()
            for point in feature.get("geometry", {"x": "", "y": ""}).values()
        ]
    # If geometry is multi point, join coordinates into a list of points using json list notation
    # and add to the record
    elif geo_type == "esriGeometryMultipoint":
        record += [convert_json_value(feature["geometry"]["points"]).strip()]
    # If geometry is Polygon get the rings and add the value to the record
    elif geo_type == "esriGeometryPolygon":
        record += [convert_json_value(feature["geometry"]["rings"]).strip()]
    # Other geometries could exist but are not currently handled
    return record


class RestMetadata:
    """
    Data class for the backing information for an ArcGIS REST server and how to query the service

    Parameters
    ----------
    url : str
        Base url of the service. Used to collect data and generate queries

    Attributes
    ----------
    name: str
        Name of the REST service. Used to generate the file name of the output
    source_count : int
        Number of records found within the service
    max_record_count : int
        Max number of records the service allows to be scraped in a single query. This is only used
        for generating queries if it's less than 10000. A property of the class, scrape_count,
        provides the true count that is use for generating queries
    pagination: bool
        Does the source provide the ability to page results. Easiest query generation
    stats: bool
        Does the source provide the ability to query statistics about the data. Used to get the max
        and min Oid field values to generate queries
    server_type : str
        Property of the server that denotes if geometry is available for each feature
    geo_type : str
        Geometry type for each feature. Guides how geometry is stored in CSV
    fields : List[str]
        Field names for each feature
    oid_field : str
        Name of unique identifier field for each feature. Used when pagination is not provided
    max_min_oid: Tuple[int, int]
        max and min Oid field values if that method is required to query all features. Defaults to
        -1 values if not required
    inc_oid : bool
        Is the Oid fields a sequential number. Checked using source_count and max Oid values
    """
    def __init__(self, url: str):
        self.url = url
        count_query = "/query?where=1%3D1&returnCountOnly=true&f=json"
        field_query = "?f=json"
        urls = [url + count_query, url + field_query]

        with Session() as session:
            try:
                for res in [fetch(session, url) for url in urls]:
                    if "count" in res:
                        self.source_count = res["count"]
                    else:
                        advanced_query = res.get("advancedQueryCapabilities", dict())
                        self.server_type = res["type"]
                        self.name = res["name"]
                        self.max_record_count = int(res["maxRecordCount"])
                        if advanced_query:
                            self.pagination = advanced_query.get("supportsPagination", False)
                        else:
                            self.pagination = res.get("supportsPagination", False)
                        if advanced_query:
                            self.stats = advanced_query.get("supportsStatistics", False)
                        else:
                            self.stats = res.get("supportsStatistics", False)
                        self.geo_type = res.get("geometryType", "")
                        self.fields = [
                            field["name"] for field in res["fields"]
                            if field["name"] != "Shape" and field["type"] != "esriFieldTypeGeometry"
                        ]
                        if self.geo_type == "esriGeometryPoint":
                            self.fields += ["X", "Y"]
                        elif self.geo_type == "esriGeometryMultipoint":
                            self.fields += ["POINTS"]
                        elif self.geo_type == "esriGeometryPolygon":
                            self.fields += ["RINGS"]
                        oid_fields = [
                            field["name"] for field in res["fields"]
                            if field["type"] == "esriFieldTypeOID"
                        ]
                        if oid_fields:
                            self.oid_field = oid_fields[0]
                if self.stats and self.oid_field and not self.pagination:
                    res = fetch(session, self.url + max_min_query(self.oid_field))
                    attributes = res["features"][0]["attributes"]
                    self.max_min_oid = (attributes["MAX_VALUE"], attributes["MIN_VALUE"])
                    diff = self.max_min_oid[0] - self.max_min_oid[1] + 1
                    self.inc_oid = diff == self.source_count
            except ConnectionError as ex:
                self.source_count = -1
                self.name = ex.strerror

    @property
    def scrape_count(self) -> int:
        """ Used for generating queries. Caps feature count per query to 10000 """
        return self.max_record_count if self.max_record_count <= 10000 else 10000

    @property
    def oid_query_count(self) -> int:
        """ Number of queries needed if Oid field used """
        return ceil((self.max_min_oid[0] - self.max_min_oid[1] + 1) / self.scrape_count)

    @property
    def pagination_query_count(self) -> int:
        """ Number of queries needed if pagination used """
        return ceil(self.source_count / self.scrape_count)

    @property
    def is_table(self) -> bool:
        """ Checks if the service is a Table type (ie no geometry provided) """
        return self.server_type == "TABLE"

    @property
    def geo_text(self) -> str:
        """
        String added to the queries for geometry. If service is a Table then empty string.
        Adds an out spatial reference for geometry to NAD83. Might need to be changed in the future

        TO-DO
        ----
        - add ability to provide spatial reference override for non-NA services
        """
        return "" if self.is_table else f"&geometryType={self.geo_type}&outSR=4269"

    @property
    def json_text(self) -> str:
        """ Converts class attributes to a dict for displaying details as JSON text """
        return dumps(
            {
                "URL": self.url,
                "Name": self.name,
                "Source Count": self.source_count,
                "Max Record Count": self.max_record_count,
                "Pagination": self.pagination,
                "Stats": self.stats,
                "Server Type": self.server_type,
                "Geometry Type": self.geo_type,
                "Fields": self.fields,
                "OID Fields": self.oid_field,
                "Max Min OID": self.max_min_oid,
                "Incremental OID": self.inc_oid
            },
            indent=4
        )

    @property
    def queries(self) -> List[str]:
        """
        Get all the queries for this service. Returns empty list when no query method available

        TO-DO
        ----
        - find other query methods when current methods exhausted
        """
        if self.pagination:
            return [
                self.url + self.get_pagination_query(i)
                for i in range(self.pagination_query_count)
            ]
        elif self.oid_field and self.stats:
            return [
                self.url + self.get_oid_query(
                    self.max_min_oid[1] + (i * self.scrape_count)
                )
                for i in range(self.oid_query_count)
            ]
        else:
            return []

    def get_pagination_query(self, query_num: int) -> str:
        """
        Generate query for service when pagination is supported using query_num to get offset
        """
        return f"/query?where=1+%3D+1&resultOffset={query_num * self.scrape_count}" \
               f"&resultRecordCount={self.scrape_count}{self.geo_text}&outFields=*&f=json"

    def get_oid_query(self, min_oid: int) -> str:
        """
        Generate query for service when Oid is used using a starting Oid number and an offset
        """
        return f"/query?where={self.oid_field}+>%3D+{min_oid}+and+" \
               f"{self.oid_field}+<%3D+{min_oid + self.scrape_count - 1}" \
               f"{self.geo_text}&outFields=*&f=json"
