import sys
import time
import re
import traceback
import os
from PyQt5.QtWidgets import (QMainWindow, QApplication, QFormLayout, QLineEdit, QWidget,
                             QPushButton, QPlainTextEdit, QProgressBar, QLabel)
from PyQt5.QtCore import pyqtSlot, pyqtSignal, QRunnable, QThreadPool, QObject
from functools import partial
from typing import Optional, List, Tuple
from requests import get, Session
from pandas import DataFrame, read_csv
from math import ceil
from json import dumps


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

        TODO
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

        TODO
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


class MainWindow(QMainWindow):

    def __init__(self):
        """
        Main window of the application. Simple setup and interface since very little is needed
        """
        super(MainWindow, self).__init__()

        # Initialize attributes to default values
        self.url = ""
        self.rest_metadata: Optional[RestMetadata] = None
        self.current_queries_running = 0
        self.thread_pool = QThreadPool()
        self.start = 0.0

        # Setup layout and widgets
        form_layout = QFormLayout()
        txt_url = QLineEdit()
        txt_url.textChanged.connect(self.url_update)
        btn_metadata = QPushButton("Fetch Metadata")
        btn_metadata.clicked.connect(self.get_metadata)
        btn_run = QPushButton("Fetch Data")
        btn_run.clicked.connect(self.get_data)
        self.txt_output = QPlainTextEdit()
        self.scraping_progress = QProgressBar()
        self.scraping_progress.setVisible(False)
        self.progress_message = QLabel()
        self.progress_message.setVisible(False)

        # Add widgets to main layout
        form_layout.addRow("URL", txt_url)
        form_layout.addWidget(btn_metadata)
        form_layout.addWidget(btn_run)
        form_layout.addWidget(self.scraping_progress)
        form_layout.addWidget(self.progress_message)
        form_layout.addWidget(self.txt_output)

        # create and set main widget
        main_widget = QWidget()
        main_widget.setLayout(form_layout)
        self.setCentralWidget(main_widget)
        self.setWindowTitle("REST Scraper")

    @pyqtSlot(str)
    def url_update(self, text: str) -> None:
        """ Everytime the url textbox is updated we update the backing attribute """
        self.url = text

    @pyqtSlot()
    def get_metadata(self):
        """ Get server metadata using Runnable in thread pool """
        # If thread pool is working, then this action does nothing
        if self.thread_pool.activeThreadCount() > 0:
            return

        # Setup window to show progress with proper values
        self.toggle_progress()
        self.progress_message.setText("Fetching Metadata")
        self.scraping_progress.setMaximum(0)

        # Create worker and connect signals
        worker = MetadataFetcher(self.url)
        worker.signals.result.connect(self.post_metadata)
        worker.signals.error.connect(self.display_error)

        # Start process
        self.thread_pool.start(worker)

    @pyqtSlot()
    def get_data(self):
        """ Scrape all features from service. Run all queries concurrently """
        if self.thread_pool.activeThreadCount() > 0:
            return
        if self.rest_metadata is None:
            return
        if self.rest_metadata.source_count == -1:
            return

        # Get queries needed for scraping
        queries = self.rest_metadata.queries
        self.current_queries_running = len(queries)

        # Setup window to show progress with proper values
        self.toggle_progress()
        self.scraping_progress.setMaximum(self.current_queries_running + 1)
        self.progress_message.setText("Fetching query data into temp files")

        # Set start time
        self.start = time.time()

        # For each query generate a worker and start process
        for i, query in enumerate(queries):
            worker = QueryFetcher(query, i + 1, self.rest_metadata)
            worker.signals.error.connect(self.display_error)
            worker.signals.finished.connect(self.post_query)
            self.thread_pool.start(worker)

    @pyqtSlot()
    def post_query(self):
        """
        Slot called after query has been handled. Changes some attributes and if all queries done
        then starting finishing process
        """
        # Update current running queries and progress bar value
        self.current_queries_running -= 1
        self.scraping_progress.setValue(self.scraping_progress.value() + 1)

        # If all queries are done then start Consolidator to collect temp files and generate result
        # CSV
        if self.current_queries_running == 0:
            self.progress_message.setText("Consolidating data and cleaning temp files")
            worker = DataConsolidator(self.rest_metadata.name, self.start)
            worker.signals.error.connect(self.display_error)
            worker.signals.result.connect(self.post_scrape)
            self.thread_pool.start(worker)

    @pyqtSlot(object)
    def post_scrape(self, scrape_stats: str) -> None:
        """ Handles Consolidator finish signal. Set output on display with stats """
        self.txt_output.setPlainText(scrape_stats)
        self.toggle_progress()

    @pyqtSlot(object)
    def post_metadata(self, result: tuple) -> None:
        """ Handles result of metadata fetching. Uses result and prints output to display """
        # Separate result tuple
        self.rest_metadata, diff = result
        text = f"Metadata\n--------\n{self.rest_metadata.json_text}\n" \
               f"Took {diff} seconds"

        # Update output
        self.txt_output.setPlainText(text)
        self.toggle_progress()

    def toggle_progress(self):
        """ Change current progress visibility to the opposite (eg if showing then hide widgets) """
        state = self.scraping_progress.isVisible()
        self.scraping_progress.setVisible(not state)
        self.progress_message.setVisible(not state)

    @pyqtSlot(str)
    def display_error(self, trace_back: str) -> None:
        """ Use traceback from exception to set the output display """
        self.txt_output.setPlainText(trace_back)


class WorkerSignals(QObject):
    """
    Holds signals for workers since they need to reside in a QObject
    """
    finished = pyqtSignal()
    error = pyqtSignal(str)
    result = pyqtSignal(object)


class QueryFetcher(QRunnable):

    def __init__(self, query: str, query_num: int, rest_metadata: RestMetadata):
        """
        Runnable action to fetch a query result from the REST service and write the result to a temp
        file

        Parameters
        ----------
        query : str
            REST service query for the worker to execute and collect the features
        query_num : int
            index from list of queries created by RestMetadata
        rest_metadata :
            pointer to Metadata. Attributes used while querying and creating temp file
        """
        super(QueryFetcher, self).__init__()
        self.query = query
        self.query_num = query_num
        self.rest_metadata = rest_metadata
        self.signals = WorkerSignals()

    @pyqtSlot()
    def run(self) -> None:
        """
        Action of runnable. Gets response from query and parses the features into a temp file
        """
        try:
            # GET request using query
            response = get(self.query)
            num_records = 0

            # If successful query then consolidate features into DataFrame and write the results to
            # a temp file
            if response.status_code == 200:
                # Map features from response using handle_record and geo_type
                data = list(
                    map(
                        partial(handle_record, self.rest_metadata.geo_type),
                        response.json()["features"]
                    )
                )
                # Create Dataframe using records and fields
                df = DataFrame(
                    data=data,
                    columns=self.rest_metadata.fields,
                    dtype=str
                )
                # Get the number of records in DataFrame
                num_records = len(df.index)
                # Write DataFrame to temp CSV file
                df.to_csv(
                    f"{os.getcwd()}\\temp_files\\{self.rest_metadata.name}_{self.query_num}.csv",
                    mode="w",
                    index=False
                )
        except:
            self.signals.error.emit(traceback.format_exc())
        else:
            self.signals.result.emit(num_records)
        finally:
            self.signals.finished.emit()


class DataConsolidator(QRunnable):

    def __init__(self, service_name: str, start: float):
        """
        Runnable action to join all temp files created by the query fetching workers into a single
        CSV result

        Parameters
        ----------
        service_name : str
            Name of the REST service. Used to find temp files and name result file
        start : float
            Starting time of the scraping operation. Used to compare starting and ending time
        """
        super(DataConsolidator, self).__init__()
        self.service_name = service_name
        self.start = start
        self.signals = WorkerSignals()

    @pyqtSlot()
    def run(self) -> None:
        """
        Action of runnable. Gets all temp files for the service and consolidates then into 1 file.
        Cleans temp files
        """
        try:
            # Initializes variables
            num_records = 0
            is_first = True
            # Finds all the temp files needed using the temp_files folder and the service name in a
            # regex to filter
            temp_files = [
                file
                for file in os.listdir(f"{os.getcwd()}\\temp_files")
                if re.search(f"{self.service_name}_\\d+\\.csv", file)
            ]
            # For each temp file, read it into a DataFrame and write that DataFrame to the result
            # file
            for f in temp_files:
                df = read_csv(f"{os.getcwd()}\\temp_files\\{f}")
                # Increment the number of records
                num_records += len(df.index)
                # Write the DataFrame to the result file. Start by writing to file and including
                # header but after the first file, switch to append mode and do not write a header
                # line
                df.to_csv(
                    f"{self.service_name}.csv",
                    mode="w" if is_first else "a",
                    index=False,
                    header=is_first
                )
                is_first = False
                os.remove(f"{os.getcwd()}\\temp_files\\{f}")
        except:
            self.signals.error.emit(traceback.format_exc())
        else:
            # Construct a result message showing records in result file and time to complete all
            # scraping operations
            text = trim_indent(f"""
            Output
            ------
            File: {self.service_name}.csv
            Record Count: {num_records}
            Run Time: {time.time() - self.start} seconds
            """)
            self.signals.result.emit(text)


class MetadataFetcher(QRunnable):

    def __init__(self, url: str):
        """
        Runnable action to fetch a services metadata used for scraping the service
        Parameters
        ----------
        url : str
            Base url of the ArcGIS REst server
        """
        super(MetadataFetcher, self).__init__()
        self.url = url
        self.signals = WorkerSignals()

    @pyqtSlot()
    def run(self) -> None:
        """
        Action of runnable. Gets a RestMetadata object from the base url.

        Object instantiation is in worker since the init of the class contain GET requests and may
        block the main thread
        """
        start = time.time()
        try:
            result = RestMetadata(self.url)
        except:
            self.signals.error.emit(traceback.format_exc())
        else:
            self.signals.result.emit((result, time.time() - start))


def trim_indent(text: str) -> str:
    """ Formats multiline text to remove the minimal/common indent in the text """
    result = text.strip()
    indents = min(len(match.group(0).replace("\t", "    "))
                  for match in re.finditer(r"^\s+", result, re.MULTILINE))
    return re.sub(" {" + str(indents) + "}", "", result)


def fetch(session: Session, url: str) -> dict:
    """ Function used to get json response from url GET request """
    with session.get(url) as response:
        data = response.json()
        if response.status_code != 200:
            print(f"Failed to retrieve using url, {url}")
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
    record = [str(value).strip() for value in feature["attributes"].values()]
    # If geometry is point, get X and Y and add to the record. If no geometry present, default to a
    # blank X and Y
    if geo_type == "esriGeometryPoint":
        record += [
            str(point).strip()
            for point in feature.get("geometry", {"x": "", "y": ""}).values()
        ]
    # If geometry is multi point, join coordinates into a list of points using json list notation
    # and add to the record
    elif geo_type == "esriGeometryMultipoint":
        record += [
            "[" + "],[".join((str(coordinate).strip() for coordinate in point)) + "]"
            for point in feature["geometry"]["points"]
        ]
    # If geometry is Polygon get the rings and add the value to the record
    elif geo_type == "esriGeometryPolygon":
        record += [str(feature["geometry"]["rings"][0]).strip()]
    # Other geometries could exist but are not currently handled
    return record


if __name__ == "__main__":
    # If temp_files folder not found in current working directory, create the directory
    if not os.path.isdir(f"{os.getcwd()}\\temp_files"):
        os.mkdir(f"{os.getcwd()}\\temp_files")
    # Create PyQt application
    app = QApplication(sys.argv)
    # Create and show main window
    window = MainWindow()
    window.show()
    # Run application
    try:
        sys.exit(app.exec_())
    except Exception as ex:
        print(ex)
