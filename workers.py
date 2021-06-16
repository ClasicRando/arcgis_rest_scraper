import time
import re
import traceback
import os

import requests.exceptions
from PyQt5.QtCore import pyqtSlot, pyqtSignal, QRunnable, QObject
from functools import partial
from requests import get
from pandas import DataFrame, read_csv
from scraping import RestMetadata, handle_record


def trim_indent(text: str) -> str:
    """ Formats multiline text to remove the minimal/common indent in the text """
    result = text.strip()
    indents = min(len(match.group(0).replace("\t", "    "))
                  for match in re.finditer(r"^\s+", result, re.MULTILINE))
    return re.sub(" {" + str(indents) + "}", "", result)


class WorkerSignals(QObject):
    """
    Holds signals for workers since they need to reside in a QObject
    """
    finished = pyqtSignal()
    error = pyqtSignal(str)
    result = pyqtSignal(object)


class QueryFetcher(QRunnable):

    def __init__(self,
                 query: str,
                 query_num: int,
                 rest_metadata: RestMetadata,
                 max_tries: int = 10):
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
        self.max_tries = max_tries
        self.signals = WorkerSignals()

    @pyqtSlot()
    def run(self) -> None:
        """
        Action of runnable. Gets response from query and parses the features into a temp file
        """
        try:
            invalid_response = True
            json_response = dict()
            try_number = 1

            while invalid_response:
                try:
                    # GET request using query
                    response = get(self.query)
                    invalid_response = response.status_code != 200
                    if invalid_response:
                        print(f"Error: {self.query} got this response:\n{response.content}")

                    json_response = response.json()
                    response.close()
                    # Check to make sure JSON response has features
                    if "features" not in json_response.keys():
                        # No features in response and JSON has an error code, retry query
                        if "error" in json_response.keys():
                            print("Request had an error... trying again")
                            invalid_response = True
                            # Sleep to give the server sometime to handle the request again
                            time.sleep(10)
                            try_number += 1
                        # No features in response and no error code. Raise error which terminates
                        # all operations
                        else:
                            raise KeyError("Response was not an error but no features found")
                except requests.exceptions.ConnectionError:
                    time.sleep(10)
                    invalid_response = True
                    try_number += 1
                if try_number > self.max_tries:
                    raise Exception(f"Too many tries to fetch query ({self.query})")

            # Once query is successful, Map features from response using handle_record and geo_type
            data = list(
                map(
                    partial(handle_record, self.rest_metadata.geo_type),
                    json_response["features"]
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
                if re.search(f"{re.escape(self.service_name)}_\\d+\\.csv", file)
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
