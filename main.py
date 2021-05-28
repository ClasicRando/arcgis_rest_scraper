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
from dataclasses import dataclass, asdict
from json import dumps


@dataclass
class RestMetadata:
    url: str
    name: str = ""
    source_count: int = -1
    max_record_count: int = -1
    pagination: bool = False
    stats: bool = False
    server_type: str = "Feature Layer"
    geo_type: str = "esriGeometryPoint"
    fields: List[str] = ()
    oid_field: str = ""
    max_min_oid: Tuple[int, int] = (-1, -1)
    inc_oid: bool = False

    @property
    def scrape_count(self) -> int:
        return self.max_record_count if self.max_record_count <= 10000 else 10000

    @property
    def oid_query_count(self) -> int:
        return ceil((self.max_min_oid[0] - self.max_min_oid[1] + 1) / self.scrape_count)

    @property
    def pagination_query_count(self) -> int:
        return ceil(self.source_count / self.scrape_count)

    @property
    def is_table(self) -> bool:
        return self.server_type == "TABLE"

    @property
    def geo_text(self) -> str:
        return "" if self.is_table else f"&geometryType={self.geo_type}&outSR=4269"

    @property
    def json_text(self) -> str:
        return dumps(asdict(self), indent=4)

    @property
    def queries(self) -> List[str]:
        if self.pagination:
            return [
                self.url + self.get_pagination_query(
                    i * self.scrape_count,
                    self.scrape_count
                )
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

    def get_pagination_query(self, offset: int, record_count: int) -> str:
        return f"/query?where=1+%3D+1&resultOffset={offset}&resultRecordCount={record_count}" \
               f"{self.geo_text}&outFields=*&f=json"

    def get_oid_query(self, min_oid: int) -> str:
        return f"/query?where={self.oid_field}+>%3D+{min_oid}+and+" \
               f"{self.oid_field}+<%3D+{min_oid + self.max_record_count - 1}" \
               f"{self.geo_text}&outFields=*&f=json"


class MainWindow(QMainWindow):

    def __init__(self):
        super(MainWindow, self).__init__()
        self.url = ""
        self.rest_metadata: Optional[RestMetadata] = None
        self.current_queries_running = 0
        self.thread_pool = QThreadPool()
        self.start = 0.0

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

        form_layout.addRow("URL", txt_url)
        form_layout.addWidget(btn_metadata)
        form_layout.addWidget(btn_run)
        form_layout.addWidget(self.scraping_progress)
        form_layout.addWidget(self.progress_message)
        form_layout.addWidget(self.txt_output)

        main_widget = QWidget()
        main_widget.setLayout(form_layout)
        self.setCentralWidget(main_widget)
        self.setWindowTitle("REST Scraper")

    @pyqtSlot(str)
    def url_update(self, text: str) -> None:
        self.url = text

    @pyqtSlot()
    def get_metadata(self):
        if self.thread_pool.activeThreadCount() > 0:
            return
        self.toggle_progress()
        self.progress_message.setText("Fetching Metadata")
        self.scraping_progress.setMaximum(0)
        worker = MetadataFetcher(self.url)
        worker.signals.result.connect(self.post_metadata)
        worker.signals.error.connect(lambda x: print(x))
        self.thread_pool.start(worker)

    @pyqtSlot()
    def get_data(self):
        if self.thread_pool.activeThreadCount() > 0:
            return

        self.toggle_progress()
        self.start = time.time()
        queries = self.rest_metadata.queries
        self.current_queries_running = len(queries)
        self.scraping_progress.setMaximum(self.current_queries_running + 1)
        self.progress_message.setText("Fetching query data into temp files")
        for i, query in enumerate(queries):
            worker = QueryFetcher(query, i + 1, self.rest_metadata)
            worker.signals.error.connect(lambda x: print(x))
            worker.signals.finished.connect(self.post_query)
            self.thread_pool.start(worker)

    @pyqtSlot()
    def post_query(self):
        self.current_queries_running -= 1
        self.scraping_progress.setValue(self.scraping_progress.value() + 1)
        if self.current_queries_running == 0:
            self.progress_message.setText("Consolidating data and cleaning temp files")
            worker = DataConsolidator(self.rest_metadata.name, self.start)
            worker.signals.error.connect(lambda x: print(x))
            worker.signals.result.connect(self.post_scrape)
            self.thread_pool.start(worker)

    @pyqtSlot(object)
    def post_scrape(self, text: str) -> None:
        self.txt_output.setPlainText(text)
        self.toggle_progress()

    @pyqtSlot(object)
    def post_metadata(self, result: tuple) -> None:
        metadata, diff = result
        self.rest_metadata = metadata
        text = f"Metadata\n--------\n{self.rest_metadata.json_text}\n" \
               f"Took {diff} seconds"
        self.txt_output.setPlainText(text)
        self.toggle_progress()

    def toggle_progress(self):
        state = self.scraping_progress.isVisible()
        self.scraping_progress.setVisible(not state)
        self.progress_message.setVisible(not state)


class WorkerSignals(QObject):

    finished = pyqtSignal()
    error = pyqtSignal(str)
    result = pyqtSignal(object)


class QueryFetcher(QRunnable):

    def __init__(self, query: str, query_num: int, rest_metadata: RestMetadata):
        super(QueryFetcher, self).__init__()
        self.query = query
        self.query_num = query_num
        self.rest_metadata = rest_metadata
        self.signals = WorkerSignals()

    @pyqtSlot()
    def run(self) -> None:
        try:
            response = get(self.query)
            num_records = 0
            if response.status_code == 200:
                data = list(
                    map(
                        partial(handle_record, self.rest_metadata.geo_type),
                        response.json()["features"]
                    )
                )
                df = DataFrame(
                    data=data,
                    columns=self.rest_metadata.fields,
                    dtype=str
                )
                num_records = len(df.index)
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
        super(DataConsolidator, self).__init__()
        self.service_name = service_name
        self.start = start
        self.signals = WorkerSignals()

    @pyqtSlot()
    def run(self) -> None:
        try:
            num_records = 0
            is_first = True
            temp_files = [
                file
                for file in os.listdir(f"{os.getcwd()}\\temp_files")
                if re.search(f"{self.service_name}_\\d+\\.csv", file)
            ]
            for f in temp_files:
                df = read_csv(f"{os.getcwd()}\\temp_files\\{f}")
                num_records += len(df.index)
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
        super(MetadataFetcher, self).__init__()
        self.url = url
        self.signals = WorkerSignals()

    @pyqtSlot()
    def run(self) -> None:
        start = time.time()
        try:
            result = fetch_metadata(self.url)
        except:
            self.signals.error.emit(traceback.format_exc())
        else:
            self.signals.result.emit(tuple([result, time.time() - start]))


def trim_indent(text: str) -> str:
    result = text.strip()
    indents = min(len(match.group(0).replace("\t", "    "))
                  for match in re.finditer(r"^\s+", result, re.MULTILINE))
    return re.sub(" {" + str(indents) + "}", "", result)


def fetch(session: Session, url: str) -> dict:
    with session.get(url) as response:
        data = response.json()
        if response.status_code != 200:
            print(f"Failed to retrieve using url, {url}")
        return data


def max_min_query(oid_field: str) -> str:
    return f'/query?outStatistics=%5B%0D%0A+%7B%0D%0A++++"statisticType"%3A+"max"%2C%0D%0A' \
           f'++++"onStatisticField' \
           f'"%3A+"{oid_field}"%2C+++++%0D%0A++++"outStatisticFieldName"%3A+"MAX_VALUE"%0D%0A' \
           f'++%7D%2C%0D%0A++%7B%0D%0A++++"statisticType"%3A+"min"%2C%0D%0A++++"onStatisticField' \
           f'"%3A+"{oid_field}"%2C+++++%0D%0A++++"outStatisticFieldName"%3A+"MIN_VALUE"%0D%0A' \
           f'++%7D%0D%0A%5D&f=json'


def fetch_metadata(url: str) -> RestMetadata:
    count_query = "/query?where=1%3D1&returnCountOnly=true&f=json"
    field_query = "?f=json"
    urls = [url + count_query, url + field_query]
    metadata = RestMetadata(url)

    with Session() as session:
        for res in [fetch(session, url) for url in urls]:
            if "count" in res:
                metadata.source_count = res["count"]
            else:
                advanced_query = res.get("advancedQueryCapabilities", dict())
                metadata.server_type = res["type"]
                metadata.name = res["name"]
                metadata.max_record_count = int(res["maxRecordCount"])
                if advanced_query:
                    metadata.pagination = advanced_query.get("supportsPagination", False)
                else:
                    metadata.pagination = res.get("supportsPagination", False)
                if advanced_query:
                    metadata.stats = advanced_query.get("supportsStatistics", False)
                else:
                    metadata.stats = res.get("supportsStatistics", False)
                metadata.geo_type = res.get("geometryType", "")
                metadata.fields = [
                    field["name"] for field in res["fields"]
                    if field["name"] != "Shape" and field["type"] != "esriFieldTypeGeometry"
                ]
                if metadata.geo_type == "esriGeometryPoint":
                    metadata.fields += ["X", "Y"]
                elif metadata.geo_type == "esriGeometryMultipoint":
                    metadata.fields += ["POINTS"]
                elif metadata.geo_type == "esriGeometryPolygon":
                    metadata.fields += ["RINGS"]
                oid_fields = [
                    field["name"] for field in res["fields"]
                    if field["type"] == "esriFieldTypeOID"
                ]
                if oid_fields:
                    metadata.oid_field = oid_fields[0]
        if metadata.stats and metadata.oid_field and not metadata.pagination:
            res = fetch(session, metadata.url + max_min_query(metadata.oid_field))
            attributes = res["features"][0]["attributes"]
            metadata.max_min_oid = (attributes["MAX_VALUE"], attributes["MIN_VALUE"])
            diff = metadata.max_min_oid[0] - metadata.max_min_oid[1] + 1
            metadata.inc_oid = diff == metadata.source_count
    return metadata


def handle_record(geo_type: str, feature: dict) -> list:
    record = [str(value).strip() for value in feature["attributes"].values()]
    if geo_type == "esriGeometryPoint":
        record += [
            str(point).strip()
            for point in feature.get("geometry", {"x": "", "y": ""}).values()
        ]
    elif geo_type == "esriGeometryMultipoint":
        record += [
            "[" + "],[".join((str(coordinate).strip() for coordinate in point)) + "]"
            for point in feature["geometry"]["points"]
        ]
    elif geo_type == "esriGeometryPolygon":
        record += [str(feature["geometry"]["rings"][0]).strip()]
    return record


if __name__ == "__main__":
    if not os.path.isdir(f"{os.getcwd()}\\temp_files"):
        os.mkdir(f"{os.getcwd()}\\temp_files")
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    try:
        sys.exit(app.exec_())
    except Exception as ex:
        print(ex)
