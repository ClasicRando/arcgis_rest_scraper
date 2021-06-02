import sys
import time
import os
from PyQt5.QtWidgets import (QMainWindow, QApplication, QFormLayout, QLineEdit, QWidget,
                             QPushButton, QPlainTextEdit, QProgressBar, QLabel)
from PyQt5.QtCore import pyqtSlot, QThreadPool
from typing import Optional
from scraping import RestMetadata
from workers import QueryFetcher, MetadataFetcher, DataConsolidator


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
