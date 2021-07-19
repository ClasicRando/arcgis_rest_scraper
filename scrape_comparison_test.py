import os
import unittest
import time
import re
from functools import partial

import requests
from pandas import read_csv, DataFrame
from requests import get
from selenium.webdriver import Firefox, FirefoxProfile
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support.expected_conditions import presence_of_element_located

from scraping import handle_record, RestMetadata

gecko_path = f"{os.getcwd()}\\test_files\\geckodriver.exe"
export_path = f"{os.getcwd()}\\test_files"
test_server = "https://gisp.mcgi.state.mi.us/arcgis/rest/services/DEQ/Environmental_Mgt/MapServer/3"


def fetch_query(rest_metadata: RestMetadata, query: str) -> DataFrame:
    invalid_response = True
    json_response = dict()
    try_number = 1
    max_tries = 10

    while invalid_response:
        try:
            # GET request using query
            response = get(query)
            invalid_response = response.status_code != 200
            if invalid_response:
                print(f"Error: {query} got this response:\n{response.content}")

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
        if try_number > max_tries:
            raise Exception(f"Too many tries to fetch query ({query})")

    # Once query is successful, Map features from response using handle_record and geo_type
    data = list(
        map(
            partial(handle_record, rest_metadata.geo_type),
            json_response["features"]
        )
    )
    # Create Dataframe using records and fields
    return DataFrame(
        data=data,
        columns=rest_metadata.fields,
        dtype=str
    )


class MyTestCase(unittest.TestCase):
    def test_something(self):
        profile = FirefoxProfile()
        profile.set_preference(
            "browser.download.dir",
            r"C:\Users\steve\IdeaProjects\arcgis_rest_scraper\test_files"
        )
        profile.set_preference("browser.helperApps.neverAsk.saveToDisk", "text/csv, text/plain")
        profile.set_preference("browser.download.folderList", 2)
        profile.set_preference("browser.download.manager.showWhenStarting", False)

        driver = Firefox(profile, executable_path=gecko_path)
        driver.get("https://cartong.github.io/arcgis-rest-service-export/")
        driver.find_element_by_id("input-service-url").send_keys(test_server)
        Select(driver.find_element_by_id("input-output-format")).select_by_value("csv")
        Select(driver.find_element_by_id("input-output-geometry")).select_by_value("true")
        driver.find_element_by_id("btn-input-service-url").click()

        try:
            WebDriverWait(driver, 60).until(
                presence_of_element_located((By.CLASS_NAME, "badge-cartong"))
            )
            driver.find_element_by_id("btn-export-data").click()
            driver.find_element_by_class_name("btn-export-data-format").click()
            time.sleep(2)
        finally:
            driver.close()
            driver.quit()

        csv_file = [file for file in os.listdir(export_path) if re.search(r"\.csv$", file)]
        if len(csv_file) == 1:
            test_df = read_csv(f"{export_path}\\{csv_file[0]}", dtype=str).fillna("")
            scrape_df = None
            metadata = RestMetadata(test_server)
            for query in metadata.queries:
                temp_df = fetch_query(metadata, query)
                if scrape_df is None:
                    scrape_df = temp_df
                else:
                    scrape_df = scrape_df.append(temp_df, ignore_index=True)
            test_df = test_df.drop(columns=["geometryType", "x", "y", "crs"])
            for column in test_df.columns:
                test_df[column] = test_df[column].str.strip()
            scrape_df = scrape_df.drop(columns=["POINTS"])

            self.assertTrue(
                len(test_df.index) == len(scrape_df.index),
                "Dataframes are not the same length"
            )
            self.assertTrue(
                test_df.columns.array == scrape_df.columns.array,
                "Dataframes do not have the same columns"
            )
            self.assertTrue(scrape_df.equals(test_df), "Dataframes are not the same")
            os.remove(f"{export_path}\\{csv_file[0]}")
        else:
            self.fail("Could not locate csv download")


if __name__ == '__main__':
    unittest.main()
