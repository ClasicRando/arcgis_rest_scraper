import unittest
import os
import json
from main import handle_record


class MyTestCase(unittest.TestCase):
    def test_record_parsing(self):
        with open(f"{os.getcwd()}\\test_files\\test_query_output.json") as f:
            query_response = json.load(f)
        features = query_response["features"]
        record = handle_record(query_response["geometryType"], features[0])
        expected_record = [
            "1", "", "AK2472", "Amchitka ROTHR Facility", "Amchitka", "", "", "", "Alaska",
            "51.5405999999999", "-178.985809999999", "State", "", "", "Open", "", "", "0", "", "No",
            "", "", "", "", "No", "", "", "", "", "", "1", "", "-178.9857946263612",
            "51.54060136788185"
        ]
        self.assertEqual(record, expected_record)

    def test_record_parsing2(self):
        with open(f"{os.getcwd()}\\test_files\\test_query_output.json") as f:
            query_response = json.load(f)
        features = query_response["features"]
        record = handle_record(query_response["geometryType"], features[1])
        expected_record = [
            "2", "", "AK1", "Adak Old Fuel Truck Shop UST T10520A",
            "Adak Naval Air Facility; Old Fuel Truck Shop", "Adak", "", "", "Alaska", "51.86417",
            "-176.645999999999", "State", "", "", "No Further Action", "", "0", "0", "", "No",
            "", "", "", "", "No", "", "", "", "", "", "", "1", "-176.6459841088737",
            "51.86417115672688"
        ]
        self.assertEqual(record, expected_record)


if __name__ == '__main__':
    unittest.main()
