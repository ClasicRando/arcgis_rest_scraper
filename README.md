<h1>Python GUI Scraper for ArcGIS Servers</h1>
**** DISCLAIMER ****

I do not take fault for any data that is scraped but does not match the server's data. This is simply to cover myself if
anything does wrong. I am open to feedback and improvements in the issues section

Simple GUI providing the user the ability to scrape a REST service from ArcGIS using the primary URL of the service.
From this URL, the metadata and scraping method are defined. Currently, just 2 cases are handled by the scraper:

1. Pagination is supported (ability to query records in chunks)
2. Statistics are supported, and the layer has a unique ID (usually OBJECTID)

Other cases could be handled in the future if they can be easily generalized

<h3>Dependencies</h3>
I tried to mostly use builtin libraries but for the GUI and data processing I decided to use outside libraries.

1. Pandas (for DataFrame)
2. PyQt5 (for GUI and Concurrency)

In the future I might decide to switch to asyncio for concurrency to use another builtin library.

<h3>Operation</h3>
Simple UI with an input for the base URL, 2 buttons for actions, and an output text area. Steps to follow when scraping
are as follows:

1. Find URL and paste into text box
2. Click "Fetch Metadata" and wait for response and output to be shown
3. Click "Fetch Data" and wait for query responses, file consolidation and output to be shown

The application creates a directory in the current working directory to store temp files (query data as to avoid memory
overhead). The directory does not need to be created by the user since the application will create the directory on
startup if it does not already exist. DO NOT delete or interact with the files or directory while scraping since this
will disrupt operations and lead to an invalid scrape.

While scraping the information, currently I do not care about the data type as it is stored on the REST server and
simply convert all json response values to string. In the future, efforts could be made to keep data types from the
service.

All features are queries with the output spatial reference as NAD83 (4269). This could be changed to allow more
flexibility in the future

<h3>Future Plans</h3>
- preserve service data types when querying
- finding and handling more scraping cases
- allow user to define output spatial reference
- test asyncio
- add actual testing for Units and Integration (not enough cases to test currently)

All the operations and code can be found in main.py