from requests_spoofer import RequestsSpoofer
import requests
import csv
import json
import ast
import datetime
import time
import types
import os
import random
from selenium_scraper import SeleniumScraper

SOLD_TEMPLATE = "https://www.redfin.com/stingray/api/gis?al=1&min_stories=1"
SOLD_TEMPLATE += "&num_homes=10500&page_number=1&sf=1,2,3,5,6,7&sold_within_days=36500&status=9"
SOLD_TEMPLATE += "&uipt=1,2,3,4,5,6&user_poly={}&v=8"
FOR_SALE_TEMPLATE = "https://www.redfin.com/stingray/api/gis?al=1n&min_stories=1&"
FOR_SALE_TEMPLATE += "num_homes=10500&ord=redfin-recommended-asc&page_number=1&sf=1,2,3,5,6,7&status=9&"
FOR_SALE_TEMPLATE += "uipt=1,2,3,4,5,6&user_poly={}&v=8"

class StingRayBulkScraper:
    def __init__(self, large_polygon_file_name, small_polygon_file_name, use_selenium):
        self.shapes = {"large": {}, "small": {}}
        with open(large_polygon_file_name, 'r') as file:
            data = file.read().replace('\n', '')
            self.shapes["large"] = ast.literal_eval(data)
        with open(small_polygon_file_name, 'r') as file:
            data = file.read().replace('\n', '')
            self.shapes["small"] = ast.literal_eval(data)
        self.spoofer = RequestsSpoofer(100000, 20, True)
        self.spoof_clean = True
        self.use_selenium = use_selenium
        if self.use_selenium:
            self.selenium = SeleniumScraper()

    def bulkScrape(self, path_name, debug=False):
        tot_time = 0
        num_calls = 0
        for size in self.shapes.keys():
            for key in self.shapes[size]:
                file_name = path_name + "/" + "{}_{}_out.txt".format(key, size)
                if  os.path.exists(file_name):
                    continue
                with open(file_name, "w") as file:
                    delay = random.randrange(5, 12)
                    print("Delay: {}".format(delay))
                    time.sleep(delay)
                    start = datetime.datetime.now()
                    print(key)
                    if not self.spoof_clean:
                        print("changing ip")
                        print(self.spoofer.current_ip)
                        self.spoofer.modifyIP()
                        print("changed_ip")
                        print(self.spoofer.current_ip)
                    vals = self.shapes[size][key]
                    if not type(vals) is list:
                        vals = [vals]
                    for link in vals:
                        # gets calls for sold and for sale houses
                        link_pairs = self.buildAPICalls(self.shapes[size][key])
                        print(link_pairs)
                        for link in link_pairs:
                            resp = self.get_response(link)
                            print(resp.status_code)
                            print(resp.text, file=file)
                            if resp.status_code in [403, 500]:
                                print(resp.text, file=file)
                                print(self.spoofer.current_ip, file=file)
                                self.spoof_clean = False
                            else:
                                self.spoof_clean = True
                            time.sleep(delay)
                    end = datetime.datetime.now()
                    tot_time += ((end - start).seconds)
                    print((end - start).seconds)
                    num_calls += 1
        
    def get_response(self, link):
        if self.use_selenium:
            return self.selenium.getPageSource(link)
        else:
            return self.spoofer.makeRequest(link)


    def bulkScrapeTest(self):
        test_link = "https://www.yahoo.com"
        redfin_request_time_estimate = 10
        tot_time = 0
        num_calls = 0
        with open("test_scrape.txt", "w") as file:
            for size in self.shapes.keys():
                for key in self.shapes[size]:
                    print(key, file=file)
                    print(key)
                    start = datetime.datetime.now()
                    links = self.buildAPICalls(self.shapes[size][key])
                    print(links, file=file)
                    self.spoofer.makeRequest(test_link)
                    end = datetime.datetime.now()
                    tot_time += ((end - start).seconds + redfin_request_time_estimate)
                    print((end - start).seconds)
                    num_calls += 1
        print("tot_time: {}, num_calls: {}, avg: {}".format(tot_time, num_calls, (tot_time / num_calls)))

    def buildAPICalls(self, shape_string):
        return [SOLD_TEMPLATE.format(shape_string)]

