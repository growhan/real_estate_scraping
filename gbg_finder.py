from selenium_scraper import SeleniumScraper
from bulk_scrape import StingRayBulkScraper
import glob
import json
import os
class GbgFinder:
    def __init__(self, large_file_name, small_file_name):
        self.blk = StingRayBulkScraper(large_file_name, small_file_name, True)
        self.broswer = SeleniumScraper()

    def determineGbg(self):
        gbg_candidate_files = glob.glob('./stingray_out_gbg/*.txt')
        quality = {"garbage": [], "valid": []}
        for file_name in gbg_candidate_files:
            file_id = file_name.split("_")[2][4:]
            with open (file_name, "r") as myfile:
                for line in myfile.readlines():
                    if "is blocked" in line or "<!DOCTYPE html5>" in line:
                        quality["garbage"].append(file_id)
                        break
            if len(quality["garbage"]) > 0 and quality["garbage"][-1] != file_id:
                quality["valid"].append(file_id)
        with open('quality.txt', 'w') as fp:
            fp.write(json.dumps(quality))
        self.handleGbg(quality["garbage"])

    def smallPolygonScrape(self):
        file_name = "stingray_out_2019-08-27/small_scrape_{}.txt"
        for cluster_id in self.blk.shapes["small"].keys():
            print(cluster_id)
            print(file_name.format(cluster_id))
            with open(file_name.format(cluster_id), "w") as file:
                for shape in self.blk.shapes["small"][cluster_id]:
                    links = self.blk.buildAPICalls(shape)
                    for link in links:
                        print(link)
                        page_s = self.broswer.getPageSource(link)
                        if self.checkPageSource(page_s):
                            file.write(page_s)
                        else:
                            print("captcha found - exiting - exiting")
                            return
                        
    


    def handleGbg(self, garbage_scrapes):
        for cluster_id in garbage_scrapes:
            file_name = "stingray_out_2019-08-27/rescrape_{}.txt"
            if  os.path.exists(file_name):
                continue
            with open(file_name.format(cluster_id), "w") as file:
                shape_string = self.blk.shapes["large"][int(cluster_id)]
                sting_ray_links = self.blk.buildAPICalls(shape_string)
                for link in sting_ray_links:
                    print(link)
                    page_s = self.broswer.getPageSource(link)
                    if self.checkPageSource(page_s):
                        file.write(page_s)
                    else:
                        print("captcha found - exiting - exiting")
                        return
    
    def checkPageSource(self, page_source):
        return not "<!DOCTYPE html5>" in page_source
                
                


#gbg_solver = GbgFinder()
#gbg_solver.smallPolygonScrape()