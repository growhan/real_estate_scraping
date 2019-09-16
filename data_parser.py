import os
import csv
import json
import glob
import ast
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup

class DataParser:
    def __init__(self):
        with open("quality.txt", "r") as file:
            s = file.read()
            self.quality = ast.literal_eval(s)
        self.quality["valid"] = set(self.quality["valid"])
        self.quality["garbage"] = set(self.quality["garbage"])

    def extractData(self, directory_name):
        candidate_files = glob.glob("./{}/*.txt".format(directory_name))
        parent_df = pd.DataFrame()
        count = 0
        stats = {"worked": [], "broken": []}
        for idx, file_name in enumerate(candidate_files):
            count += 1
            print(idx)
            file_id = self.getIDNum(file_name)
            print(file_id)
            print(file_name)
            if file_id in self.quality["garbage"] and not "rescrape" in file_name:
                continue
            csv_name = file_name[0:-4] + ".csv"
            #if os.path.exists(csv_name):
            #    stats["worked"].append(file_name)
            #    continue
            with open(file_name, "r") as file:
                    file_text = file.readlines()[0].replace("\n", "")
                    has_html = "rescrape" in file_name or "small_scrape" in file_name
                    file_texts = self.removeNonsense(file_text, has_html)
                    #print(file_texts)
                    for response in file_texts:
                        #print(response)
                        if not response:
                            continue
                        data = []
                        try:
                            data = json.loads(response)
                        except:
                            stats["broken"].append(file_name)
                            continue
                        if data["errorMessage"] != "Success":
                            continue
                        #print((data["payload"]["homes"]))
                        result = self.dfResponse(file_id, file_name, data["payload"]["homes"])
                        if not result.empty:
                            stats["broken"].append(file_name)
                            continue
                        stats["worked"].append(file_name)
                        #print(result.dtypes)
                        if idx == 0:
                            parent_df = result
                        else:
                            parent_df = pd.concat([parent_df, result], sort=False)
        parent_df.to_csv("total.csv")
        with open("total_csv_stats.txt", "w") as file:
            file.write(str(stats))

    def dfResponse(self, cluster_id, file_name, homes):
        if not homes:
            print("empty homes")
            print(homes)
            return pd.DataFrame()
        df = pd.DataFrame(columns=['cluster_id', 'file_name', 'is_sold'])
        for idx, val in enumerate(homes):
            #print(idx)
            df.at[idx, "cluster_id"] = cluster_id
            df.at[idx, "file_name"] = file_name
            for field in val:
                #print(df)
                datum = val[field]
                #print(field)
                #print(datum)
                if type(datum) is dict:
                    if "value" in datum:
                        datum = datum["value"]
                    else:
                        continue
                if not field in df.columns:
                    df[str(field)] = ""
                df.at[idx, field] = datum
            #print(list(df))
            if 'mlsStatus' in df and df.at[idx, 'mlsStatus'] in ["Sold", "Active"]:
                df.at[idx, 'is_sold'] = df.at[idx, 'mlsStatus']
            else:
                sash_data = df.at[idx, 'sashes']
                if sash_data and 'sashTypeName' in sash_data[0]:
                    df.at[idx, 'is_sold'] = sash_data[0]['sashTypeName']
        #print(df)
        print("df_made")
        file_name = file_name[0:-4] + ".csv"
        df.to_csv( file_name, index = False)
        return df

    def removeNonsense(self, inp, is_html):
        file_text = inp
        if is_html:
            file_text = file_text[4:]
            file_text = BeautifulSoup(inp,  "html5lib").get_text()
        split_index = file_text.find("""{}&&""")
        if split_index == -1:
            return [file_text]
        else:
            part_one = file_text[0:split_index]
            part_two = file_text[split_index + 4 : ]
            return [part_one, part_two]

    def getIDNum(self, file_name):
        if "rescrape" in file_name:
            file_name = file_name.replace(".txt", "")
            file_name = file_name.replace("./stingray_out_2019-08-27/rescrape_", "")
            return file_name
        else:
            return (file_name[26:]).split("_")[0]
    

parser = DataParser()
parser.extractData("stingray_out_2019-08-27")
