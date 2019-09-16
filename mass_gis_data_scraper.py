import urllib.request
import zipfile
import pandas as pd
import csv
from os import getenv, path, remove
import psycopg2
from dotenv import load_dotenv
from sql_queries import create_table_query, template_url, delete_duplicates_query

template_url = "http://download.massgis.digital.mass.gov/shapefiles/mad/town_exports/adv_addr/AdvancedAddresses_M{}.zip"

class MassGISScraper:
    def __init__(self):
        load_dotenv()
        db_name = getenv('DB_NAME')
        user = getenv('DB_USER_NAME')
        password = getenv('DB_PASSWORD')
        host = getenv('HOST')
        connection_string = "host={} dbname={} user={} password={}".format(host, db_name, user, password)
        self.db_connection = psycopg2.connect(connection_string)
        self.cursor = self.db_connection.cursor()
        self.cursor.execute(create_table_query)

    def expanded_index(self, index):
        string = str(index)
        while len(string) != 3:
            string = "0" + string
        return string

    def scrape(self):
        for i in range(1, 1000):
            print(i)
            expanded = self.expanded_index(i)
            link = template_url.format(expanded)
            file_name = str(i) + ".zip"
            spreadsheet = "AdvancedAddresses_M" + expanded + ".xlsx" 
            tsv_name = str(i) + ".tsv"
            #print(link)
            continue_status = self.download_file(link, file_name)
            if not continue_status:
                print("exit")
                continue
            self.unzip_file(spreadsheet, file_name)
            self.convert_and_store(spreadsheet, tsv_name)
            #self.add_to_storage(tsv_name)
            self.clean_up(file_name, spreadsheet)
        self.delete_duplicates()
        self.db_connection.commit()

    def download_file(self, link, file_name):
        if self.file_already_extracted(file_name):
             return True
        try:
            urllib.request.urlretrieve(link, file_name)
            return True
        except:
            return False
    
    def unzip_file(self, spreadsheet, file_name):
        if self.file_already_extracted(spreadsheet):
            return
        zip_ref = zipfile.ZipFile(file_name, 'r')
        zip_ref.extractall()
        zip_ref.close()

    def convert_and_store(self, spreadsheet, tsv_name):
        if self.file_already_extracted(tsv_name):
            return
        #data_xls = pd.read_excel(spreadsheet, 'Sheet1', index_col=None, header = 1, skiprows = 1)
        data_xls = pd.read_excel(spreadsheet, 'Sheet1', index_col=None, dtype=object)
        data_xls = data_xls.astype(str)
        rows = data_xls.shape[0]
        blocks = int(rows / 100)
        for i in range(blocks - 1):
            new_tsv_name = str(i) + tsv_name
            try:
                new_df = data_xls[(i * 100) : ((i + 1) * 100)]
            except:
                continue
            new_df.to_csv(new_tsv_name, encoding='utf-8', index=False, sep = '\t')
            self.add_to_storage(new_tsv_name)
            remove(new_tsv_name)
            

    def add_to_storage(self, tsv_name):
        with open(tsv_name, 'r') as f:
            next(f)
            try:
                self.cursor.copy_from(f, 'mass_gis_scraped_data', sep='\t')
            except:
                print("bad copy")
            self.db_connection.commit()

    def clean_up(self, file_name, spreadsheet_name):
        if self.file_already_extracted(file_name):
            remove(file_name)
        if self.file_already_extracted(spreadsheet_name):
            remove(spreadsheet_name)

    def file_already_extracted(self, input_name):
        return path.exists(input_name)

    def delete_duplicates(self):
        self.cursor.execute(delete_duplicates_query)

#scrap = MassGISScraper()
#scrap.scrape()
