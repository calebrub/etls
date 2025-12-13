#from pipeline.etl import fetch_and_generate_dat
# sing accont
#from pipeline.etl_csv_to_dat import fetch_and_generate_dat
# all accounts 
from pipeline.etl_csv_to_dat_all import fetch_and_generate_dat

from pipeline.load_data import load_extracted_data
from configparser import ConfigParser
import logging
import os 
config = ConfigParser()
config.read('config/config.ini')
# logging.basicConfig(filename='logs/loader.log', level=logging.INFO)

import os

def orchestrate():
    fetch_and_generate_dat()

    data_folder = os.path.abspath('dat_files')  

    load_extracted_data(data_folder)


if __name__ == "__main__":
    orchestrate()
    print("we run job finanly ",file = open(r"C:\Users\rkimera\Desktop\reveloop\collboaratemd_project\reportAPI.txt","w"))


    
