import os
import io
import logging
import requests
import regex as re
import pandas as pd
from bs4 import BeautifulSoup

logging.basicConfig(filename=os.path.join(os.getcwd(), r'logs')+"/"+"Log.txt",
                level=logging.INFO,
                format='%(levelname)s: %(asctime)s %(message)s',
                datefmt='%m/%d/%Y %I:%M:%S')



def get_data(url,headers={},payload={}):
    """
    Return: URL response HTML text 
    Input: URL
    Desc: This function makes a request call to the URL, gets the text response from the URL.
    """
    logging.info("Calling the API- {}".format(url))
    response = requests.request("GET", url, headers=headers, data=payload, timeout=10)
    try:
        logging.info("API sucessfully retrieved".format(url))
        return response.text
    except:
        logging.info("API failed".format(url))

def parse_html(html_string):
    """
    Return: a list of file names that need to be donloaded into our storage
    Input: HTML text from get_data() function
    Desc: This function parses the HTML text and extracts all the file names that we need to download to our storage
          which is our folder directory in this case, This can be exteded to any storage with parameters and adding 
          configuration.
    """
    final= []
    logging.info("Parsing HTML data")
    try:
        soup = BeautifulSoup(html_string,features="html.parser")
        for a in soup.find_all('a',href=True):
            val = str(a['href'].split('/')[-1])
            if re.search("^[USC]{3}", val):
                final.append(val)
        if len(final) < 1:
            logging.info("No data has been parsed from the website")
        else:
            logging.info("Data successfully parsed")
        return final
    except:
        logging.info("Error Parsing HTML data")

def get_data_save(ur_data):
    """
    Return: None
    Input: a list of file names
    Desc: This function takes in a list of file names we need to download, goes back to the raw format to github,
          extracts all the information from the specific file, saves it into a pandas dataframe, file name column
          is added to the dataframe. The function saves each pandas dataframe as a .txt filr in raw_files directory.
    """
    try:
        for lS in ur_data:
            logging.info("Extracting data from {}".format(lS))
            url = 'https://raw.githubusercontent.com/corteva/code-challenge-template/main/wx_data/'+lS
            data=requests.get(url, timeout= 10).content
            string_data = io.StringIO(data.decode('utf-8'))
            dF=pd.read_csv(filepath_or_buffer= string_data, sep=r'\t',engine='python',header=None,names=["date","max_temp_C","min_temp_C","precipitation_MM"])
            dF['filename'] = lS
            current_directory = os.getcwd()
            final_directory = os.path.join(current_directory, r'raw_files')
            if not os.path.exists(final_directory):
                logging.info("Path {}  does not exist, Creating path {}".format(final_directory))
                os.makedirs(final_directory)
            save_directory = final_directory+'/'+ str(lS)
            logging.info("Saving data from {}".format(lS))
            dF.to_csv(save_directory,index=False)
            logging.info("Data available at {}".format(save_directory))
        return None
    except:
        logging.info("Saving data failed")






