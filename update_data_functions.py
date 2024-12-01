import requests
from io import BytesIO
import zipfile
import re
import os
import polars as pl
from common_functions import *

def create_unknown_urls(n_quarters, last_known_quarter='24-1T'):
    # Defining the start year and quarter
    year = int('20' + last_known_quarter[:2])
    quarter = int(last_known_quarter[-2])

    # Looping to get the first and last quarters
    i = 0
    while i < n_quarters:
        quarter = quarter + 1
        if quarter == 5:
            quarter = 1
            year += 1
        if i == 0:
            first_unknown_quarter = f'{str(year)[-2:]}-{quarter}T'
        i += 1

    last_unknown_quarter = f'{str(year)[-2:]}-{quarter}T'

    # Creating the dict with the URLs
    key_list = create_key_list(last_unknown_quarter, first_unknown_quarter)
    url_list = create_url_list(key_list)

    return dict(zip(key_list, url_list))

def test_urls(url_dict):
    valid_urls = {}
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}

    for key, url in url_dict.items():
        try:
            response = requests.get(url, headers=headers, timeout=20,
                                    allow_redirects=True)
            
            # Check if redirected to the error page
            if "Error-Default" in response.url:
                continue  # Skip URLs that redirect to the placeholder page
            
            # Optionally inspect the content for error indicators
            if "Error" in response.text or "404" in response.text:
                continue  # Skip URLs with error messages in their content

            # If no issues, consider the URL valid
            valid_urls[key] = url

        except requests.exceptions.RequestException:
            continue  # Handle connection errors gracefully

    return valid_urls

def download_new_zips_to_memory(valid_urls):
    new_zip_files = []
    for key, url in valid_urls.items():
        response = requests.get(url)
        file_in_memory = BytesIO(response.content)
        new_zip_files.append(file_in_memory)
    return new_zip_files

def extract_new_zips_to_memory(new_zip_files):
    hogar_txt_files = []
    ind_txt_files = []
    for file in new_zip_files:
        with zipfile.ZipFile(file) as z:
            for i in z.namelist():
                filename = os.path.basename(i)
                # Skip directories
                if not filename:
                    continue

                # Regex patterns to try to avoid problems with filenames
                hogar_re = r'[hH][oO][gG][aA][rR]'
                ind_re = r'[iI][nN][dD][iI][vV][iI][dD][uU][aA][lL]'

                if re.search(hogar_re, filename) and filename.endswith('.txt'):
                    hogar_txt = BytesIO(z.read(i))
                    hogar_txt_files.append(hogar_txt)

                if re.search(ind_re, filename) and filename.endswith('.txt'):
                    ind_txt = BytesIO(z.read(i))
                    ind_txt_files.append(ind_txt)

    return hogar_txt_files, ind_txt_files

def extract_old_zips_to_memory(dataset):

    if dataset == 'hogar':
        path = 'data/hogar_data.zip'
        with zipfile.ZipFile(path) as z:
            old_csv = BytesIO(z.read(z.namelist()[0]))

    elif dataset == 'ind':
        path = 'data/individual_data.zip'
        with zipfile.ZipFile(path) as z:
            old_csv = BytesIO(z.read(z.namelist()[0]))

    else:
        raise ValueError('dataset should be either "hogar" or "ind"')
    
    return old_csv

def read_text_and_merge(old_csv, new_txt):

    df = pl.read_csv(old_csv)
    for file in new_txt:
        new_df = pl.read_csv(file, separator=';', decimal_comma=True,
                             schema=df.schema)
        df = pl.concat([df, new_df])
    return df