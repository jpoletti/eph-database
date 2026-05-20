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
            content_type = response.headers.get('Content-Type', '')
            if 'zip' in content_type or 'octet-stream' in content_type:
                valid_urls[key] = url

        except requests.exceptions.RequestException:
            continue

    return valid_urls

def download_release_zips(repo='jpoletti/eph-database'):
    """
    Downloads the hogar and individual ZIP files from the latest GitHub
    Release into memory.
 
    Args:
        repo (str): The GitHub repository in 'owner/repo' format.
 
    Returns:
        tuple: A tuple of (hogar_zip, individual_zip) as BytesIO objects.
    """
    base_url = f'https://github.com/{repo}/releases/latest/download'
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}

    for filename in ('hogar_data.zip', 'individual_data.zip'):
        response = requests.get(f'{base_url}/{filename}', headers=headers)
        if response.status_code != 200:
            raise RuntimeError(
                f'Failed to download {filename} from release: '
                f'HTTP {response.status_code}'
            )
        if filename == 'hogar_data.zip':
            hogar_zip = BytesIO(response.content)
        else:
            individual_zip = BytesIO(response.content)

    return hogar_zip, individual_zip

def extract_release_zip_to_memory(release_zips):
    """
    Extracts the CSV from a ZIP BytesIO object into a new BytesIO object.
 
    Args:
        release_zips (tuple): A tuple of ZIP files in memory containing single
                              CSVs.
 
    Returns:
        BytesIO: The extracted CSV as a BytesIO object.
    """
    release_csv_list = []
    for zip in release_zips:
        with zipfile.ZipFile(zip) as z:
            release_csv = BytesIO(z.read(z.namelist()[0]))
            release_csv_list.append(release_csv)

    return release_csv_list

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

def read_text_and_merge(old_csv, new_txt):

    df = pl.read_csv(old_csv)
    for file in new_txt:
        new_df = pl.read_csv(file, separator=';', decimal_comma=True,
                             schema=df.schema)
        df = pl.concat([df, new_df])
    return df