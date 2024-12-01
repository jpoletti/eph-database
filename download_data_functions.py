import requests
import os
import time
import zipfile
import shutil
import re
import io
from tqdm import tqdm
import polars as pl
from common_functions import *


def create_known_urls(last_known_quarter='24-1T'):

    key_list = create_key_list(last_quarter=last_known_quarter)
    url_list = create_url_list(key_list)

    # Create and return the dictionary
    return dict(zip(key_list, url_list))

def create_folder_and_download(url_dict):
    # Create the "data" folder if it doesn't exist
    if 'data' not in os.listdir():
        os.makedirs('data')
        os.makedirs('data/zip')

    # Download the files
    for key in tqdm(url_dict, desc='Downloading files'):
        url = url_dict[key]
        response = requests.get(url, stream=True)

        # If the website responds 
        if response.status_code == 200:
            with open(f'data/zip/{key}.zip', 'wb') as f:
                f.write(response.content)

            # Try to avoid DDoS detection
            time.sleep(1)

        else:
            print(f'Failed to download {key}')

def check_zip_files():
    error_files = 0
    zip_dir = 'data/zip'

    for file in os.listdir(zip_dir):
        zip_path = os.path.join(zip_dir, file)
        try:
            with zipfile.ZipFile(zip_path, 'r') as z:
                z.testzip()
        except zipfile.BadZipFile:
            error_files += 1
            print(f'{file} is not a valid zip file')

    if error_files == 0:
        print('It looks like all the zip files are valid')
    else:
        print(f'Found {error_files} invalid zip file(s)')

def extract_rename_and_move(key):
    """This function extracts the .txt files from the .zip file, renames them 
    and moves them to the correct folders (data/individual and data/hogar). To
    simplify the code a bit it creates a folder called extracted, which it uses 
    to store the extracted files, and it deletes the folder at the end 
    Args:
        key (str): the key corresponding to the quarter. Its format should be
        yy-QT, so that, for example, 23-2T is the second quarter of 2023.
    """
    # Create the folders inside the "data" folder
    if 'individual' not in os.listdir('data'):
        os.mkdir('data/individual')
    if 'hogar' not in os.listdir('data'):
        os.mkdir('data/hogar')

    # Creating the "extracted" directory
    extr_dir = 'data/extracted'
    if not os.path.exists(extr_dir):
        os.makedirs(extr_dir)
    
    zip_dir = f'data/zip/{key}.zip'
    
    # Extracting the files without the folders
    with zipfile.ZipFile(zip_dir, 'r') as z:
        for i in z.namelist():
            filename = os.path.basename(i)
            # Skip directories
            if not filename:
                continue
            
            source = z.open(i)
            target_path = os.path.join(extr_dir, filename)
            with open(target_path, "wb") as target:
                shutil.copyfileobj(source, target)
            source.close()
    
    # Regex patterns to try to avoid problems with filenames
    hogar_re = r'[hH][oO][gG][aA][rR]'
    individual_re = r'[iI][nN][dD][iI][vV][iI][dD][uU][aA][lL]'
    
    # Looping over the recently extracted files
    for filename in os.listdir(extr_dir):
        old_path = os.path.join(extr_dir, filename)
        
        # Finding the "hogar" data
        if re.search(hogar_re, filename) and filename.endswith('.txt'):
            new_filename = f'{key}-hogar.txt'
            new_path = os.path.join('data', 'hogar', new_filename)
            os.rename(old_path, new_path)
        
        # Finding the "individual" data
        elif ((re.search(individual_re, filename) or 'personas' in filename) and 
              filename.endswith('.txt')):
            new_filename = f'{key}-individual.txt'
            new_path = os.path.join('data', 'individual', new_filename)
            os.rename(old_path, new_path)
    
    # Deleting the "extracted" folder with all of its contents
    shutil.rmtree(extr_dir)

def extract_all_and_remove_folder(url_dict):
    for key, value in url_dict.items():
        extract_rename_and_move(key)
    shutil.rmtree('data/zip')

def check_file_existence(url_dict):
    equal_counter = 0
    for file in os.listdir('data/individual'):
        for key, value in url_dict.items():
            if file[:5] == key:
                equal_counter += 1
                continue

    if equal_counter == len(url_dict):
        print('All the files for the individual dataset are available.')
    else:
        print(f'There are {len(url_dict) - equal_counter} missing files')

    # Checking if all the files for the hogar dataset are there
    equal_counter = 0
    for file in os.listdir('data/hogar'):
        for key, value in url_dict.items():
            if file[:5] == key:
                equal_counter += 1
                continue

    if equal_counter == len(url_dict):
        print('All the files for the hogar dataset are available.')
    else:
        print(f'There are {len(url_dict) - equal_counter} missing files')

def read_and_format(path, schema_overrides=None):
    if schema_overrides is None:
        df = pl.read_csv(path,
                         separator=';',
                         null_values=["NA"],
                         decimal_comma=True,
                         infer_schema_length=10000)
        
    else:
        df = pl.read_csv(path,
                         separator=';',
                         null_values=["NA"],
                         decimal_comma=True,
                         infer_schema_length=10000,
                         schema_overrides=schema_overrides)
        
    # Cast all i64 columns to i32
    df = df.with_columns([
        df[col].cast(pl.Int32) for col in df.columns
        if df[col].dtype == pl.Int64
])
    # Cast all f64 columns to f32
    df = df.with_columns([
    df[col].cast(pl.Float32) for col in df.columns
    if df[col].dtype == pl.Float64
])
    
    # Remove unnamed columns
    unnamed_columns = [col for col in df.columns if col == ""]
    if unnamed_columns:
        df = df.drop(unnamed_columns)
        
    return df

def process_txt_files(file_paths, schema_overrides=None):
    final_df = None

    for index, path_str in enumerate(file_paths):
        # Read and format the current file
        current_df = read_and_format(path_str, schema_overrides)

        if index == 0:
            # Initialize with the first file
            final_df = current_df
        else:
            # Clean invalid values and align column schemas
            for col_name, col_type in final_df.schema.items():
                if col_name in current_df.schema:
                    # Clean and align columns expected to be Int32
                    if col_type == pl.Int32:
                        current_df = current_df.with_columns(
                            pl.when(pl.col(col_name).cast(pl.Utf8).str
                                    .strip_chars() == "")
                            .then(None)
                            .otherwise(pl.col(col_name))
                            .alias(col_name)
                        )
                    # Cast the column to the expected type
                    current_df = current_df.with_columns(pl.col(col_name)
                                                         .cast(col_type))
            
            # Concatenate the aligned DataFrame
            final_df = pl.concat([final_df, current_df])

            # Align schemas
            for col in final_df.columns:
                if col not in current_df.columns:
                    current_df = current_df.with_columns(pl.lit(None)
                                                           .alias(col))
            
            for col in current_df.columns:
                if col not in final_df.columns:
                    current_df = current_df.drop(col)

        print(f"Processing file: {path_str}")

    return final_df

