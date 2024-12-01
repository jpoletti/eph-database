if __name__ == '__main__':

    import sys
    from update_data_functions import *
    from common_functions import save_dataframe_as_zip
    
    # Checking the last quarter in the known data
    with open('last_known_quarter.txt') as f:
        last_known_quarter = f.read()
    
    # Checking if there's new data
    url_dict = create_unknown_urls(4, last_known_quarter)
    valid_urls = test_urls(url_dict)
    
    # Stopping the script if there's no new data
    if not valid_urls:
        print("There's no new data")
        sys.exit()
    
    # Download the zip file/s with the new data
    new_zip_files = download_new_zips_to_memory(valid_urls)
    
    # Extracting the files with new data to BytesIO
    hogar_txt_files, ind_txt_files = extract_new_zips_to_memory(new_zip_files)
    del(new_zip_files)
    
    # Extracting the file with old hogar data
    old_hogar_csv = extract_old_zips_to_memory('hogar')
    
    # Merging the old and new hogar data
    hogar = read_text_and_merge(old_hogar_csv, hogar_txt_files)
    
    
    zip_file_path = "data/hogar_data.zip"
    csv_file_name = "hogar_data.csv"
    save_dataframe_as_zip(hogar, zip_file_path, csv_file_name)
    
    # Small memory optimization
    del(hogar)
    del(old_hogar_csv)
    del(hogar_txt_files)
    
    # Extracting the file with old individual data
    old_ind_csv = extract_old_zips_to_memory('ind')
    
    # Merging the old and new individual data
    ind = read_text_and_merge(old_ind_csv, ind_txt_files)
    
    # Saving the new individual table (with old and new data) as a zip file
    zip_file_path = "data/individual_data.zip"
    csv_file_name = "individual_data.csv"
    save_dataframe_as_zip(ind, zip_file_path, csv_file_name)
    
    # Save the the new last known quarter in the txt file
    valid_keys = []
    for key, url in valid_urls.items():
        valid_keys.append(key)
    with open('last_known_quarter.txt', 'w') as f:
        f.write(valid_keys[-1])
        f.close()