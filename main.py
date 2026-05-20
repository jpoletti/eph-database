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

    # Download the data in the current release
    release_zip_files = download_release_zips()

    # Extract the CSV files from the release ZIPs into memory
    (hogar_release,
     ind_release) = extract_release_zip_to_memory(release_zip_files)
    del release_zip_files

    # Download the zip file/s with the new data
    new_zip_files = download_new_zips_to_memory(valid_urls)

    # Extracting the files with new data to BytesIO
    new_hogar, new_ind = extract_new_zips_to_memory(new_zip_files)
    del new_zip_files

    # Merging the old and new hogar data
    hogar = read_text_and_merge(hogar_release, new_hogar)
    del hogar_release
    del new_hogar

    # Saving the new hogar table (with old and new data) as a zip file
    save_dataframe_as_zip(hogar, 'hogar_data.zip', 'hogar_data.csv')
    del hogar

    # Merging the old and new individual data
    individual = read_text_and_merge(ind_release, new_ind)
    del ind_release
    del new_ind

    # Saving the new individual table (with old and new data) as a zip file
    save_dataframe_as_zip(individual, 'individual_data.zip',
                          'individual_data.csv')
    del individual

    # Save the the new last known quarter in the txt file
    valid_keys = []
    with open('last_known_quarter.txt', 'w') as f:
        f.write(list(valid_urls.keys())[-1])