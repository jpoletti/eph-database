import io
import zipfile

def create_key_list(last_quarter, start_quarter='16-2T'):
    
    # Finding the year and quarter for the starting quarter
    year = int('20' + start_quarter[:2])
    quarter = int(start_quarter[-2])
    # Generate key list
    key_list = []
    while True:
        year_str = str(year)[-2:]
        key_str = f'{year_str}-{quarter}T'
        key_list.append(key_str)
        if key_str == last_quarter:
            break
        quarter += 1
        if quarter == 5:
            quarter = 1
            year += 1
    return key_list

def create_url_list(key_list):
    # Generate URL list
    value_list = []
    for key in key_list:
        quarter = key[-2]
        year = '20' + key[:2]
        
        # Dealing with URLs for 2016
        if year == '2016':
            if quarter == '2':
                ordinal_str = 'do'
            if quarter == '3':
                ordinal_str = 'er'
            if quarter == '4':
                ordinal_str = 'to'
            url_1 = 'https://www.indec.gob.ar/ftp/cuadros/menusuperior/eph/'
            url_2 = f'EPH_usu_{quarter}{ordinal_str}Trim_{year}_txt.zip'
            value_list.append(url_1 + url_2)
            continue

        # Dealing with the 2017-1T case
        if key == '17-1T':
            url_1 = 'https://www.indec.gob.ar/ftp/cuadros/menusuperior/eph/'
            url_2 = 'EPH_usu_1er_Trim_2017_txt.zip'
            value_list.append(url_1 + url_2)
            continue

        # The rest should work with this
        else:
            url_1 = 'https://www.indec.gob.ar/ftp/cuadros/menusuperior/'
            url_2 = f'eph/EPH_usu_{quarter}_Trim_{year}_txt.zip'
            value_list.append(url_1 + url_2)

    return value_list

def save_dataframe_as_zip(df, zip_filepath, csv_filename):
    """
    Saves a Polars DataFrame as a CSV file compressed inside a ZIP archive
    directly to disk.

    Args:
        df (pl.DataFrame): The DataFrame to save.
        zip_filepath (str): The path for the ZIP file to save.
        csv_filename (str): The name of the CSV file inside the ZIP archive.
    """
    # Create a BytesIO object to hold the CSV data in memory
    csv_buffer = io.BytesIO()

    # Write the DataFrame to the CSV buffer in memory
    df.write_csv(csv_buffer)

    # Open the ZIP file and write the CSV content directly into it
    with zipfile.ZipFile(zip_filepath, mode='w', 
                         compression=zipfile.ZIP_DEFLATED) as zf:
        # Ensure the cursor is at the start of the buffer
        csv_buffer.seek(0)
        # Write the CSV buffer as a file inside the ZIP archive
        zf.writestr(csv_filename, csv_buffer.getvalue())

    print(f"DataFrame saved and compressed as {zip_filepath}")