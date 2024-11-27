import requests

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

def create_known_urls(last_known_quarter='24-1T'):

    key_list = create_key_list(last_quarter=last_known_quarter)
    url_list = create_url_list(key_list)

    # Create and return the dictionary
    return dict(zip(key_list, url_list))

def create_unknown_urls(n_quarters, last_known_quarter='24-1T'):
    # Defining the start year and quarter
    year = int('20' + last_known_quarter[:2])
    quarter = int(last_known_quarter[-2])

    # Looping to get the first and last quarters
    i = 0
    while i > n_quarters:
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

# def test_unknown_urls():
#     for i in