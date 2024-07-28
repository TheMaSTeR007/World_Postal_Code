import gzip, hashlib, os, requests, pymysql
from lxml import html
from pymysql import cursors
from pymysql.constants import CLIENT

from db_maker import db_schema_creater
from sys import argv

# Creating Database Schema if not exists
db_schema_creater()

# Connecting to the Database
my_host = 'localhost'
my_user = 'root'
my_password = 'actowiz'
my_database = 'world_pincode_new'
my_charset = 'utf8mb4'

connection = pymysql.connect(host=my_host, user=my_user, database=my_database, password=my_password, charset=my_charset, cursorclass=pymysql.cursors.DictCursor, autocommit=True, client_flag=CLIENT.MULTI_STATEMENTS)
if connection.open:
    print('Database connection Successful!')
else:
    print('Database connection Un-Successful.')
cursor = connection.cursor()


def req_sender(url: str, method: str) -> bytes or None:
    # Prepare headers for the HTTP request
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
    }
    # Send HTTP request
    _response = requests.request(method=method, url=url, headers=headers)
    # Check if response is successful
    if _response.status_code != 200:
        print(f"HTTP Status code: {_response.status_code}")  # Print status code if not 200
        return None
    return _response  # Return the response if successful


def ensure_dir_exists(path: str):
    # Check if directory exists, if not, create it
    if not os.path.exists(path):
        os.makedirs(path)
        print(f'Directory {path} Created')  # Print confirmation of directory creation


country_start = argv[1]
country_end = argv[2]

region_start = argv[3]
region_end = argv[4]

sub_region_start = argv[5]
sub_region_end = argv[6]


def country_fetcher(table_name: str, status_column: str):
    # Fetching all data from countries table
    select_query = f'''SELECT * FROM `{table_name}` WHERE {status_column} = 'Pending' and id between {country_start} and {country_end};'''
    cursor.execute(select_query)
    data = cursor.fetchall()
    return data


def region_fetcher(table_name: str, status_column: str):
    # Fetching all data from countries table
    select_query = f'''SELECT * FROM `{table_name}` WHERE {status_column} = 'Pending' and id between {region_start} and {region_end};'''
    cursor.execute(select_query)
    data = cursor.fetchall()
    return data


def sub_region_fetcher(table_name: str, status_column: str):
    # Fetching all data from countries table
    select_query = f'''SELECT * FROM `{table_name}` WHERE {status_column} = 'Pending' and id between {sub_region_start} and {sub_region_end};'''
    cursor.execute(select_query)
    data = cursor.fetchall()
    return data


def page_checker(url: str, method: str, directory_path: str):
    # Create a unique hash for the URL to use as the filename
    page_hash = hashlib.sha256(string=url.encode(encoding='UTF-8', errors='backslashreplace')).hexdigest()
    ensure_dir_exists(path=directory_path)  # Ensure the directory exists
    file_path = os.path.join(directory_path, f"{page_hash}.html.gz")  # Define file path
    if os.path.exists(file_path):  # Check if the file already exists
        print("File exists, reading it...")  # Notify that the file is being read
        print(f"Existing, Filename is {page_hash}")
        with gzip.open(filename=file_path, mode='rb') as file:
            file_text = file.read().decode(encoding='UTF-8', errors='backslashreplace')  # Read and decode file
        return file_text, page_hash  # Return the content of the file
    else:
        print("File does not exist, Sending request & creating it...")  # Notify that a request will be sent
        _response = req_sender(url=url, method=method)  # Send the HTTP request
        print(f"HTTP Status code: {_response.status_code}")
        if _response is not None:
            print(f"New, Filename is {page_hash}")
            with gzip.open(filename=file_path, mode='wb') as file:
                if isinstance(_response, str):
                    file.write(_response.encode())  # Write response if it is a string
                    return _response
                file.write(_response.content)  # Write response content if it is bytes
            return _response.text, page_hash  # Return the response text


def scrape_func(url: str, method: str, path: str):
    # Get HTTP response text
    html_response_text = page_checker(url=url, method=method, directory_path=path)[0]
    # Parse HTML content using lxml
    parsed_html = html.fromstring(html=html_response_text)

    base_link = url[:-1]
    # Xpath for getting url for countries
    xpath_countries_href = '//span[contains(@class, "flag")] /following-sibling::a/@href'
    countries_list = parsed_html.xpath(xpath_countries_href)
    countries_links = [base_link + country_link for country_link in countries_list]
    print(countries_links)

    country_path = os.path.join(path, "country_data")
    ensure_dir_exists(country_path)
    regions_path = os.path.join(path, "regions_path")
    ensure_dir_exists(regions_path)
    sub_regions_path = os.path.join(path, "sub_regions_path")
    ensure_dir_exists(sub_regions_path)

    for each_country_link in countries_links:
        print('Country Link: ', each_country_link)
        each_country_name = each_country_link.split('/')[-2].title()
        each_country_filename = page_checker(url=each_country_link, method=method, directory_path=os.path.join(project_files_dir, 'Country_Data'))[1]
        print('Country Name: ', each_country_name)

        # Storing Country's Data into DB Table
        try:
            insert_query = f'''INSERT INTO country_status (country_name, country_link, country_status, country_filename)
                                VALUES ('{each_country_name}', '{each_country_link}', 'Pending', '{each_country_filename}');'''
            print(insert_query)
            cursor.execute(insert_query)
        except Exception as e:
            print(e)

    # Fetching data from country's Data from DB Table
    country_data_list = country_fetcher(table_name='country_status', status_column='country_status')
    for this_country in country_data_list:
        this_country_link = this_country.get('country_link')
        this_country_response = page_checker(url=this_country_link, method=method, directory_path=os.path.join(project_files_dir, 'Country_Data'))[0]

        # Regions part
        xpath_regions_href = '//div[@class="regions"]/a/@href'
        parsed_country_html = html.fromstring(html=this_country_response)
        regions_links_absolute = parsed_country_html.xpath(xpath_regions_href)
        each_region_link_list = [base_link + region_link for region_link in regions_links_absolute]

        for each_region_link in each_region_link_list:
            print('Region Link: ', each_region_link)
            each_region_name = each_region_link.split('/')[-2].title() if each_region_link.split('/')[-1].title() == '' else each_region_link.split('/')[-1].title()
            each_region_filename = page_checker(url=each_region_link, method=method, directory_path=os.path.join(project_files_dir, 'Regions_Data'))[1]

            print('Region Name: ', each_region_name)
            # Storing Region's Data into DB Table
            try:
                insert_query = f'''INSERT INTO regions_status (region_name, region_link, region_status, region_filename, country_name)
                                    VALUES ('{each_region_name}', '{each_region_link}', 'Pending', '{each_region_filename}', '{this_country.get('country_name')}');'''
                print(insert_query)
                cursor.execute(insert_query)
            except Exception as e:
                print(e)
        # Updating the country status
        update_query = f'''UPDATE country_status
                            SET country_status = 'Done'
                            WHERE id = {this_country.get('id')};'''
        cursor.execute(update_query)
    # Fetching data from region's Data from DB Table
    regions_data_list = region_fetcher(table_name='regions_status', status_column='region_status')
    for this_region in regions_data_list:
        this_region_name = this_region.get('region_name')
        this_country_name = this_region.get('country_name')
        print('Region Name: ', this_region_name)
        this_region_link = this_region.get('region_link')
        this_region_response = page_checker(url=this_region_link, method=method, directory_path=os.path.join(project_files_dir, 'Regions_Data'))[0]

        # Retrieving Areas with pincode if there are any on region's page
        # Checking if there is Sub-Region
        if not this_region_link.endswith('/'):
            print('Does not have Sub-Region')
            parsed_region_page = html.fromstring(this_region_response)
            xpath_area_container = "//div[contains(@class, 'container')]"
            all_area_containers = parsed_region_page.xpath(xpath_area_container)
            for i in all_area_containers:
                if i.xpath("./div[contains(@class, 'place')]/text()"):
                    print('Area ka naam: ', i.xpath("./div[contains(@class, 'place')]/text()"))
                    this_area_name = i.xpath("./div[contains(@class, 'place')]/text()")[0]
                    this_area_pincodes = i.xpath("./div[contains(@class, 'code')]//text()")
                    for pincode in this_area_pincodes:
                        if pincode != ' ':
                            try:
                                insert_query = f'''INSERT INTO area_status (area_name, area_pincode, sub_region_name, region_name, country_name)
                                                    VALUES ("{this_area_name}", '{pincode}', 'N/A', '{this_region_name}', '{this_country_name}');'''
                                print(insert_query)
                                cursor.execute(insert_query)
                            except Exception as e:
                                print(e)
            print('Outer Areas Done')
        else:
            print('Has Sub-Region')
            # Here you have sub-regions also with areas with pincode, so storing area's data & also sending request on sub-regions page for working on it further

            # Storing the area & pincode data that we are getting in region with sub-regions
            parsed_region_page = html.fromstring(this_region_response)
            xpath_area_container = "//div[contains(@class, 'container')]"
            all_area_containers = parsed_region_page.xpath(xpath_area_container)
            for i in all_area_containers:
                if i.xpath("./div[contains(@class, 'place')]/text()"):
                    print('Area ka naam: ', i.xpath("./div[contains(@class, 'place')]//text()"))
                    this_area_name = i.xpath("./div[contains(@class, 'place')]//text()")[0]
                    this_area_pincodes = i.xpath("./div[contains(@class, 'code')]//text()")
                    for pincode in this_area_pincodes:
                        if pincode != ' ':
                            try:
                                insert_query = f'''INSERT INTO area_status (area_name, area_pincode, sub_region_name, region_name, country_name)
                                                    VALUES ("{this_area_name}", '{pincode}', 'N/A', '{this_region_name}', '{this_country_name}');'''
                                print(insert_query)
                                cursor.execute(insert_query)
                            except Exception as e:
                                print(e)

            # Sending request on each Sub-Region
            xpath_sub_regions_links = '//div[@class="regions"]/a/@href'
            sub_regions_links = parsed_region_page.xpath(xpath_sub_regions_links)
            sub_regions_links_list = [base_link + this_region_link for this_region_link in sub_regions_links]

            for each_sub_region_link in sub_regions_links_list:
                print('Sub-Region Link: ', each_sub_region_link)
                each_sub_region_filename = page_checker(url=each_sub_region_link, method=method, directory_path=os.path.join(project_files_dir, 'Sub_Regions_Data'))[1]
                each_sub_region_name = each_sub_region_link.split('/')[-2].title() if each_sub_region_link.split('/')[-1].title() == '' else each_sub_region_link.split('/')[-1].title()

                print('Sub-Region Name: ', each_sub_region_name)
                # Storing Country's Data into DB Table
                try:
                    insert_query = f'''INSERT INTO sub_regions_status (sub_region_name, sub_region_link, sub_region_status, sub_region_filename, region_name, country_name) VALUES ('{each_sub_region_name}', '{each_sub_region_link}', 'Pending', '{each_sub_region_filename}', '{this_region_name}', '{this_country_name}');'''
                    print(insert_query)
                    cursor.execute(insert_query)
                except Exception as e:
                    print(e)

                # Fetching data from region's Data from DB Table
                sub_region_data_list = sub_region_fetcher(table_name='sub_regions_status', status_column='sub_region_status')
                for this_sub_region_data in sub_region_data_list:
                    this_sub_region_name = this_sub_region_data.get('sub_region_name')
                    this_region_name = this_sub_region_data.get('region_name')
                    this_country_name = this_sub_region_data.get('country_name')
                    print('Sub-Region Name: ', this_sub_region_name)
                    this_sub_region_link = this_sub_region_data.get('sub_region_link')

                    this_sub_region_response = page_checker(url=this_sub_region_link, method=method, directory_path=os.path.join(project_files_dir, 'Sub_Regions_Data'))[0]
                    parsed_sub_region_page = html.fromstring(this_sub_region_response)
                    xpath_area_container = "//div[contains(@class, 'container')]"
                    all_area_containers = parsed_sub_region_page.xpath(xpath_area_container)
                    for i in all_area_containers:
                        if i.xpath("./div[contains(@class, 'place')]/text()"):
                            this_area_name = i.xpath("./div[contains(@class, 'place')]//text()")[0]
                            print('Area ka naam: ', this_area_name)
                            this_area_pincodes = i.xpath("./div[contains(@class, 'code')]//text()")
                            for pincode in this_area_pincodes:
                                if pincode != ' ':
                                    try:
                                        insert_query = f'''INSERT INTO area_status (area_name, area_pincode, sub_region_name, region_name, country_name)
                                                            VALUES ("{this_area_name}", '{pincode}', '{this_sub_region_name}', '{this_region_name}', '{this_country_name}');'''
                                        print(insert_query)
                                        cursor.execute(insert_query)
                                    except Exception as e:
                                        print(e)
                    # Updating the Sub-region status
                    update_query = f'''UPDATE sub_regions_status
                                        SET sub_region_status = 'Done'
                                        WHERE id = {this_sub_region_data.get('id')};'''
                    cursor.execute(update_query)

        # Updating the region status
        update_query = f'''UPDATE regions_status
                            SET region_status = 'Done'
                            WHERE id = {this_region.get('id')};'''
        cursor.execute(update_query)


# Define main URL, method, and path
my_url = "https://worldpostalcode.com/"
my_method = "GET"

# Creating Saved Pages Directory for this Project if not Exists
project_name = 'World_Postal_Code'

project_files_dir = f'C:\\Project Files\\{project_name}_Project_Files'
ensure_dir_exists(path=project_files_dir)

# Call the scraping function with specified parameters
scrape_func(url=my_url, method=my_method, path=os.path.join(project_files_dir, 'Main_Page'))
