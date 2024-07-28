import pymysql
from pymysql.cursors import DictCursor

# Connecting to the Database
my_host = 'localhost'
my_user = 'root'
my_password = 'actowiz'
my_database = 'world_pincode_new'
my_charset = 'utf8mb4'

connection = pymysql.connect(host=my_host, user=my_user, database=my_database, password=my_password, charset=my_charset, cursorclass=pymysql.cursors.DictCursor, autocommit=True)
if connection.open:
    print('Database connection Successful!')
else:
    print('Database connection Un-Successful.')
cursor = connection.cursor()


def db_schema_creater():
    # Creating countries table if not exists
    create_query = f'''CREATE TABLE IF NOT EXISTS country_status (
                        id INTEGER AUTO_INCREMENT PRIMARY KEY,
                        country_name VARCHAR(255),
                        country_link VARCHAR(255) UNIQUE,
                        country_status VARCHAR(255),
                        country_filename VARCHAR(255));'''
    cursor.execute(create_query)
    print('Countries Table created!')

    # Creating regions table if not exists
    create_query = f'''CREATE TABLE IF NOT EXISTS regions_status (
                        id INTEGER AUTO_INCREMENT PRIMARY KEY,
                        region_name VARCHAR(255),
                        region_link VARCHAR(255) UNIQUE,
                        region_status VARCHAR(255),
                        region_filename VARCHAR(255),
                        country_name VARCHAR(255));'''
    cursor.execute(create_query)
    print('Regions Table created!')

    # Creating Sub_regions table if not exists
    create_query = f'''CREATE TABLE IF NOT EXISTS sub_regions_status (
                        id INTEGER AUTO_INCREMENT PRIMARY KEY,
                        sub_region_name VARCHAR(255),
                        sub_region_link VARCHAR(255) UNIQUE,
                        sub_region_status VARCHAR(255),
                        sub_region_filename VARCHAR(255),
                        region_name VARCHAR(255),
                        country_name VARCHAR(255));'''
    cursor.execute(create_query)
    print('Sub-Regions Table created!')

    # Creating areas table if not exists
    create_query = f'''CREATE TABLE IF NOT EXISTS area_status (
                        id INTEGER AUTO_INCREMENT PRIMARY KEY,
                        area_name VARCHAR(255),
                        area_pincode VARCHAR(255),
                        sub_region_name VARCHAR (255),
                        region_name VARCHAR(255),
                        country_name VARCHAR(255));'''
    cursor.execute(create_query)
    print('Areas Table created!')


# db_schema_creater()
