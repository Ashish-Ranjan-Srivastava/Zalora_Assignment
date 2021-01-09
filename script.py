""" Using ETL Process Loading data from file into bigquery table
Approach -  Reading file into Pandas DataFrame and then
load the data into bulk after doing transformation
"""
import socket
import sys
import logging
import datetime
from datetime import datetime
import requests
import pandas as pd
from google.cloud import bigquery
import pandas_gbq

# pylint: disable= line-too-long, logging-fstring-interpolation, C0103, E0602, redefined-outer-name, global-statement

logging.basicConfig(
    level=logging.DEBUG,
    format='[%(message)s]',
    handlers=[logging.StreamHandler(sys.stdout)]
                    )

logger = logging.getLogger("Zalora-BitCoin Case Study")

logging.info(" ===GET Environment Variables and Target Table Names and Schemas From Config File=== ")

# Reading config file which have database connection and meta data information
# and converting the data into dictionary '''
config_info = dict((line.strip().split(' :: ') for line in open('config.conf')))
print(config_info)

# Declaring global variable
download_url = config_info['url']
target_csv_path = config_info['target_csv_path']
time_window = ['time_window']
socket.getaddrinfo('localhost', 8080)


''' download_file function will download the file locally  in current working directory'''
def download_file():
    """ Making Api Call to storage and writing the content into file """
    try :
        response = requests.get(download_url)
        response.raise_for_status()
        logging.info("Waiting for File to Download !!\n")
    except Exception as e:
        logging.info(f"""Failed to {target_csv_path} File due to {e} """)
    with open(target_csv_path, "wb") as f :
        f.write(response.content)
    logging.info(f"""File {target_csv_path} Downloaded Successfully""")

# Reading the file into pandas datframe
def read_file():
    """ fetching the content of file and loading data into pandas dataframe """
    df = pd.read_csv(target_csv_path)
    # dropping NaN row
    df = df[['Timestamp','Open','High','Low','Close']].dropna(how='any')
    #converting Epoch Time into Normal Timestamp
    df['Timestamp'] = pd.to_datetime(df['Timestamp'],unit='s')
    print(len(df))
    df = df.reset_index()
    del df['index']
    print(df.head())
    helper_df = pd.DataFrame()
    helper_df= df.copy()
    helper_df['Timestamp'] = helper_df['Timestamp'].astype(dtype='datetime64[D]')

    return df,helper_df


def calculate_trades(df,helper_df):
    """ This function will check for successful trades by seperating the records """
    for index in df.index:
        time_diff = df['Timestamp'][index]
        time_diff = str(time_diff).split(' ')[0]
        price = df['Low'][index]
#helper dataframe will use for lookup date
        next_day = datetime.datetime.strptime(time_diff, "%Y-%m-%d") + datetime.timedelta(days=5)
        print(next_day)
# Applying Filter condition
        output = df[df.Timestamp  >= next_day]
        output = output[df.High  > price]
        output['Buying_Date'] = datetime.datetime.strptime(time_diff, "%Y-%m-%d")
        calculate_roi(output,df,helper_df)
        print("helper\n",helper_df.head())
        print("df\n",df.head())
        #print((output.head()))


def calculate_roi(output,df,helper_df):
    """ This function will calculate Rate Of Interest """
    #result = output
    print(helper_df['Low'])
    output['Sell_Price'] = helper_df['Low']
    output = output.rename(columns={'Timestamp': 'Selling_Date'})
    result['Selling_Date'] = output['Selling_Date']
    result['Buy_Date'] = output['Buy_Date']
    result['Sell_Price'] = output['Sell_Price']
    result['Buy_Price'] = helper_df['Low']
    print(result.head())
    # Iterating dataframe for calculation ROI
    for index in df.index:
        result['ROI'][index] = ((result['Sell_Price'][index] - result['Buy_Price'][index])/ result['Buy_Price'][index]) * 100
    # Loading data to BigQuery Table
    load_data_to_bq(result)


def load_data_to_bq(data):
    """ load_to_table will load the dataframe into target table after mapping with the data """
    projectName = config_info['project']
    datasetName = config_info['dataset']
    table = config_info['table']
    key = 'Cloud_Ingestion_Ts' # latest data insertion timestamp
    current_time = [datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-2] for i in result.index]
    current_time = [cur_time  for i in result.index]
    series = pd.Series(current_time, name = 'Cloud_Ingestion_Ts')
    temp_df.insert(0, 'Cloud_Ingestion_Ts', series)
    temp_df[key] = pd.to_datetime(temp_df[key])
    data[key] = temp_df[key]
    destination = datasetName+"."+table
    pandas_gbq.to_gbq(result, destination, projectName, if_exists = 'append')
    print("=====Execution Ended ======",str(datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-5]))

# Execution will start form here
if __name__ == "__main__":
    """ Calling user define functon """
    download_file()
    df,helper_df = read_file()
    calculate_trades(df,helper_df)
