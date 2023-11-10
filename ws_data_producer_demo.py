from polygon.websocket import WebSocketClient
from polygon.websocket.models import WebSocketMessage, Market
from polygon import OptionsClient
from polygon import ReferenceClient
from datetime import datetime, timedelta
from kafka import KafkaProducer
from kafka.errors import KafkaError
import random
import time
import json
from pymongo import MongoClient
from typing import List
import re


# If you are running this script for grading
# You should run this script using a terminal separated from app.py
##########################################################################################
# This is a kafka producer with data source RANDOMLY generated, NOT USING polygon.io     #
# You should run this producer instead of ws_data_producer.py outside market hours       #
# Market hours: 9:30 - 16:00 Eastern Time                                                #
##########################################################################################

# POLYGON API KEY '5400Share'
# This API key is for class project share, will be deleted later
POLYGON_API_KEY = ''

##########################################################################################
# Function definitions

def get_sub_str(type: str, symbol: str):
    return type + '.' + symbol

def get_today_str():
    return datetime.today().strftime('%Y-%m-%d')

def extract_alphabet_after_colon(input_string):
    pattern = r':([A-Za-z]+)\d'
    match = re.search(pattern, input_string)
    
    if match:
        return match.group(1)
    else:
        return None
    
def build_option_symbol(underlying_asset, expiration_date, option_type, strike_price):
    return f'O:{underlying_asset}{expiration_date}{option_type}00{strike_price}000'

def get_all_options_tickers_gte(reference_client: ReferenceClient, 
                                underlying_symbol: str, 
                                expiration_date: str, 
                                return_type: str = 'list'):
    if expiration_date is None:
        expiration_date = get_today_str()
    option_tickers_set = set()
    option_tickers_list = list()

    last_date = expiration_date
    to_continue = True

    while(to_continue):
        response = reference_client.get_option_contracts(underlying_ticker=underlying_symbol, 
                                                         expiration_date_gte=last_date)
        contracts = response.get('results')
        if contracts is not None:
            for contract in contracts:
                if contract['ticker'] not in option_tickers_set:
                    option_tickers_list.append(contract['ticker'])
                    option_tickers_set.add(contract['ticker'])
                last_date = contract['expiration_date']
                
            if len(contracts) < 1000:
                to_continue = False
        else:
            to_continue = False
        
    if return_type == 'set':
        return option_tickers_set
    return option_tickers_list

def convert_nanosecond_timestamp_to_milisecond_timestamp(timestamp: int):
    return timestamp // 1000000

# Function definitions
##########################################################################################

# Kafka producer and topic
producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))
kafka_topic = "real_time_trades"

# Set up MongoDB connection
# Can be neglected because this program is not pushing randomly generated data to database
mongo_client = MongoClient('localhost', 27017)
mongo_db = mongo_client['APAN5400Proj']
mongo_collection = mongo_db['Trades']

# FIXED SYMBOLS
# Do not change, program functions not fully linked to this fixed symbol list
SYMBOLS = ['SPY','QQQ','AAPL','TSLA','META'] 

# main to be ran
def main():
    n = 1
    # This program should last for slightly more than 7200 seconds
    # sleep one second after each iteration
    while n <= 7200:
        # Stock symbol set to 'DEMO' to indicate record is for demo purposes
        st_sym = 'DEMO'
        expiration_date = '230425'
        option_type = random.choice(['C', 'P'])
        strike_price = random.randint(100, 300)
        op_sym = build_option_symbol(st_sym, expiration_date, option_type, strike_price)

        # Calculate the min and max price based on the strike price
        max_price = round(100 / strike_price * 100, 2)
        min_price = max(0.01, max_price - 100)

        # Generate a random price within the calculated range
        p = round(random.uniform(min_price, max_price), 2)
        s = random.randint(1, 10000)

        # set t to be timestamp now in miliseconds
        t = datetime.now().timestamp() * 1000
        prem = int(p * s * 100)

        # compose data to produce
        trade_record_to_kafka = {
            'st_sym': st_sym,
            'op_sym': op_sym,
            'p': p,
            's': s,
            't': t,
            'prem': prem
        }

        # send record to kafka
        producer.send(kafka_topic, trade_record_to_kafka)
        # sleep one second at each iteration
        time.sleep(1)


# should be run separately from app.py
if __name__ == '__main__':
    main()