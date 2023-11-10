from polygon.websocket import WebSocketClient
from polygon.websocket.models import WebSocketMessage, Market
from polygon import OptionsClient
from polygon import ReferenceClient
from datetime import datetime, timedelta
from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
from pymongo import MongoClient
from typing import List
import re

# If you are running this script for grading
# You should run this script using a terminal separated from app.py
##########################################################################################
# This is a kafka producer with data source from polygon.io optiosn websocket trade data #
# You can run this producer inside market hours                                          #
# Be awasre of LARGE amount of data to be refreshed on the frontend                      #
# If you want less amount of data to be pushed                                           # 
# Shrink the SYMBOLS list to less symbols, at least one needed                           # 
# Market hours: 9:30 - 16:00 Eastern Time                                                #
# Note: This is a SYNC implementation of websocket client                                #
# So should be run as a standalone script in separate terminal                           #
##########################################################################################

# POLYGON API KEY '5400Share'
# This API key is for class project share, will be deleted later
POLYGON_API_KEY = ''

# FIXED SYMBOLS
SYMBOLS = ['SPY','QQQ','AAPL','TSLA','META'] 

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


# Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))
kafka_topic = "real_time_trades"

# Set up MongoDB connection
mongo_client = MongoClient('localhost', 27017)
mongo_db = mongo_client['APAN5400Proj']
mongo_collection = mongo_db['Trades']

# main to be ran
def main():
    # clients and websocket client from polygon.io
    rc = ReferenceClient(POLYGON_API_KEY)
    oc = OptionsClient(POLYGON_API_KEY)
    ws = WebSocketClient(
            market=Market.Options,
            api_key=POLYGON_API_KEY,
            verbose=True)
    
    # get the option tickers to subscribe to
    option_tickers = list()
    for symbol in SYMBOLS:
        option_tickers.extend(get_all_options_tickers_gte(rc, symbol, None, 'list'))
    
    # subscribe each ticker
    for ticker in option_tickers:
        ws.subscribe(get_sub_str('T', ticker))

    # handle msg function to be used 
    def handle_msg(msgs: List[WebSocketMessage]):
        for m in msgs:
            # only need
            sym = m.symbol
            p = m.price
            s = m.size 
            t = m.timestamp
            premium = p * s * 100
            trade_record_to_kafka = {
                'st_sym': extract_alphabet_after_colon(sym),
                'op_sym': sym,
                'p': p,
                's': s,
                't': t,
                'prem': premium
            }
            # Send to Kafka
            producer.send(kafka_topic, trade_record_to_kafka)

            # Insert to mongodb 
            # Disabled for now
            #mongo_collection.insert_one(trade_record_to_kafka)
    
    # this start the websocket client on receiving messages
    ws.run(handle_msg)

    ws.close()


if __name__ == '__main__':
    main()