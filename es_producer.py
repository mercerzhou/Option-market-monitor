from polygon.websocket import WebSocketClient
from polygon.websocket.models import WebSocketMessage, Market
from polygon import OptionsClient
from polygon import ReferenceClient
from datetime import datetime, timedelta
import json
from pymongo import MongoClient
from typing import List
import re
import asyncio
import nest_asyncio
from elasticsearch import Elasticsearch

POLYGON_API_KEY = ''

# FIXED SYMBOLS
SYMBOLS = ['SPY','QQQ','AAPL','TSLA','META'] 

quotes = dict()

es = Elasticsearch([{'host': 'localhost', 'port': 9200, 'scheme': 'http'}])

# Step 2: Define the mapping
trade_mapping = {
    "mappings": {
        "properties": {
            "type": {
                "type": "keyword"
            },
            "symbol": {
                "type": "keyword"
            },
            "timestamp": {
                "type": "date",
                "format": "epoch_millis"
            },
            "price": {
                "type": "float"
            },
            "size": {
                "type": "integer"
            },
            "prem": {
                "type": "integer"
            },
            "option_type": {
                "type": "keyword"
            },
            "buy_sell": {
                "type": "keyword"
            },
            "trade_range": {
                "type": "keyword"
            },
            "bid_price": {
                "type": "float"
            },
            "bid_size": {
                "type": "integer"
            },
            "ask_price": {
                "type": "float"
            },
            "ask_size": {
                "type": "integer"
            },
            "quote_timestamp": {
                "type": "date",
                "format": "epoch_millis"
            }
        }
    }
}

# Define the mapping for quote data
quote_mapping = {
    "mappings": {
        "properties": {
            "type": {
                "type": "keyword"
            },
            "symbol": {
                "type": "keyword"
            },
            "bid_price": {
                "type": "float"
            },
            "bid_size": {
                "type": "integer"
            },
            "ask_price": {
                "type": "float"
            },
            "ask_size": {
                "type": "integer"
            },
            "timestamp": {
                "type": "date",
                "format": "epoch_millis"
            }
        }
    }
}

# Step 3: Create the index
trade_index = "option_trade_data"
if not es.indices.exists(index=trade_index):
    es.indices.create(index=trade_index, body=trade_mapping)
    print(f"Index {trade_index} created successfully!")
else:
    print(f"Index {trade_index} already exists!")
    
quote_index = "option_quote_data"
if not es.indices.exists(index=quote_index):
    es.indices.create(index=quote_index, body=quote_mapping)
    print(f"Index {quote_index} created successfully!")
else:
    print(f"Index {quote_index} already exists!")


# main to be ran
def main():
    try:
        # clients and websocket client from polygon.io
        rc = ReferenceClient(POLYGON_API_KEY)
        oc = OptionsClient(POLYGON_API_KEY)
        ws = WebSocketClient(
                market=Market.Options,
                api_key=POLYGON_API_KEY,
                verbose=False)

        option_tickers = get_all_options_tickers_on(rc, 'SPY', None)

        global quotes
        global es

        for ticker in option_tickers:
            quotes[ticker] = dict()
            quotes[ticker]['bid_price'] = 0
            quotes[ticker]['ask_price'] = 0
            quotes[ticker]['bid_size'] = 0
            quotes[ticker]['ask_size'] = 0
            quotes[ticker]['timestamp'] = int(datetime.now().timestamp() * 1000)
            ws.subscribe(get_sub_str('T', ticker))
            ws.subscribe(get_sub_str('Q', ticker))

        print('subscribe all')

        # handle msg function to be used 
        def handle_msg(msgs: List[WebSocketMessage]):
            for m in msgs:
                if m.event_type == 'T':
                    bid_price = quotes[m.symbol]['bid_price']
                    ask_price = quotes[m.symbol]['ask_price']

                    price_comp = int(m.price * 100)
                    bid_comp = int(bid_price * 100)
                    ask_comp = int(ask_price * 100)

                    prem = int(m.size * m.price * 100)
                    compare = 'At Mid'
                    buy_sell = ''
                    mid = ((bid_price+ask_price) / 2)
                    mid_comp = (bid_comp + ask_comp) / 2

                    if price_comp == mid_comp:
                        compare = 'At Mid'
                    elif price_comp == ask_comp:
                        compare = 'At Ask'
                        buy_sell = 'Buy'
                    elif price_comp == bid_comp:
                        compare = 'At Bid'
                        buy_sell = 'Sell'
                    elif price_comp > ask_comp:
                        compare = 'Above Ask'
                        buy_sell = 'Buy'
                    elif price_comp < bid_comp:
                        compare = 'Below Bid'
                        buy_sell = 'Sell'
                    elif price_comp > mid_comp:
                        compare = 'Above Mid Below Ask'
                        buy_sell = 'Buy'
                    elif price_comp < mid_comp:
                        compare = 'Below Mid Above Bid'
                        buy_sell = 'Sell'
                    elif price_comp == mid_comp:
                        compare = 'At Mid'
                    else:
                        print(bid_price, ask_price, m.price)

                    doc = {
                        'type': 'trade',
                        'symbol': m.symbol,
                        'timestamp': m.timestamp,
                        'price': m.price,
                        'size': m.size,
                        'prem': prem,
                        'option_type': m.symbol[11],
                        'buy_sell': buy_sell,
                        'trade_range': compare,
                        'bid_price': quotes[m.symbol]['bid_price'],
                        'bid_size': quotes[m.symbol]['bid_size'],
                        'ask_price': quotes[m.symbol]['ask_price'],
                        'ask_size': quotes[m.symbol]['ask_size'],
                        'quote_timestamp': quotes[m.symbol]['timestamp'],
                    }

                    try:
                        es.index(index='option_trade_data', document=doc)
                    except Exception as e:
                        print(e)

                    if prem >= 100000:
                        call_put = 'call'
                        if m.symbol[11] == 'P':
                            call_put = 'put '
                        print(buy_sell,call_put, m.symbol, '$',prem, m.size,compare, 'bid:',bid_price, 'ask:', ask_price, 'price:', m.price)
                elif m.event_type == 'Q':
                    if m.bid_price and m.ask_price and m.timestamp:
                        quotes[m.symbol]['bid_price'] = m.bid_price
                        quotes[m.symbol]['ask_price'] = m.ask_price
                        quotes[m.symbol]['bid_size'] = m.bid_size
                        quotes[m.symbol]['ask_size'] = m.ask_size
                        quotes[m.symbol]['timestamp'] = int(m.timestamp)

                        doc = quotes[m.symbol]
                        doc['type'] = 'quote'
                        doc['symbol'] = m.symbol

                        try:
                            es.index(index='option_quote_data', document=doc)
                        except Exception as e:
                            print(e)

        
        # this start the websocket client on receiving messages
        ws.run(handle_msg)

    except GeneratorExit as e:
        print(f"An error occurred: {e}")

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

def get_all_options_tickers_on(reference_client: ReferenceClient, 
                                underlying_symbol: str, 
                                expiration_date: str, 
                                return_type: str = 'list'):
    if expiration_date is None:
        expiration_date = get_today_str()
    option_tickers_set = set()
    option_tickers_list = list()

    last_date = expiration_date
    to_continue = True

    reponse = reference_client.get_option_contracts(underlying_ticker=underlying_symbol,
                                                    expiration_date=expiration_date,
                                                    all_pages=True)
    for contract in reponse:
        if contract['ticker'] not in option_tickers_set:
            option_tickers_list.append(contract['ticker'])
            option_tickers_set.add(contract['ticker'])
    
    return option_tickers_list

def convert_nanosecond_timestamp_to_milisecond_timestamp(timestamp: int):
    return timestamp // 1000000

if __name__ == '__main__':
    main()