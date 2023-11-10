#pip install polygon-api-client
#pip install polygon
#pip install flask
#pip install flask-socketio
#pip install kafka-python
#pip install pymongo
# You can find the packages needed in requirements.txt
# Or you can use the virtual environment directly if you are on windows
# source venv/Scripts/activate if you are on windows

from datetime import datetime, timedelta
from polygon import OptionsClient, ReferenceClient
from flask import Flask, render_template_string, request, jsonify
from flask_socketio import SocketIO, emit
from kafka import KafkaConsumer
from pymongo import MongoClient
from polygon import OptionsClient, ReferenceClient
import requests
from requests.exceptions import ReadTimeout
from requests.models import JSONDecodeError
import re
import json

##############################################################################################################
# You can find a readme file in the same directory for complete description and instruction                  #
# readme file will help you better prepare the environment and understande the structure of the project      #
# to avoid hiccups during the setup and running processn                                                     #
# If you have any questions please contact me at yz4482@columbia                                             #
##############################################################################################################

# POLYGON API KEY '5400Share'
# This API key is for class project share, will be deleted later
# Should work through the grading the process
# If the api_key does not work please contact me at yz4482@columbia.edu
# use in production os.getenv()
POLYGON_API_KEY = ''

##############################################################################################################
# Function definitions

def get_today_str():
    return datetime.today().strftime('%Y-%m-%d')

def get_weekdays(start_date, end_date):
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")

    weekdays = []

    while start_date <= end_date:
        # Check if it's a weekday (0: Monday, 1: Tuesday, ..., 6: Sunday)
        if start_date.weekday() < 5:
            weekdays.append(start_date.strftime("%y%m%d"))
        start_date += timedelta(days=1)

    return weekdays

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

def build_option_symbol(underlying_asset, expiration_date, option_type, strike_price):
    return f'O:{underlying_asset}{expiration_date}{option_type}00{strike_price}000'

def is_friday(date_str):
    try:
        # Parse the date string using the format 'yymmdd'
        date_obj = datetime.strptime(date_str, '%y%m%d')
        
        # Check if the day of the week is Friday (weekday() returns 4 for Friday)
        if date_obj.weekday() == 4:
            return True
        else:
            return False
    except ValueError:
        print("Invalid date string format. Please use 'yymmdd'.")
        return False

def extract_alphabet_after_colon(input_string):
    pattern = r':([A-Za-z]+)\d'
    match = re.search(pattern, input_string)
    
    if match:
        return match.group(1)
    else:
        return None

def get_trades_response(optionsClient, symbol, timestamp, stock_symbol):
    try:
        response = optionsClient.get_trades(option_symbol=symbol, timestamp_gte=timestamp, limit=50000, raw_response=True)
        try:
            # Parse the JSON response
            response = response.json()
        except JSONDecodeError:
            print(f"JSONDecodeError for {symbol}, {timestamp}: Response is empty or not a valid JSON")
            return []
    except ReadTimeout:
        print(f"ReadTimeout error on {symbol}, {timestamp}")
        return list()
    
    results = response.get('results')
    
    if results is None:
        return results
    if len(results) == 0:
        return results
    elif len(results) < 50000:
        to_return = list()
        for result in results:
            to_append = dict()
            to_append['st_sym'] = stock_symbol
            to_append['op_sym'] = symbol
            to_append['p'] = result['price']
            to_append['s'] = result['size']
            to_append['prem'] = result['price'] * result['size'] * 100
            to_append['t'] = result['sip_timestamp']// 1000000
            to_return.append(to_append)
        return to_return
    elif len(results) >= 50000:
        print('more than 50000 found')
        to_return = list()
        for result in results:
            to_append = dict()
            to_append['st_sym'] = stock_symbol
            to_append['op_sym'] = symbol
            to_append['p'] = result['price']
            to_append['s'] = result['size']
            to_append['prem'] = result['price'] * result['size'] * 100
            to_append['t'] = result['sip_timestamp']// 1000000
        last_timestamp = results[-1]['sip_timestamp']
        to_return.extend(get_trades_response(optionsClient, symbol, last_timestamp,stock_symbol))
        return to_return
    
    return results

def parse_option_symbol(option_symbol):
    expiration_date = option_symbol[-15:-9]
    option_type = option_symbol[-9]
    strike_price_str = option_symbol[-8:-3]
    strike_price = float(strike_price_str)
    
    return expiration_date, option_type, strike_price

def get_header_str(stock_symbol,start_date, end_date,filter_type,filter_comparison,filter_value,sort_value,sort_type):
    if filter_comparison == 'gte':
        filter_comparison = '>='
    elif filter_comparison == 'lte':
        filter_comparison = '<='
    if filter_type == 'prem':
        filter_type = 'premium'
        filter_value = "${:,.0f}".format(filter_value)
    elif filter_type == 's':
        filter_type = 'trade size'
    if sort_value == 't':
        sort_value = 'timestamp'
    elif sort_value == 'p':
        sort_value = 'price'
    elif sort_value == 's':
        sort_value = 'size'
    if sort_type == 'Desc':
        sort_type = 'descending'
    elif sort_type == 'Aesc':
        sort_type = 'ascending'
    
    if start_date == end_date:
        date_str = f'on {start_date}'
    else:
        date_str = f'from {start_date} to {end_date}'
    
    header_str = f'Options trades of {stock_symbol} {date_str} with {filter_type} {filter_comparison} {filter_value} sorted by {sort_value} {sort_type}'
    return header_str

def parse_results_to_table_html(result, sort_value, sort_type, header_str):
    if sort_type == 'Desc':
        result = sorted(result, key=lambda x: x[sort_value], reverse=True)
    elif sort_type == 'Aesc':
        result = sorted(result, key=lambda x: x[sort_value], reverse=False)


    table_rows = ""
    for record in result:
        record['_id'] = str(record['_id'])  # Add this line to convert ObjectId to a string
        stock_symbol = record["st_sym"]
        price = record["p"]
        size = record["s"]
        premium = "${:,.0f}".format(int(record["prem"]))
        option_symbol = record["op_sym"]
        timestamp = datetime.fromtimestamp(record["t"] / 1000).strftime('%Y-%m-%d %H:%M:%S')

        expiration_date, option_type, strike_price = parse_option_symbol(option_symbol)
        
        expiration_date = datetime.strptime(expiration_date, "%y%m%d").strftime("%Y-%m-%d")
        option_type = "Call" if option_type == "C" else "Put"

        table_rows += f"<tr><td>{stock_symbol}</td><td>{price}</td><td>{size}</td><td>{premium}</td><td>{expiration_date}</td><td>{option_type}</td><td>{strike_price}</td><td>{timestamp}</td></tr>"


    total_records = len(result)
    table_html = f"""
    <html>
    <head>
        <style>
            body {{
                font-family: Arial, sans-serif;
                margin: 0;
                padding: 0;
            }}
            .back-btn {{
                background-color: grey;
                color: white;
                text-decoration: none;
                padding: 10px;
                margin-bottom: 20px;
            }}
            .back-btn:hover {{
                background-color: #45a049;
            }}
            .container {{
                padding: 20px;
            }}
            table {{
                border-collapse: collapse;
                width: 100%;
            }}
            th, td {{
                border: 1px solid #ccc;
                padding: 10px;
                text-align: left;
            }}
            th {{
                background-color: #f2f2f2;
                font-weight: bold;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <a href="/" class="back-btn">Back to Homepage</a>
            <h2>{header_str}</h2>
            <h2>Total records found: {total_records}</h2>
            <table>
                <thead>
                    <tr>
                        <th>Stock Symbol</th>
                        <th>Price</th>
                        <th>Size</th>
                        <th>Premium</th>
                        <th>Expiration Date</th>
                        <th>Option Type</th>
                        <th>Strike Price</th>
                        <th>Timestamp</th>
                    </tr>
                </thead>
                <tbody>
                    {table_rows}
                </tbody>
            </table>
        </div>
    </body>
    </html>
    """

    return table_html

##############################################################################################################

rc = ReferenceClient(POLYGON_API_KEY, read_timeout=15)
oc = OptionsClient(POLYGON_API_KEY, read_timeout=15)

# FIXED SYMBOLS
symbols = ['SPY','QQQ','AAPL','TSLA','META'] 

# Get all option tickers for the symbols above
option_symbols = list()
all_trades = list()

# cutoff dates for using the API to fetch data
# should return about 1.59 Gb size of data if exported to a json file
start_date = "2023-04-03" 
end_date = "2023-04-24"
all_weekdays = get_weekdays(start_date, end_date)


# DATA FETCHING METHOD 1: USING POLYGON API
'''IMPORTANT:
# The function is for fetching data from Polygon API.
# It is very TIME CONSUMING: takes about MORE THAN AN HOUR to fetch all data
# In addition, if the code is ran outside of the market hours(9:30am-4pm on weekdays eastern time), 
# polygon API will return missing or empty data because it RESTRICTS traffic outside these hours
# Therefore, it is suggested to use this methond ONLY DURING MARKET HOURS(9:30am-4pm on weekdays eastern time)
# Suggested to use the data file provided in the next section below
# The data file was prepared through running this code section on March 24, 2023
# DATA FETCHING METHOD USING POLYGON API
'''
def get_all_trades_from_polygon_source(symbols, all_weekdays, option_symbols, all_trades):
    for symbol in symbols:
        for expiration_date in all_weekdays:
            if symbol == 'SPY':
                for strike_price in range(340,440):
                    option_symbols.append(build_option_symbol(symbol, expiration_date,'C',strike_price))
                    option_symbols.append(build_option_symbol(symbol, expiration_date,'P',strike_price))
                    
            elif symbol =='QQQ':
                for strike_price in range(200,350):
                    option_symbols.append(build_option_symbol(symbol, expiration_date,'C',strike_price))
                    option_symbols.append(build_option_symbol(symbol, expiration_date,'P',strike_price))
                    
            if is_friday(expiration_date):
                if symbol == 'AAPL':
                    strike_price = 100
                    end = 200
                    step_size = 2.5
                    while strike_price < end:
                        option_symbols.append(build_option_symbol(symbol, expiration_date, 'C', strike_price))
                        option_symbols.append(build_option_symbol(symbol, expiration_date, 'P', strike_price))
                        strike_price += step_size
                elif symbol == 'TSLA':
                    strike_price = 60
                    end = 300
                    step_size = 2.5
                    while strike_price < end:
                        option_symbols.append(build_option_symbol(symbol, expiration_date, 'C', strike_price))
                        option_symbols.append(build_option_symbol(symbol, expiration_date, 'P', strike_price))
                        strike_price += step_size
                elif symbol == 'META':
                    strike_price = 80
                    end = 250
                    step_size = 2.5
                    while strike_price < end:
                        option_symbols.append(build_option_symbol(symbol, expiration_date, 'C', strike_price))
                        option_symbols.append(build_option_symbol(symbol, expiration_date, 'P', strike_price))
                        strike_price += step_size

    for symbol in symbols:
        option_symbols.extend(get_all_options_tickers_gte(rc, symbol, expiration_date=end_date))

    for symbol in option_symbols:
        trades = list()
        stock_symbol = extract_alphabet_after_colon(symbol)
        trades = get_trades_response(oc, symbol, start_date,stock_symbol)
        all_trades.extend(trades)

# USING DATA FETCHING METHOD 1: USING POLYGON API
# Uncomment the following line to use this method
#get_all_trades_from_polygon_source(symbols, all_weekdays, option_symbols, all_trades)



# DATA FETCHING METHOD 2: USING PREPARED DATA FILE which was pre-generated using the above method on March 24, 2023
'''IMPORTANT:
Suggested method to get data from prepared data file
The prepared data file is approximately 1.59GB in size
DO NOT USE THIS METHOD IF USING OTHER DATA FETCHING METHOD
Filename: all_trades_final.json
This is file is 1.59gb in size with cutoff date from 2023-04-03 to 2023-04-25
In the same directory, you will find all_trades.json which is 4.5gb in size
This file contains has a trade record cutoff date from 2023-01-01 to 2023-04-24 and option symbols expiring after 2023-04-17 on the fixed symbols
It is advised to use the all_trades_final.json file as it is smaller in size and has a more recent cutoff date
The hardcoded cutoff date in the code is from 2023-04-03 to 2023-04-24 so it should work as the all_trades_final.json NOT the all_trades.json
'''
def get_all_trades_from_file(all_trades):
    with open('all_trades_final.json', 'r') as f:
        print('Loading data from file...')
        all_trades = json.load(f)
        print('Data loading completed!')

# USING DATA FETCHING METHOD 2: USING PREPARED DATA FILE
# RUN THIS CODE CHUNK ONLY IF YOU NEED TO INSERT DATA
# Uncomment the following line to use this method
#get_all_trades_from_file(all_trades)



# Connect to MongoDB
client = MongoClient('localhost', 27017)

# Create or connect db at 'APAN5400Proj'
db = client['APAN5400Proj']

# create or connect collection at 'Trades'
trades_collection = db['Trades']

# This part is database initialization and data insertion
# RUN THIS PART ONLY ONCE
def set_indexes(trades_collection):
    trades_collection.create_index('st_symbol') #stock symbol
    trades_collection.create_index('prem') #premium
    trades_collection.create_index('s') #size
    trades_collection.create_index('t') #timestamp in miliseconds
    trades_collection.create_index([("st_symbol", 1), ("t", 1), ("prem", 1)])
    trades_collection.create_index([("st_symbol", 1), ("t", 1), ("s", 1)])
    trades_collection.create_index([("t", 1), ("prem", 1)])
    trades_collection.create_index([("t", 1), ("s", 1)])
set_indexes(trades_collection)
# You should comment this part out if you already created indexes

# ACTIAL DATA INSERTION
# RUN THIS PART ONLY ONCE IF DATABASE DOES NOT ALREADY HAVE
#print('Inserting data into database...')
#trades_collection.insert_many(all_trades)
#print('Data insertion completed!')


# flask app config
app = Flask(__name__)
app.config['SECRET_KEY'] = 'secret!'
socketio = SocketIO(app, cors_allowed_origins="*")

# Kafka connection
# You should have zookeeper and kafka running on Docker
kafka_server = 'localhost:9092'
kafka_topic = 'real_time_trades'

# stream trade function from kafka
def stream_trade():
    consumer = KafkaConsumer(
        kafka_topic,
        bootstrap_servers=[kafka_server],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest',
        enable_auto_commit=True,
    )

    for msg in consumer:
        trade = msg.value
        socketio.emit('new_trade', trade)

@socketio.on('connect')
def start_stream_trade():
    socketio.start_background_task(stream_trade)

@app.route('/')
def main():
    form_html = '''
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>APAN5400Proj</title>
        <style>
            body {
                font-family: Arial, sans-serif;
                display: flex;
                flex-direction: column;
                justify-content: center;
                align-items: center;
                height: 100vh;
                margin: 0;
            }
            h1 {
                text-align: center;
            }
            form {
                display: flex;
                flex-direction: column;
                align-items: center;
            }
            .input-group {
                display: flex;
                flex-direction: column;
                align-items: center;
                margin-bottom: 1rem;
            }
            label {
                margin-top: 1rem;
                font-weight: bold;
            }
            input, select {
                margin-top: 0.5rem;
            }
            .input-row {
                display: flex;
                justify-content: space-around;
                width: 100%;
            }
            input[type="submit"] {
                width: 150px;
                margin-top: 1rem;
                background-color: grey;
                color: white;
                border: 1px solid grey;
                padding: 10px;
                cursor: pointer;
            }
            input[type="submit"]:hover {
                background-color: #45a049;
            }
            .table-container {
                width: 80%;
                max-height: 300px;
                overflow-y: auto;
                margin-top: 1rem;
                border: 1px solid #ddd;
                box-shadow: 0 4px 8px rgba(0, 0, 0, 0.1);
                border-radius: 4px;
            }
            table {
                border-collapse: collapse;
                width: 100%;
            }
            th, td {
                border: 1px solid #ddd;
                padding: 8px;
                text-align: center;
            }
            th {
                background-color: #f2f2f2;
                font-weight: bold;
            }
        </style>
    </head>
    <script src="https://cdn.socket.io/4.0.0/socket.io.min.js"></script>
    <script>
        function parseOptionSymbol(optionSymbol) {
            const expirationDate = optionSymbol.slice(-15, -9);
            const optionType = optionSymbol.slice(-9, -8);
            const strikePrice = parseFloat(optionSymbol.slice(-8, -3));
            return [expirationDate, optionType, strikePrice];
        }

        document.addEventListener('DOMContentLoaded', () => {
            const socket = io.connect('http://' + document.domain + ':' + location.port);

            socket.on('new_trade', trade => {
                const row = document.createElement('tr');

                const stockSymbol = trade['st_sym'];
                const price = trade['p'];
                const size = trade['s'];
                const premium = `$${parseInt(trade['prem']).toLocaleString()}`;
                const optionSymbol = trade['op_sym'];
                const timestamp = new Date(trade['t']).toLocaleString();

                console.log(optionSymbol);
                const [expirationDate, optionType, strikePrice] = parseOptionSymbol(optionSymbol);
                console.log(expirationDate, optionType, strikePrice);

                const formattedExpirationDate = '20'+ expirationDate.slice(0, 2) + '-' + expirationDate.slice(2, 4) + '-' + expirationDate.slice(4, 6);
                const formattedOptionType = optionType === 'C' ? 'Call' : 'Put';
                
                const values = [stockSymbol, price, size, premium, formattedExpirationDate, formattedOptionType, strikePrice, timestamp];
                
                values.forEach(value => {
                    const cell = document.createElement('td');
                    cell.textContent = value;
                    row.appendChild(cell);
                });

                const table = document.getElementById('trade_table');
                const header = table.getElementsByTagName('thead')[0];
                header.parentNode.insertBefore(row, header.nextSibling);
            });
        });
    </script>
    <body>
        <h1>APAN5400 Group 13</h1>
        <h1>Option Market Trade Monitor</h1>
        <form action="/process" method="post">
            <div class="input-row">
                <div class="input-group">
                    <label for="stock_symbol">Stock Symbol</label>
                    <select name="stock_symbol" id="stock_symbol" value="SPY">
                        <option value="all stocks">All Stocks</option>
                        <option value="SPY">SPY</option>
                        <option value="QQQ">QQQ</option>
                        <option value="AAPL">AAPL</option>
                        <option value="TSLA">TSLA</option>
                        <option value="META">META</option>
                        <option value="AMZN">AMZN</option>
                        <option value="MSFT">MSFT</option>
                        <!-- Add more options as needed -->
                    </select>
                </div>
                <div class="input-group">
                    <label for="start_date">Start Date</label>
                    <input type="date" name="start_date" id="start_date" min="2023-04-03" value="2023-04-03">
                </div>
                <div class="input-group">
                    <label for="end_date">End Date</label>
                    <input type="date" name="end_date" id="end_date" min="2023-04-03" value="2023-04-24">
                </div>
                <div class="input-group">
                    <label for="filter_type">Filter</label>
                    <select name="filter_type" id="filter_type">
                        <option value="prem">Premium</option>
                        <option value="s">Trade size</option>
                    </select>
                </div>
                <div class="input-group">
                    <label for="filter_comparison">Comparison</label>
                    <select name="filter_comparison" id="filter_comparison">
                        <option value="gte">Greater than or equal to</option>
                        <option value="lte">Less than or equal to</option>
                    </select>
                </div>
                <div class="input-group">
                    <label for="filter_value">Amount</label>
                    <input type="number" name="filter_value" id="filter_value" value="1000000" min="0">
                    </select>
                </div>
                <div class="input-group">
                    <label for="sort_value">Sort Value</label>
                    <select name="sort_value" id="sort_value">
                        <option value="t">Timestamp</option>
                        <option value="prem">Premium</option>
                        <option value="p">Option Price</option>
                        <option value="s">Trade Size</option>

                    </select>
                </div>
                <div class="input-group">
                    <label for="sort_type">Sort</label>
                    <select name="sort_type" id="sort_type">
                        <option value="Desc">Descending</option>
                        <option value="Aesc">Aescending</option>
                    </select>
                </div>
            </div>
            <input type="submit" value="Submit" onclickvalue="process_form()">
        </form>
        <div class="table-container">
            <table id="trade_table">
                <thead>
                    <tr>
                        <th>Stock Symbol</th>
                        <th>Price</th>
                        <th>Size</th>
                        <th>Premium</th>
                        <th>Expiration Date</th>
                        <th>Option Type</th>
                        <th>Strike Price</th>
                        <th>Timestamp</th>
                    </tr>
                </thead>
                <tbody>
                    <!-- Table Rows-->
                </tbody>
            </table>
        </div>
    </body>
    </html>
    '''
    return render_template_string(form_html)

@app.route('/process', methods=['POST'])
def process_form():
    stock_symbol = request.form['stock_symbol']
    start_date = request.form['start_date']
    end_date = request.form['end_date']
    filter_type = request.form['filter_type']
    filter_comparison = request.form['filter_comparison']
    filter_value = float(request.form['filter_value'])
    sort_type = request.form['sort_type']
    sort_value = request.form['sort_value']

    # Convert the date to start and end timestamps
    # convert to nanoseconds: *1000000
    # convert to miliseconds: *1000
    # database should be using miliseconds
    start_date_obj = datetime.strptime(start_date, "%Y-%m-%d")
    end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")
    start_timestamp = int(datetime(start_date_obj.year, start_date_obj.month, start_date_obj.day, 0, 0).timestamp() * 1000)
    end_timestamp = int(datetime(end_date_obj.year, end_date_obj.month, end_date_obj.day, 23, 59).timestamp() * 1000)

    if end_timestamp < start_timestamp:
        end_timestamp = int(datetime(start_date_obj.year, start_date_obj.month, start_date_obj.day, 23, 59).timestamp() * 1000)

    # Build query
    query = {
        "st_sym": stock_symbol,
        "t": {"$gte": start_timestamp, "$lte": end_timestamp},
    }
    if stock_symbol == "all stocks":
        del query["st_sym"]
    if filter_comparison == "gte":
        query[filter_type] = {"$gte": filter_value}
    elif filter_comparison == "lte":
        query[filter_type] = {"$lte": filter_value}

    # query from database
    results = list(trades_collection.find(query))

    # produce the header str to display above the table
    header_str = get_header_str(stock_symbol, start_date, end_date, filter_type, filter_comparison, filter_value, sort_value, sort_type)

    # parse to html table(result page)
    table_html = parse_results_to_table_html(results, sort_value, sort_type, header_str)

    return render_template_string(table_html)

if __name__ == '__main__':
    socketio.run(app, host='0.0.0.0', port=8080)