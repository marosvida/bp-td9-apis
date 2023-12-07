import requests
import sqlite3
import json

#1 Get a list of all available cryptocurrencies and display it
def get_all_symbols():
    url = "https://api.binance.com/api/v3/exchangeInfo"

    print("Getting symbols...")
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        # Parse and print the JSON response
        data = response.json()

        symbols = data.get('symbols', [])

        # Extract and print only the name of symbol
        for item in symbols:
            print(item.get('symbol'))
    else:
        print(f"Failed to retrieve data: Status code {response.status_code}")

#2 Create a function to display the ’ask’ or ‘bid’ price of an asset. Direction and asset name as parameters
def getDepth(direction, pair):
    url = "https://api.binance.com/api/v3/depth"

    parameters = {
        "symbol": pair,
    }

    print("Retrieving data...")
    response = requests.get(url, params=parameters)

    # Check if the request was successful
    if response.status_code == 200:
        # Parse and print the JSON response
        data = response.json()

        if direction == "ask":
            print("Ask prices for " + pair + ": ")
            print(data.get('asks'))
        elif direction == "bid":
            print("Bid prices for " + pair + ": ")
            print(data.get('bids'))
        else:
            print("Invalid direction")
    else:
        print(f"Failed to retrieve data: Status code {response.status_code}")

#3 Get order book for an asset
def getOrderBook(pair):
    url = "https://api.binance.com/api/v3/depth"

    parameters = {
        "symbol": pair,
    }

    print("Retrieving Order book data...")
    response = requests.get(url, params=parameters)

    # Check if the request was successful
    if response.status_code == 200:
        # Parse and print the JSON response
        data = response.json()

        print("Order book for " + pair + ": ")
        print(data)
    else:
        print(f"Failed to retrieve data: Status code {response.status_code}")

#4 Create a function to read agregated trading data (candles)
def refreshDataCandle(pair='BTCUSDT', duration='5m'):
    url = "https://api.binance.com/api/v3/klines"

    # Parameters for the API call
    params = {
        'symbol': pair,
        'interval': duration
    }

    print("Retrieving candle data...")
    response = requests.get(url, params=params)

    # Check if the request was successful
    if response.status_code == 200:
        print("Retrieved data for " + pair + " for " + duration + " duration")
        return response.json()
    else:
        print(f"Failed to retrieve data: Status code {response.status_code}")
        return None

#5 Create a sqlite table to store said data
def create_candlestick_table(db_file):
    # Connect to the SQLite database
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # SQL query to create a table
    create_table_query = """
    CREATE TABLE IF NOT EXISTS candlestick_data (Id INTEGER PRIMARY KEY,
        date INT, high REAL, low REAL, open REAL, close
        REAL, volume REAL)
    """

    # Execute the query
    cursor.execute(create_table_query)

    # Commit changes and close the connection
    conn.commit()
    conn.close()
    print("Table created successfully")

#6 Store candle data in the db
def insert_candlestick_data(db_name, candlestick_json):
    # Connect to the SQLite database
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # Prepare the insert statement
    insert_query = """
    INSERT INTO candlestick_data (date, high, low, open, close, volume)
    VALUES (?, ?, ?, ?, ?, ?);
    """

    # Extract data from JSON and insert into the table
    for entry in candlestick_json:
        data_to_insert = (
            entry[0],   # date
            float(entry[1]),  # high
            float(entry[2]),  # low
            float(entry[3]),  # open
            float(entry[4]),  # close
            float(entry[5])   # volume
        )
        
        # Execute the insert statement
        cursor.execute(insert_query, data_to_insert)

    # Commit changes and close the connection
    conn.commit()
    conn.close()
    print("Data inserted successfully")

if __name__ == '__main__':
    #1 Get a list of all available cryptocurrencies and display it
    get_all_symbols()

    #2 Get bid/ask price for an asset
    getDepth(direction="ask", pair ="BTCUSDT")

    #3 Get order book for an asset
    getOrderBook(pair="BTCUSDT")

    #4 Get candle data
    candle_data = refreshDataCandle(pair='BTCUSDT', duration='5m')
    print(candle_data)

    #5 Create a sqlite table to store candle_stick data
    db_name = 'bp-td9.db'
    create_candlestick_table(db_name)

    #6 Store candle data in the db
    transformed_candle_data = []
    for candle in candle_data:
        transformed_candle_data.append([
            candle[0],  # date
            float(candle[2]),  # high
            float(candle[3]),  # low
            float(candle[1]),  # open
            float(candle[4]),  # close
            float(candle[5])   # volume
        ])
    insert_candlestick_data(db_name, transformed_candle_data)

    