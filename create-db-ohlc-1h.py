import os
import sqlite3
import csv

# Function to extract information from file names
def extract_info(filename):
    # Assuming the ticker is the first part of the filename before any digits or '-' character
    ticker = ''.join(filter(lambda x: x.isalpha(), filename.split('-')[0]))
    trade_type = "Call" if ord(filename[4]) <= ord('L') else "Put"
    
    # Extract expiration month based on the 5th character of the original filename part
    month_mapping = {
        'A': 'January', 'B': 'February', 'C': 'March', 'D': 'April', 'E': 'May', 'F': 'June', 
        'G': 'July', 'H': 'August', 'I': 'September', 'J': 'October', 'K': 'November', 'L': 'December',
        'M': 'January', 'N': 'February', 'O': 'March', 'P': 'April', 'Q': 'May', 'R': 'June', 
        'S': 'July', 'T': 'August', 'U': 'September', 'V': 'October', 'W': 'November', 'X': 'December'
    }
    expiration_month = month_mapping.get(filename[4], "")
    
    # Extract strike price
    strike_price = ''.join(filter(str.isdigit, filename.split('-')[0]))
    
    return ticker, trade_type, strike_price, expiration_month

# Directory containing the CSV files
directory = '/Agg'  # Update this path

# Connect to SQLite database
conn = sqlite3.connect('trades_database.db')
cursor = conn.cursor()

# Create a table to store trade information with new fields
cursor.execute('''CREATE TABLE IF NOT EXISTS trades
                (ticker TEXT, trade_type TEXT, strike_price TEXT, expiration_month TEXT, time TEXT, 
                 open REAL, high REAL, close REAL, low REAL, volume INTEGER)''')

# Iterate over the files in the directory
for filename in os.listdir(directory):
    if filename.endswith('-ohlc-1h.csv'):  # Process files ending with -ohlc-1h.csv
        ticker, trade_type, strike_price, expiration_month = extract_info(filename)
        with open(os.path.join(directory, filename), 'r', encoding='utf-8') as file:
            csv_reader = csv.reader(file)
            next(csv_reader)  # Skip header row
            # Insert rows into the database
            for row in csv_reader:
                time, open_price, high, low, close, volume = row
                # Convert string values to appropriate types
                open_price = float(open_price)
                high = float(high)
                low = float(low)
                close = float(close)
                volume = int(volume)
                # Insert data into the database
                cursor.execute('''INSERT INTO trades (ticker, trade_type, strike_price, expiration_month, time, 
                                  open, high, close, low, volume)
                                  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                               (ticker, trade_type, strike_price, expiration_month, time, open_price, high, close, low, volume))

# Commit changes and close the database connection
conn.commit()
conn.close()

print("Data inserted into the database successfully.")
