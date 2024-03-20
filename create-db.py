import os
import sqlite3
import csv

# Function to extract information from file names
def extract_info(filename):
    ticker = filename[:4]
    trade_type = "Call" if ord(filename[4]) <= ord('L') else "Put"
    
    # Extract expiration month based on the 5th character
    expiration_month = ""
    month_mapping = {
        'A': 'January', 'B': 'February', 'C': 'March', 'D': 'April', 'E': 'May', 'F': 'June', 
        'G': 'July', 'H': 'August', 'I': 'September', 'J': 'October', 'K': 'November', 'L': 'December',
        'M': 'January', 'N': 'February', 'O': 'March', 'P': 'April', 'Q': 'May', 'R': 'June', 
        'S': 'July', 'T': 'August', 'U': 'September', 'V': 'October', 'W': 'November', 'X': 'December'
    }
    expiration_month = month_mapping.get(filename[4], "")
    
    # Extract strike price only including numbers before the letter 'W'
    strike_price = filename[6:].split('W')[0].split('.')[0]
    
    return ticker, trade_type, strike_price, expiration_month

# Directory containing the CSV files
directory = '/root_dir/'

# Connect to SQLite database
conn = sqlite3.connect('trades_database.db')
cursor = conn.cursor()

# Create a table to store trade information
cursor.execute('''CREATE TABLE IF NOT EXISTS trades
                (ticker TEXT, trade_type TEXT, strike_price TEXT, expiration_month TEXT, time TEXT, last_price REAL, volume INTEGER)''')

# Iterate over the files in the directory
for filename in os.listdir(directory):
    # Check if the file is a CSV file
    if filename.endswith('.csv'):
        ticker, trade_type, strike_price, expiration_month = extract_info(filename)
        with open(os.path.join(directory, filename), 'r', encoding='utf-8') as file:
            # Read CSV file
            csv_reader = csv.reader(file)
            next(csv_reader)  # Skip header if exists
            # Insert rows into the database
            for row in csv_reader:
                time, last_price, volume = row
                last_price = float(last_price)
                volume = int(volume)
                if last_price != 0 and volume != 0:  # Skip rows with volume or last price equal to 0
                    cursor.execute('''INSERT INTO trades (ticker, trade_type, strike_price, expiration_month, time, last_price, volume)
                                      VALUES (?, ?, ?, ?, ?, ?, ?)''', (ticker, trade_type, strike_price, expiration_month, time, last_price, volume))

# Commit changes and close connection
conn.commit()
conn.close()

print("Data inserted into the database successfully.")
