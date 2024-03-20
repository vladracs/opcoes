import pandas as pd

# Load the data from the original CSV file
input_file = 'file.csv'  # Replace with your input file path
output_file = 'ohlc_output.csv'  # Output file path

# Read the CSV file into a DataFrame with UTF-8 encoding
df = pd.read_csv(input_file, encoding='utf-8')

# Ensure the 'time' column is recognized as datetime type by pandas
df['time'] = pd.to_datetime(df['time'])

# Set the 'time' column as the index of the DataFrame
df.set_index('time', inplace=True)

# Resample the data into 1-hour bins and calculate OHLC for the 'last' column
ohlc = df['last'].resample('1H').ohlc()

# You can also calculate the sum of 'volume' for each 1-hour interval if needed
ohlc['volume'] = df['volume'].resample('1H').sum()

# Filter out intervals where 'volume' is 0
ohlc = ohlc[ohlc['volume'] != 0]

# Save the OHLC data to a new CSV file with UTF-8 encoding
ohlc.to_csv(output_file, encoding='utf-8')

print(f'OHLC data has been saved to {output_file}')
