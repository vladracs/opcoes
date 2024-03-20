import pandas as pd
import os

# Specify the directory containing your CSV files
input_folder_path = '/Agg/test/'  # Replace with your input folder path
output_folder_path = '/Agg'  # Replace with your output folder path



# Ensure the output directory exists
os.makedirs(output_folder_path, exist_ok=True)

# Iterate over all files in the input folder
for filename in os.listdir(input_folder_path):
    if filename.endswith('.csv'):  # Check if the file is a CSV
        input_file_path = os.path.join(input_folder_path, filename)
        output_file_name = filename.replace('.csv', '-ohlc-1h.csv')
        output_file_path = os.path.join(output_folder_path, output_file_name)

        # Read the CSV file into a DataFrame with UTF-8 encoding
        df = pd.read_csv(input_file_path, encoding='utf-8')

        try:
            # Ensure the 'time' column is recognized as datetime type by pandas
            df['time'] = pd.to_datetime(df['time'])

            # Convert 'last' and 'volume' to numeric, coercing errors to NaN
            df['last'] = pd.to_numeric(df['last'], errors='coerce')
            df['volume'] = pd.to_numeric(df['volume'], errors='coerce')

            # Set the 'time' column as the index of the DataFrame
            df.set_index('time', inplace=True)

            # Resample the data into 1-hour bins
            resampled = df.resample('1H')

            # Calculate OHLC for the 'last' column
            ohlc = resampled['last'].ohlc()

            # Calculate the sum of 'volume' for each 1-hour interval
            volume = resampled['volume'].sum()

            # Combine OHLC and volume data
            ohlc['volume'] = volume

            # Filter out intervals where 'volume' is 0
            ohlc_filtered = ohlc[ohlc['volume'] != 0]

            if not ohlc_filtered.empty:
                # Save the OHLC data to a new CSV file with UTF-8 encoding
                ohlc_filtered.to_csv(output_file_path, encoding='utf-8')
                print(f'Processed {filename} and saved OHLC data to {output_file_name}')
            else:
                print(f'No trading activity in {filename}, no file created.')
        except Exception as e:
            print(f"Error processing {filename}: {e}")
