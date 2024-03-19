import os
import pandas as pd

# Function to aggregate CSV files in a folder
def aggregate_csv(folder_path):
    # List all files in the folder
    files = os.listdir(folder_path)
    
    # Filter out only CSV files
    csv_files = [f for f in files if f.endswith('.csv')]
    
    # Check if folder is empty or no CSV files are found
    if not csv_files:
        print(f"No CSV files found in {folder_path}. Skipping.")
        return
    
    # Define columns to keep
    columns_to_keep = ['time', 'last', 'volume']
    
    # Initialize an empty list to store dataframes
    dfs = []
    
    # Iterate over each CSV file
    for file in csv_files:
        file_path = os.path.join(folder_path, file)
        # Read the CSV file and check column names
        df = pd.read_csv(file_path, sep=';')
        print(f"Columns in {file}: {df.columns}")
        # Perform a case-insensitive check for required columns
        if all(col.lower() in df.columns.str.lower() for col in columns_to_keep):
            df = df[columns_to_keep]
            dfs.append(df)
        else:
            print(f"Skipping file {file}: Missing required columns.")
    
    # Check if there are any dataframes to concatenate
    if not dfs:
        print(f"No dataframes to concatenate in {folder_path}. Skipping.")
        return
    
    # Concatenate all dataframes into a single dataframe
    combined_df = pd.concat(dfs, ignore_index=True)
    
    # Sort the combined dataframe based on the 'time' column
    combined_df['time'] = pd.to_datetime(combined_df['time'])
    combined_df = combined_df.sort_values(by='time')
    
    # Save the aggregated data to a new CSV file in the root directory
    output_file = os.path.join(os.path.dirname(folder_path), os.path.basename(folder_path) + '_aggregated_data.csv')
    combined_df.to_csv(output_file, index=False)
    print(f"Aggregated data saved to {output_file}")

# Main function to iterate through folders
def main(root_dir):
    # Iterate through each folder in the root directory
    for folder in os.listdir(root_dir):
        folder_path = os.path.join(root_dir, folder)
        if os.path.isdir(folder_path) and not os.listdir(folder_path):
            print(f"Skipping empty directory: {folder_path}")
            continue
        if os.path.isdir(folder_path):
            aggregate_csv(folder_path)

# Replace 'root_directory_path' with the directory path containing your folders
root_directory_path = 'root_directory_path'
main(root_directory_path)
