import os

import pandas as pd

# Define the input directory and output file
input_dir = r'C:\\Users\\Ronja\\Documents\\Studium\\Semester 4\\DABI2\\Projekt\\Umsatzdaten\\DESTATIS\\2301_2404\\'  # replace with your input directory
output_file = 'C:\\Users\\Ronja\\Documents\\Studium\\Semester 4\\DABI2\\Projekt\\Umsatzdaten\\DESTATIS\\Output\\2301_2404\\campSites.csv'  # replace with your output file path

# Initialize an empty DataFrame
combined_df = pd.DataFrame()

# Loop over all XLSX files in the input directory
for filename in os.listdir(input_dir):
    if filename.endswith('.xlsx'):
        # Read the sheet named "csv-45412-12" into a DataFrame
        df = pd.read_excel(os.path.join(input_dir, filename), sheet_name='csv-45412-12')

        # Append this DataFrame to the combined DataFrame
        combined_df = pd.concat([combined_df, df])

# Write the combined DataFrame to a CSV file
combined_df.to_csv(output_file, index=False)
