import os
import re

import pandas as pd
import xlrd

# Define the directory containing the Excel files
input_directory = 'C:\\Users\\Ronja\\Documents\\Studium\\Semester 4\\DABI2\\Projekt\\Umsatzdaten\\DESTATIS\\Monatsbericht_Tourismus\\'

# Create the output parent directory if it doesn't exist
output_parent_directory = 'C:\\Users\\Ronja\\Documents\\Studium\\Semester 4\\DABI2\\Projekt\\Umsatzdaten\\DESTATIS\\Output\\'
os.makedirs(output_parent_directory, exist_ok=True)

# Define a dictionary to specify row ranges and columns for each sheet
sheet_specs = {
    '1.8': {'row_range': (15, 67), 'columns': ['A', 'B', 'C', 'D', 'E', 'F']},
    '1.9': {'row_range': (14, 79), 'columns': ['A', 'B', 'C', 'D', 'E', 'F']},
    '2.4': {'row_range': (4, 33), 'columns': ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H']},
    # Add more sheets and specifications as needed
}

# Function to extract specific rows and columns from a given sheet
def extract_data(sheet, row_range, columns):
    data = []
    if isinstance(sheet, pd.DataFrame):
        max_row = sheet.shape[0]
    else:
        max_row = sheet.nrows
    start_row, end_row = row_range
    end_row = min(end_row, max_row)  # Ensure the end row does not exceed the max rows in the sheet

    for row in range(start_row, end_row + 1):  # Ensure we include the end row
        row_data = {}
        for col in columns:
            try:
                if isinstance(sheet, pd.DataFrame):
                    cell_value = sheet.iloc[row-1, ord(col) - ord('A')]  # Adjust for zero-indexing
                else:
                    cell_value = sheet.cell_value(row-1, ord(col) - ord('A'))  # Adjust for zero-indexing
                row_data[col] = cell_value
            except Exception as e:
                print(f"Error extracting cell at row {row}, column {col}: {e}")
        if row_data:
            data.append(row_data)

    # Create a DataFrame from the extracted data
    df = pd.DataFrame(data, columns=columns)
    return df

# Function to extract the four-digit sequence from the filename
def extract_sequence(filename):
    match = re.search(r'_(\d{4})', filename)
    return match.group(1) if match else None

# Iterate over each file in the directory
for filename in os.listdir(input_directory):
    if filename.endswith(".xlsx") or filename.endswith(".xls"):
        file_path = os.path.join(input_directory, filename)

        # Extract the four-digit sequence from the filename
        sequence = str(extract_sequence(filename))

        # Load the Excel file
        if filename.endswith(".xlsx"):
            xl = pd.ExcelFile(file_path)
            xls = False
        elif filename.endswith(".xls"):
            xl = xlrd.open_workbook(file_path)
            xls = True

        # Get sheet names
        if xls:
            sheet_names = xl.sheet_names()
        else:
            sheet_names = xl.sheet_names

        # Iterate over each sheet in the specifications
        for sheet_name, specs in sheet_specs.items():
            if (xls and sheet_name in sheet_names) or (not xls and sheet_name in sheet_names):
                if xls:
                    sheet = xl.sheet_by_name(sheet_name)
                else:
                    sheet = xl.parse(sheet_name)
                df = extract_data(sheet, specs['row_range'], specs['columns'])

                # Add the sequence to a new column in the DataFrame
                df['Sequence'] = "20" + sequence

                # Create output directory for this sheet if it doesn't exist
                output_directory = os.path.join(output_parent_directory, sheet_name)
                os.makedirs(output_directory, exist_ok=True)

                # Add metadata columns to the DataFrame
                #df['filename'] = filename
                #df['sheet_name'] = sheet_name

                # Define the output CSV filename and path
                output_filename = f"{filename.replace('.xlsx', '').replace('.xls', '')}.csv"
                output_path = os.path.join(output_directory, output_filename)

                # Save the extracted data to a CSV file
                df.to_csv(output_path, index=False)
