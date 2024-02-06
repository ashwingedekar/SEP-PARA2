import requests
import pandas as pd
from io import StringIO

# Read parameters from file
with open("server_address.txt", "r") as file:
    server_parameters = dict(line.strip().split("=") for line in file)

# Read flags from the "max_min_flags.txt" file
flags = {}
flags = {}
with open("max_min_flags.txt", "r") as file:
    for line in file:
        line = line.strip()
        if "=" in line:
            key, value = line.split("=")
            flags[key] = value

# Extract id values
id_values = []
if "id" in flags:
    id_values = flags["id"].split(";")

# Construct API requests for each ID
for id_value in id_values:
    # Construct the API endpoint URL using the extracted parameters
    api_endpoint = f'https://{server_parameters.get("server")}/api/historicdata.csv?id={id_value}&avg={flags.get("avg")}&sdate={flags.get("sdate")}&edate={flags.get("edate")}&username={server_parameters.get("username")}&passhash={server_parameters.get("passhash")}'

    # Make the API request
    response = requests.get(api_endpoint)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        print(f"Request successful for ID: {id_value}")

        try:
            # Use pandas to read the CSV data
            df = pd.read_csv(StringIO(response.text))

            # Clean up the column names (remove leading and trailing spaces)
            df.columns = df.columns.str.strip()

            # Extract specified columns along with "Date Time"
            selected_columns = ["Date Time", "Traffic Total (Speed)", "Traffic Total (Speed)(RAW)"]
            selected_data = df[selected_columns]

            # Convert "Traffic Total (Speed)(RAW)" to numeric type
            selected_data.loc[:, "Traffic Total (Speed)(RAW)"] = pd.to_numeric(selected_data["Traffic Total (Speed)(RAW)"], errors='coerce')

            # Drop rows with NaN values in "Traffic Total (Speed)(RAW)"
            selected_data = selected_data.dropna(subset=["Traffic Total (Speed)(RAW)"])

            # Check if the DataFrame is not empty
            if not selected_data.empty:
                if flags.get("max") == '1':
                    # Find the row with the maximum "Traffic Total (Speed)(RAW)"
                    max_raw_speed_row = selected_data.loc[selected_data["Traffic Total (Speed)(RAW)"].idxmax()]
                    print(f"Row with the maximum Traffic Total (Speed)(RAW) for ID {id_value}:")
                    print(max_raw_speed_row)
                if flags.get("min") == '1':
                    # Find the row with the minimum "Traffic Total (Speed)(RAW)"
                    min_raw_speed_row = selected_data.loc[selected_data["Traffic Total (Speed)(RAW)"].idxmin()]
                    print(f"Row with the minimum Traffic Total (Speed)(RAW) for ID {id_value}:")
                    print(min_raw_speed_row)
            else:
                print(f"No non-NaN values found in 'Traffic Total (Speed)(RAW)' for ID {id_value}")

        except Exception as e:
            print(f"Error processing CSV data for ID {id_value}: {e}")

    else:
        print(f"Error: {response.status_code} - {response.text}")
