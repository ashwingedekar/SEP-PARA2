import requests
import pandas as pd
from io import StringIO
import warnings
from datetime import datetime

warnings.filterwarnings("ignore", category=DeprecationWarning)

# Read parameters from file
with open("server_address.txt", "r") as file:
    server_parameters = dict(line.strip().split("=") for line in file)

# Read flags from the "max_min_flags.txt" file
flags = {}
id_prefix = 'id'
id_values = []

with open("max_min_flags.txt", "r") as file:
    for line in file:
        line = line.strip()
        if "=" in line:
            key, value = line.split("=")
            if key.startswith(id_prefix):
                id_values.append(value)
            else:
                flags[key] = value

# Create a string to store the formatted output
output_text = ""

# Construct API requests for each ID
for id_value in id_values:
    # Construct the API endpoint URL using the extracted parameters
    api_endpoint = f'https://{server_parameters.get("server")}/api/historicdata.csv?id={id_value}&avg={flags.get("avg")}&sdate={flags.get("sdate")}&edate={flags.get("edate")}&username={server_parameters.get("username")}&passhash={server_parameters.get("passhash")}'

    # Make the API request
    response = requests.get(api_endpoint)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        output_text += f"ID {id_value}:\n{'-' * len('ID ' + id_value + ':')}\n"

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
                    output_text += f"MAX SPEED{' ' * (25 - len('MAX SPEED'))}{max_raw_speed_row['Traffic Total (Speed)']}\n"
                    output_text += f"MAX SPEED(RAW){' ' * (22 - len('MAX SPEED(RAW)'))}{max_raw_speed_row['Traffic Total (Speed)(RAW)']}\n"
                    output_text += f"Date Time{' ' * (26 - len('Date Time'))}{max_raw_speed_row['Date Time']}\n\n"

                if flags.get("min") == '1':
                    # Find the row with the minimum "Traffic Total (Speed)(RAW)"
                    min_raw_speed_row = selected_data.loc[selected_data["Traffic Total (Speed)(RAW)"].idxmin()]
                    output_text += f"MIN SPEED{' ' * (25 - len('MIN SPEED'))}{min_raw_speed_row['Traffic Total (Speed)']}\n"
                    output_text += f"MIN SPEED(RAW){' ' * (22 - len('MIN SPEED(RAW)'))}{min_raw_speed_row['Traffic Total (Speed)(RAW)']}\n"
                    output_text += f"Date Time{' ' * (26 - len('Date Time'))}{min_raw_speed_row['Date Time']}\n\n"

            else:
                output_text += f"No non-NaN values found in 'Traffic Total (Speed)(RAW)' for ID {id_value}\n\n"

        except Exception as e:
            output_text += f"Error processing CSV data for ID {id_value}: {e}\n\n"
    else:
        output_text += f"Error: {response.status_code} - {response.text}\n\n"

    output_text += "#" * 55 + "\n\n"

# Get the current date and time
current_datetime = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

# Write the formatted output to a text file with the current date and time appended to the filename
output_file_name = f"output_{current_datetime}.txt"
with open(output_file_name, "w") as output_file:
    output_file.write(output_text)

# Print the output to the terminal
print(output_text)

print(f"Output has been saved to {output_file_name}")
