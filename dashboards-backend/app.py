import requests
import json
from time import sleep
from flask import Flask, jsonify
from threading import Thread
from data_parser import get_ltv, get_top_five_pools, get_top_ten_pools, add_symbols_column
import pandas as pd
from flask_cors import CORS

# Define your GraphQL query and variables
query = '''
query Pools($chains: [String]!, $sortField: PoolSortFields, $sortDirection: SortDirections) {
  pools(chains: $chains, sortField: $sortField, sortDirection: $sortDirection) {
    chainId
    poolLiquidityUsd
    totalRewards {
      apr
    }
    assets {
      address
      symbol
    }
  }
}
'''

variables = {
    "chains": ["phoenix-1", "injective-1", "neutron-1"],
    "sortField": "TVL",
    "sortDirection": "DESC"
}

url = "https://develop-multichain-api.astroport.fi/graphql"

# Function to fetch data from the GraphQL API
def fetch_data():
    response = requests.post(url, json={"query": query, "variables": variables})
    data = response.json()
    return data

# Function to schedule fetching data every 24 hours (86400 seconds)
def schedule_fetch_data():
    while True:
        data = fetch_data()
        pools = data.get('data', {}).get('pools', [])

        # Convert the data to a pandas DataFrame
        df = pd.DataFrame(pools)

        # Flatten the nested columns (totalRewards and assets)
        df = df.join(pd.json_normalize(df["totalRewards"]).add_prefix("totalRewards."))
        df = df.join(pd.json_normalize(df["assets"]).add_prefix("assets."))
        df.drop(columns=["totalRewards", "assets"], inplace=True)

        # Add the new column
        df = add_symbols_column(df)

        # Save the DataFrame to a CSV file
        csv_path = "pools_data.csv"
        df.to_csv(csv_path, index=False)

        print("Data saved to pools_data.csv")

        # Parse the data
        parsed_data_ltv = get_ltv(csv_path)
        # Perform any further processing with the parsed_data if needed

        # Print the parsed data to the console
        print("Parsed data:")
        print(parsed_data_ltv)

        parsed_data_top_five_pools = get_top_five_pools(csv_path)
        print("Parsed data:")
        print(parsed_data_top_five_pools)

        parsed_data_top_ten_pools = get_top_ten_pools(csv_path)
        print("Parsed data:")
        print(parsed_data_top_ten_pools)

        sleep(86400)  # Wait for 24 hours before fetching and parsing data again

# Start fetching data in the background
fetch_data_thread = Thread(target=schedule_fetch_data)
fetch_data_thread.start()

# Your existing Flask code
app = Flask(__name__)
CORS(app)

@app.route('/api/data', methods=['GET'])
def get_data():
    # Call the get_ltv function to get the LTV data
    ltv_data = get_ltv('pools_data.csv')

    # Convert the LTV DataFrame to a JSON object
    ltv_json = ltv_data.to_json(orient='records')

    # Call the get_top_five_pools function to get the top 5 pools by liquidity
    top_five_pools = get_top_five_pools('pools_data.csv')

    # Convert the top 5 pools DataFrame to a JSON object
    top_five_json = top_five_pools.to_json(orient='records')

    # Call the get_top_ten_pools function to get the top 10 pools by liquidity
    top_ten_pools = get_top_ten_pools('pools_data.csv')

    # Convert the top 10 pools DataFrame to a JSON object
    top_ten_json = top_ten_pools.to_json(orient='records')

    # Return the JSON data
    return jsonify({
        "ltv_data": ltv_json,
        "top_five_pools": top_five_json,
        "top_ten_pools": top_ten_json
    })

if __name__ == '__main__':
    app.run(debug=True, port=5001)  # Change the port number here
