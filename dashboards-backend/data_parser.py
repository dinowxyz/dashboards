import pandas as pd
import ast

def get_ltv(pools_data):
    # Read the CSV file into a DataFrame
    df = pd.read_csv(pools_data)

    # Group by 'chainId' and sum the 'poolLiquidityUsd' values
    grouped_df = df.groupby('chainId')['poolLiquidityUsd'].sum().reset_index()

    # Rename the columns
    grouped_df.columns = ['chainId', 'totalLiquidityUsd']

    # Optionally, return the parsed data
    return grouped_df

def get_top_five_pools(pools_data, n=5):
    # Read the CSV file into a pandas DataFrame
    df = pd.read_csv(pools_data)

    # Sort the data by poolLiquidityUsd in descending order
    sorted_df = df.sort_values('poolLiquidityUsd', ascending=False)

    # Select the top n pools by liquidity
    top_pools = sorted_df.head(n)

    # Filter the DataFrame to keep only the chainId, poolLiquidityUsd, and assets columns
    filtered_df = top_pools[['chainId', 'poolLiquidityUsd', 'assets.0', 'assets.1']]

    # Return the top pools DataFrame
    return filtered_df

def get_top_ten_pools(pools_data, n=10):
    # Read the CSV file into a pandas DataFrame
    df = pd.read_csv(pools_data)

    # Sort the data by poolLiquidityUsd in descending order
    sorted_df = df.sort_values('poolLiquidityUsd', ascending=False)

    # Select the top n pools by liquidity
    top_pools = sorted_df.head(n)

    # Filter the DataFrame to keep only the chainId, poolLiquidityUsd, and assets columns
    filtered_df = top_pools[['chainId', 'poolLiquidityUsd', 'totalRewards.apr']]

    # Return the top pools DataFrame
    return filtered_df

def add_symbols_column(df):
    # Use apply method to pull out 'symbol' from each assets column and join them
    df['asset_symbols'] = df['assets.0'].apply(lambda x: x['symbol']) + '-' + df['assets.1'].apply(lambda x: x['symbol'])
    return df




