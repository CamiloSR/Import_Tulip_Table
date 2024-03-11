import requests
import pandas as pd
import base64
import urllib.parse

def import_tulip_table(instance: str = None, authorization: str = None, table_id: str = None, table_name: str = None, rename_columns: bool = True, query: str = "") -> pd.DataFrame:
    """
    Import data from a Tulip table into a Pandas DataFrame.
    You can specify either the Table Name or the Table ID; however, using the Table ID is preferred.

    Parameters:
    - instance (str): The instance of the Tulip API.
    - authorization (str): Bearer Token for authentication.
    - table_name (str): The name of the table from which data is to be fetched.
    - table_id (str): The identifier of the table from which data is to be fetched.
    - rename_columns (bool, optional): Whether to rename columns in the resulting DataFrame (default is True).
    - query (str, optional): An optional query string to filter the data.

    Returns:
    - pd.DataFrame: A Pandas DataFrame containing the fetched data from the remote API.
    
    Raises:
    - ValueError: If neither table_name nor table_id are provided, or if both are provided.
    """
    
    # Ensure only one identifier is used: either table_name or table_id
    if (table_name is None and table_id is None) or (table_name is not None and table_id is not None):
        raise ValueError("Exactly one of 'table_name' or 'table_id' must be provided.")

    # Encode the authorization token for API requests
    encoded_token = f"Basic {base64.b64encode(authorization.encode()).decode()}"
    
    # Define headers for HTTP request
    headers = {
        "accept": "*/*",
        "accept-language": "fr-CA,fr;q=0.9,fr-FR;q=0.8,en-CA;q=0.7,en;q=0.6,en-US;q=0.5,en-GB;q=0.4",
        "authorization": encoded_token,
    }
    
    # Construct URL for the tables endpoint
    tables_url = f"https://{instance}/api/v3/w/1/tables?includeDeleted=false&includeHidden=true"
    
    # Fetch table data from Tulip API
    response = requests.get(tables_url, headers=headers)

    # Check for successful response
    if response.status_code != 200:
        return "Invalid Credentials, please update and try again"
    
    # Parse JSON response to get table data
    tables_data = response.json()
    
    # Determine the identifier (id or label) to match table information
    if table_id is not None:
        table_match = table_id
        table_identifier = "id"
    else:
        table_identifier = "label"
        table_match = table_name
        
    # Find the specific table using the identifier
    table_info = next((record for record in tables_data if record.get(table_identifier) == table_match), None)
    
    # Handle case where table is not found
    if not table_info:
        return f"Table {table_match} not found, please check the table name or ID in Tulip and type it as is"
    
    # Extract table ID, name, and column information
    table_id = table_info.get("id")
    table_name = table_info.get("label")
    table_columns = table_info["columns"]

    # Construct URL to fetch records from table, incorporating any provided query
    url = f"https://{instance}/api/v3/w/1/tables/{table_id}/records" + query
    
    # Parse 'limit' parameter from query to manage pagination
    parsed_query = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
    max_limit = int(parsed_query.get("limit", [0])[0])
    
    # Calculate the maximum number of iterations for pagination
    max_iterations = max_limit / 1000 if max_limit != 0 else 99999999
    iteration = 0

    # Initialize parameters for paginated requests
    default_limit = max_limit if max_limit > 0 and max_limit < 1000 else 1000
    offset = 0
    all_records = []

    # Fetch records in a loop, handling pagination
    while True:
        params = {"limit": default_limit, "offset": offset}

        response = requests.get(url, headers=headers, params=params)

        if response.status_code == 200:
            iteration += 1
            # Parse table data from response
            table_data = response.json()
            all_records.extend(table_data)
            
            # Break loop if last page is reached or max iterations exceeded
            if len(table_data) < default_limit or iteration >= max_iterations:
                break
            else:
                offset += default_limit  # Increment offset to fetch next batch
        else:
            print("Error:", response.status_code)
            break

    # Convert list of records to a Pandas DataFrame
    df = pd.DataFrame(all_records)

    # Handle case where DataFrame is empty
    if df.empty:
        column_names = [column["name"] for column in table_columns]
        df = pd.DataFrame(columns=column_names)
        df['_createdAt'] = None
        df['_updatedAt'] = None
        df['_Sequencenumber'] = None

    # Process date columns, converting timezone to 'America/Montreal'
    for utc_col in ['_createdAt', '_updatedAt']:
        try:
            df[utc_col] = pd.to_datetime(df[utc_col])
            if df[utc_col].dt.tz is None:
                df[utc_col] = df[utc_col].dt.tz_localize('UTC').dt.tz_convert('America/Montreal')
            else:
                df[utc_col] = df[utc_col].dt.tz_convert('America/Montreal')
        except:
            print(f"Column {utc_col} does not Exist")

    # Rename columns if specified
    if rename_columns:
        column_mapping = {entry['name']: entry['label'] for entry in table_columns}
        df.rename(columns=column_mapping, errors='ignore', inplace=True)
        df.rename(columns={"_createdAt": "Created_At", "_updatedAt": "Updated_At"}, errors='ignore', inplace=True)

    # Standardize column names: capitalize and replace spaces with underscores
    df.columns = [col.upper() if col == "ID" else col.title() for col in df.columns]
    df.columns = df.columns.str.replace(" ", "_")

    return df
