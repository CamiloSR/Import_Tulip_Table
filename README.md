```markdown
# Tulip Table Importer

This script provides a convenient function to import data from a Tulip table into a Pandas DataFrame. It allows for easy retrieval of data by specifying either the Table Name or the Table ID (preferably the latter). Additional features include the option to rename columns for consistency and the ability to filter data using a query string.

## Features

- Fetch data from a Tulip table via the Tulip API.
- Return the data as a Pandas DataFrame for easy manipulation and analysis.
- Options to rename columns based on table schema for better readability.
- Ability to filter data using a custom query string.
- Error handling for cases like invalid credentials or non-existent tables.

## Requirements

Before running the script, ensure that the following packages are installed:

```bash
pip install pandas requests
```

## Usage

Import the function and call it with the required parameters:

```python
from your_script_name import import_tulip_table

df = import_tulip_table(
    instance="<tulip_instance_url>",
    authorization="<bearer_token>",
    table_id="<table_id_or_name>",
    rename_columns=True,
    query="<optional_query_string>"
)
```

### Parameters

- `instance` (str): The instance of the Tulip API.
- `authorization` (str): Bearer Token for authentication.
- `table_name` (str, optional): The name of the table from which data is to be fetched.
- `table_id` (str, optional): The identifier of the table from which data is to be fetched.
- `rename_columns` (bool, optional): Whether to rename columns in the resulting DataFrame (default is True).
- `query` (str, optional): An optional query string to filter the data.

### Returns

- `pd.DataFrame`: A Pandas DataFrame containing the fetched data from the remote API.

### Raises

- `ValueError`: If neither `table_name` nor `table_id` are provided, or if both are provided.

## Notes

- The function prioritizes `table_id` over `table_name` for identifying the table.
- It performs pagination automatically to handle large datasets.
- It also converts specific timestamp columns to 'America/Montreal' timezone for consistency.

Ensure that you have the proper authorization and correct table identifiers when using this function.

This README provides an overview of the script, installation instructions, how to use it, and details about its functionality and parameters. You can adjust the content as needed to match your repository's structure or specific requirements.


## Usage Example

Below is a practical example of how to use the `import_tulip_table` function to fetch data from a specific Tulip table and load it into a Pandas DataFrame:


```python
from import_tulip import import_tulip_table

# Define your Tulip instance, API key, and other credentials
domain = "example.tulip.co"
api_key = "apikey.2_*************"
tulip_id = "xxxxxyyyy"
secret = "*-*******************-*****************+++"
token = f'{api_key}:{secret}'

# Optionally, define a query to filter or sort the data
table_query_example = '?sortOptions=[{"sortBy":"_createdAt","sortDir":"desc"}]'

# Call the import_tulip_table function with the necessary parameters
df = import_tulip_table(
    instance = domain, 
    authorization = token, 
    table_id = tulip_id, 
    query = table_query_example
)

# Now, 'df' holds your Tulip table data as a Pandas DataFrame.
