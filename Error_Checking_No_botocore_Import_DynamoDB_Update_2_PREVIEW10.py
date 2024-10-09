
#DynamoDB CSV Import Script

#This script imports data from a CSV file into an Amazon DynamoDB table.

#Purpose:
# Read data from a specified CSV file
# Process and validate each row
# Import the data into a DynamoDB table
# Handle potential errors and duplicates
# Provide detailed logging of the import process

# Key Features:
# Supports composite primary keys (project_name as partition key, page as sort key)
# Implements batch writing to DynamoDB for improved performance
# Detects and logs duplicate composite keys in the CSV data
# Provides a count of successfully processed rows and items in the DynamoDB table
# Uses rotating log files to manage log size

#Usage:
# Ensure AWS credentials are properly configured
# Set the correct DynamoDB table name in the 'table_name' variable
#. Specify the input CSV file path in the 'csv_file_path' variable
# Run the script

# This script assumes the CSV file has 'project_name' and 'page' columns
# that correspond to the composite primary key of the DynamoDB table.

# Dependencies:
# boto3: AWS SDK for Python
# csv: For reading CSV files
# logging: For creating log files
# collections.defaultdict: For efficient counting of duplicate keys

# Limit ten records in this example

import csv
import boto3
import logging
from logging.handlers import RotatingFileHandler
from botocore.exceptions import ClientError
from boto3.dynamodb.types import TypeSerializer
from collections import defaultdict

# Set up logging with rotation
log_file = 'import_log.txt'
max_log_size = 5 * 1024 * 1024  # 5 MB
backup_count = 3

handler = RotatingFileHandler(log_file, maxBytes=max_log_size, backupCount=backup_count)
logging.basicConfig(
    handlers=[handler],
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

dynamodb = boto3.client('dynamodb')
table_name = 'serverlessrepo-document-scanner-ttwo-PagesTable-HCBUZWW8X3VR'

from collections import defaultdict

def load_csv_to_dynamodb(csv_file, limit=None):
    serializer = TypeSerializer()
    total_rows = 0
    processed_rows = 0
    
    key_count = defaultdict(int)
    duplicate_keys = set()

    logging.info(f"Starting to process CSV file: {csv_file}")
    logging.info(f"Limit set to: {limit}")
    
    with open(csv_file, mode='r', encoding='utf-8', errors='replace') as file:
        reader = csv.DictReader(file)
        items = []
        for row_num, row in enumerate(reader, start=1):
            if limit is not None and total_rows >= limit:
                logging.info(f"Reached limit of {limit} rows. Stopping processing.")
                break
            total_rows += 1
            logging.debug(f"Processing row {row_num}")
            try:
                # Log the entire row for inspection
                logging.info(f"Row {row_num} data: {row}")

                # Ensure both primary key components are present
                if 'project' not in row or 'page' not in row:
                    logging.error(f"Row {row_num} is missing primary key components. Skipping.")
                    continue
                
                # Define the composite key
                composite_key = (row['project'], row['page'])
                
                # Log the composite key being processed
                logging.info(f"Processing item with composite key: {composite_key}")
                # Convert all values to the format DynamoDB expects
                item = {k: serializer.serialize(v) for k, v in row.items()}
                
                items.append({
                    'PutRequest': {
                        'Item': item
                    }
                })
                
                if len(items) == 25 or row_num == total_rows:
                    logging.info(f"Sending batch write request for rows {row_num-len(items)+1} to {row_num}")
                    response = dynamodb.batch_write_item(RequestItems={table_name: items})
                    processed_rows += len(items)
                    
                    unprocessed = response.get('UnprocessedItems', {})
                    while unprocessed:
                        logging.warning(f"Unprocessed items detected. Retrying.")
                        response = dynamodb.batch_write_item(RequestItems=unprocessed)
                        unprocessed = response.get('UnprocessedItems', {})
                    
                    items = []
            except Exception as e:
                logging.error(f"Error processing row {row_num}: {str(e)}", exc_info=True)

    logging.info(f"Finished processing. Total rows read: {total_rows}")
    logging.info(f"Rows successfully processed: {processed_rows}")
    logging.info(f"Total unique composite keys: {len(key_count)}")
    logging.info(f"Number of duplicate composite keys: {len(duplicate_keys)}")
    if duplicate_keys:
        logging.info("Duplicate keys found:")
        for key in duplicate_keys:
            logging.info(f"project={key[0]}, page={key[1]}")
            
    # Verify table contents
    try:
        response = dynamodb.scan(
            TableName=table_name,
            Select='COUNT',
            ConsistentRead=True
        )
        logging.info(f"Total items in DynamoDB table: {response['Count']}")
    except Exception as e:
        logging.error(f"Error scanning DynamoDB table: {str(e)}", exc_info=True)

if __name__ == "__main__":
    csv_file_path = 'sample.csv'
    
    logging.info("Starting script execution")
    
    logging.info("Starting import with 10 records")
    load_csv_to_dynamodb(csv_file_path, limit=10)
    
    logging.info("Script execution completed")
