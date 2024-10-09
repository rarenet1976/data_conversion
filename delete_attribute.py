# Script will be used to delete a attribute within a specific DynamoDB table
import boto3
from botocore.exceptions import ClientError
import logging
from datetime import datetime

# Set up logging
log_filename = f"dynamodb_delete_columns_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(filename=log_filename, level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize DynamoDB client
dynamodb = boto3.client('dynamodb')

# Set the table name
table_name = 'serverlessrepo-document-scanner-ttwo-PagesTable-HCBUZWW8X3VR'

# Columns to delete
columns_to_delete = ['text']

def scan_and_delete_columns():
    try:
        # Scan the table
        paginator = dynamodb.get_paginator('scan')
        page_iterator = paginator.paginate(TableName=table_name)

        for page in page_iterator:
            for item in page['Items']:
                project = item['project']
                page_num = item['page']
                
                # Check if any of the columns to delete exist in this item
                columns_present = [col for col in columns_to_delete if col in item]
                
                if columns_present:
                    try:
                        # Prepare the update expression and attribute names
                        update_expression = "REMOVE " + ", ".join([f"#{col}" for col in columns_present])
                        expression_attribute_names = {f"#{col}": col for col in columns_present}

                        # Remove the columns
                        response = dynamodb.update_item(
                            TableName=table_name,
                            Key={
                                'project': project,
                                'page': page_num
                            },
                            UpdateExpression=update_expression,
                            ExpressionAttributeNames=expression_attribute_names,
                            ReturnValues="ALL_OLD"
                        )
                        
                        logging.info(f"Deleted columns {columns_present} from item with project {project['S']} and page {page_num['N']}")
                        
                        # Log the old values of the deleted columns
                        old_item = response.get('Attributes', {})
                        for col in columns_present:
                            if col in old_item:
                                logging.info(f"Deleted column '{col}' had value: {old_item[col]}")
                    
                    except ClientError as e:
                        logging.error(f"Error deleting columns from item with project {project['S']} and page {page_num['N']}: {e.response['Error']['Message']}")

        logging.info("Finished processing all items in the table.")

    except ClientError as e:
        logging.error(f"Error scanning table: {e.response['Error']['Message']}")

try:
    scan_and_delete_columns()
    logging.info("Operation completed successfully.")
except Exception as e:
    logging.error(f"An unexpected error occurred during the operation: {str(e)}")

print(f"Operation complete. Check the log file '{log_filename}' for details.")
