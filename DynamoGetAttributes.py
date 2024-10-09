#Get Table Unique Attributes

import boto3
from boto3.dynamodb.conditions import Key

# Initialize DynamoDB resource
dynamodb = boto3.resource('dynamodb')

# Specify the table name
table_name = 'serverlessrepo-document-scanner-ttwo-PagesTable-HCBUZWW8X3VR'
table = dynamodb.Table(table_name)

def list_all_attribute_names():
    all_attributes = set()
    
    # Scan the table
    response = table.scan()
    
    while True:
        items = response.get('Items', [])
        
        # Add all attribute names from each item to the set
        for item in items:
            all_attributes.update(item.keys())
        
        # Check if there are more items to scan
        if 'LastEvaluatedKey' not in response:
            break
        
        # Continue scanning from where we left off
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
    
    # Convert set to sorted list for better readability
    return sorted(list(all_attributes))

# Run the function and print the results
if __name__ == "__main__":
    attribute_names = list_all_attribute_names()
    print("All attribute names in the table:")
    for name in attribute_names:
        print(f"- {name}")
