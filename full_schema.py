#This python script will retrieve the attribute types within a specific DynamoDB table
import boto3

dynamodb = boto3.resource('dynamodb', region_name='ca-central-1')
table_name = 'serverlessrepo-document-scanner-ttwo-PagesTable-HCBUZWW8X3VR'  # Replace with your actual table name

#Get items from the table:
def get_attribute_types():
    table = dynamodb.Table(table_name)
    response = table.scan()
    items = response.get('Items', [])

#Inspect the attribute types:
    for item in items:
        for key, value in item.items():
            print(f"Attribute: {key}, Type: {type(value)}")

if __name__ == '__main__':
    get_attribute_types()