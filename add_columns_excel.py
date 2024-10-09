#Script to add columns in EXCEL CSV file
import csv
import random

# Define the payment types and their probabilities
payment_types = {
    "Mastercard": 0.20,
    "Debit": 0.50,
    "Cash": 0.25,
    "Cheque": 0.05
}

# Function to choose a payment type based on the given probabilities
def choose_payment_type(payment_types):
    return random.choices(list(payment_types.keys()), weights=list(payment_types.values()))[0]

# Input and output file names
input_file = 'Final_Expanded_Data_Latest_v2_with_pages.csv'
output_file = 'Final_Expanded_Data_Latest_v2_with_pages_and_payment.csv'

# Read the input CSV and write to the output CSV with the new field
with open(input_file, 'r', newline='') as infile, open(output_file, 'w', newline='') as outfile:
    reader = csv.DictReader(infile)
    fieldnames = reader.fieldnames + ['PAYMENT_TYPE']
    
    writer = csv.DictWriter(outfile, fieldnames=fieldnames)
    writer.writeheader()
    
    for row in reader:
        row['PAYMENT_TYPE'] = choose_payment_type(payment_types)
        writer.writerow(row)

print(f"Process completed. New file created: {output_file}")
