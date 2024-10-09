#Script will be used to add the appropriate pre-tax amounts, updating the 'gst' and 'pst' columns based on the category rules while maintaining the original 'TOTAL' value.

import pandas as pd

# Load the CSV file
file_path = "Final_Expanded_Data_Latest_v2_with_pages.csv"
data = pd.read_csv(file_path)

# Define the tax rates
GST_RATE = 0.05  # 5%
PST_RATE = 0.08  # 8%
HST_RATE = 0.13  # 13%

# Define tax rules for each category
category_tax_rules = {
    'Entertainment': 'Full HST',
    'Other': 'Full HST',
    'Utilities': 'GST Only',
    'Groceries': 'GST Only',
    'Motor-Vehicle-Expenses': 'Full HST',
    'Restaurants': 'Full HST'
}

# Function to update tax fields based on the category
def update_tax_fields(row):
    category = row['VENDOR_NAME']
    total_with_tax = row['TOTAL']
    
    if category in category_tax_rules:
        rule = category_tax_rules[category]
        if rule == 'Full HST':
            total_before_tax = total_with_tax / (1 + HST_RATE)
            row['gst'] = total_before_tax * GST_RATE
            row['pst'] = total_before_tax * PST_RATE
        elif rule == 'GST Only':
            total_before_tax = total_with_tax / (1 + GST_RATE)
            row['gst'] = total_before_tax * GST_RATE
            row['pst'] = 0
    else:
        # Assume PST only for categories not listed
        total_before_tax = total_with_tax / (1 + PST_RATE)
        row['gst'] = 0
        row['pst'] = total_before_tax * PST_RATE
    
    return row

# Apply the tax rules to update the tax fields
data = data.apply(update_tax_fields, axis=1)

# Round the tax columns to two decimal places
data['gst'] = data['gst'].round(2)
data['pst'] = data['pst'].round(2)

# Save the updated CSV
updated_file_path = "Final_Expanded_Data_Latest_v2_with_pages_updated.csv"
data.to_csv(updated_file_path, index=False)

print(f"Updated file saved as {updated_file_path}")
