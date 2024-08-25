import csv

input_file = r'C:\path-to-your-file\Daily_Port_Activity_Data_and_Trade_Estimates.csv'  #Path to the input CSV file
output_file = r'C:\path-to-your-file\output.csv' #Path to the output CSV file

#Open input and output files
with open(input_file, 'r') as file_in, open(output_file, 'w', newline='') as file_out:
    reader = csv.DictReader(file_in)
    writer = csv.DictWriter(file_out, fieldnames=reader.fieldnames)
    writer.writeheader()

    #Iterate over rows and write the filtered rows to the output file
    for row in reader:
        if row['year'] == '2024': #e.g. 2024
            writer.writerow(row)

print('Filtered rows saved to', output_file)
