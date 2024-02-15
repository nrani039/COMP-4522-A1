import random
import csv
from datetime import datetime
import os

data_base = []  # Global binding for the Database contents
transactions = [['1', 'Department', 'Music'], ['5', 'Civil_status', 'Divorced'],
                ['15', 'Salary', '200000']]
DB_Log = []

def recovery_script(log):
    global data_base
    for log_entry in reversed(log):
        if 'Before' in log_entry and log_entry['Status'] != 'Committed':
            entry = log_entry['Before']
            for db_entry in data_base[1:]:
                if db_entry[0] == entry[0]:
                    data_base[data_base.index(db_entry)] = entry
                    break

def read_file(file_name:str)->list:
    '''
    Read the contents of a CSV file line-by-line and return a list of lists
    '''
    data = []
    with open(file_name, 'r') as reader:
        csv_reader = csv.reader(reader)
        for line in csv_reader:
            data.append(line)
    size = len(data)
    print('The data entries BEFORE updates are presented below:')
    for item in data:
        print(item)
    print(f"\nThere are {size} records in the database, including one header.\n")
    return data

def generate_log_filename():
    current_time = datetime.now()
    timestamp = current_time.strftime("%Y%m%d%H%M%S%f")
    log_filename = f'Transaction_Log_new_{timestamp}.csv'
    return log_filename

def transaction_processing():
    global DB_Log
    global data_base
    for db_record in data_base[1:]:  # Skip the header row
        uID = db_record[0]
        # Log the transaction before processing
        DB_Log.append({'Transaction': uID, 'Time': datetime.now(), 'Before': db_record.copy(), 'Status': 'Processing'})

        # Process the transaction
        for transaction in transactions:
            if transaction[0] == uID:
                attribute = transaction[1]
                newValue = transaction[2]
                attribute_index = data_base[0].index(attribute)
                old_value = db_record[attribute_index]
                db_record[attribute_index] = newValue

                if is_there_a_failure():
                    db_record[attribute_index] = old_value  # Rollback to old value
                    DB_Log[-1]['Status'] = 'Rolled Back'
                else:
                    DB_Log[-1]['Status'] = 'Committed'
                break  # Proceed to the next database record

        # Log transactions not attempted due to previous failures
        if DB_Log[-1]['Status'] == 'Processing':
            DB_Log[-1]['Status'] = 'Not Executed'

def is_there_a_failure() -> bool:
    '''
    Simulates randomly a failure, returning True or False, accordingly
    '''
    failure_probability = 0.2  # 40% probability of failure
    if random.random() < failure_probability:
        return True
    else:
        return False

def main():
    global data_base
    global DB_Log
    data_base = read_file('Employees_DB_ADV.csv')
    print("Database Table Status Before Transactions:")
    for item in data_base:
        print(item)

    transaction_processing()

    print("\nDatabase Table Status After Transactions and Recovery:")
    for item in data_base:
        print(item)

    print("\nTransaction Log:")
    rollback_occurred = False  # Flag to track if a rollback occurred
    for log in DB_Log:
        print(log)
        if log['Status'] == 'Rolled Back':
            rollback_occurred = True
            break  # Stop printing when rollback occurs

    # Save transaction log to a different CSV file
    log_filename = generate_log_filename()
    with open(log_filename, 'w', newline='') as csvfile:
        fieldnames = ['Transaction', 'Time', 'Before', 'Status']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for log_entry in DB_Log:
            writer.writerow(log_entry)
            if rollback_occurred and log_entry['Status'] == 'Rolled Back':
                break  # Stop writing to CSV when rollback occurs

    print(f"Transaction log saved to {log_filename}")





main()
