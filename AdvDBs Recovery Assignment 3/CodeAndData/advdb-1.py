# Adv DB Winter 2024 - 1 good version

import random
import csv
from datetime import datetime

data_base = []  # Global binding for the Database contents
'''
transactions = [['id1',' attribute2', 'value1'], ['id2',' attribute2', 'value2'],
                ['id3', 'attribute3', 'value3']]
'''
transactions = [['1', 'Department', 'Music'], ['5', 'Civil_status', 'Divorced'],
                ['15', 'Salary', '200000']]
DB_Log = [] # <-- You WILL populate this as you go


def recovery_script(log):
    global data_base
    for log_entry in reversed(log):
        if not log_entry.get('Committed'):
            before_state = log_entry['Before']
            for i, db_entry in enumerate(data_base):
                if db_entry[0] == before_state[0]:
                    data_base[i] = before_state
            break
    '''
    Restore the database to stable and sound condition, by processing the DB log.
    '''
    print("Calling your recovery script with DB_Log as an argument.")
    print("Recovery in process ...\n")
    pass

def process_transaction(index, transaction):
    global DB_Log
    uID, attribute, newValue = transaction
    for db_entry in data_base[1:]:
        if db_entry[0] == uID:
            original_entry = db_entry.copy()
            attribute_index = data_base[0].index(attribute)
            db_entry[attribute_index] = newValue
            
            if is_there_a_failure():
                print(f"Transaction {index} failed. \n")
                db_entry[attribute_index] = original_entry[attribute_index]  # Revert change
                DB_Log.append({'Transaction': index, 'Before': original_entry, 'After': db_entry.copy(), 'Committed': False})
                return False
            else:
                DB_Log.append({'Transaction': index, 'Before': original_entry, 'After': db_entry.copy(), 'Committed': True})
                return True

def transaction_processing():
    all_successful = True  # Assume all transactions will be successful
    for index, transaction in enumerate(transactions, start=1):
        print(f"\nProcessing transaction No. {index}. UPDATES have not been committed yet...")
        if not process_transaction(index, transaction):
            recovery_script(DB_Log)
            all_successful = False  # A failure occurred, indicating not all transactions were successful
            break  # Stop further processing
        else:
            print(f"Transaction {index} completed successfully.")
    return all_successful
   
def read_file(file_name:str)->list:
    '''
    Read the contents of a CSV file line-by-line and return a list of lists
    '''
    data = []
    #
    # one line at-a-time reading file
    #
    with open(file_name, 'r') as reader:
    # Read and print the entire file line by line
        line = reader.readline()
        while line != '':  # The EOF char is an empty string
            line = line.strip().split(',')
            data.append(line)
             # get the next line
            line = reader.readline()

    size = len(data)
    print('The data entries BEFORE updates are presented below:')
    for item in data:
        print(item)
    print(f"\nThere are {size} records in the database, including one header.\n")
    return data

def is_there_a_failure()->bool:
    '''
    Simulates randomly a failure, returning True or False, accordingly
    '''
    value = random.randint(0,1)
    if value == 1:
        result = True
    else:
        result = False
    return result

def newTable(log, file_name):
    committed_transactions = [entry['After'] for entry in log if entry.get('Committed')]

    with open(file_name, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(data_base[0])
        for item in data_base:
            writer.writerow(item)

def transactionLog(log, file_name):
    with open(file_name, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Transaction', 'Time', 'Status'])
        
        for entry in log:
            transaction = entry.get('Transaction', '')
            status = 'Committed' if entry.get('Committed') else 'Rolled back'
            time = entry.get('Time', '')
            writer.writerow([transaction, time, status])
                       

def main():
    global data_base
    data_base = read_file('Employees_DB_ADV.csv')
    
    print('The data entries BEFORE updates are presented below:')
    for item in data_base:
        print(item)

    # Process transactions and capture the outcome
    all_transactions_successful = transaction_processing()

    newTable(DB_Log, 'New_Employees_DB_ADV.csv')
    transactionLog(DB_Log, 'TransactionLog.csv')
    
    # Adjust final messaging based on transaction outcomes
    if all_transactions_successful:
        print("All transactions processed successfully. Updates have been committed to the database.")
    else:
        print("Not all transactions ended well. Please review the recovery actions taken.")

    print('The data entries AFTER updates -and RECOVERY, if necessary- are presented below:')
    for item in data_base:
        print(item)

        
main()
