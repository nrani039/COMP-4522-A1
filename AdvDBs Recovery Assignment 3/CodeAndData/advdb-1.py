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


'''
Responsible for restoring the database to a stable and sound condition after a failure.
It processes the database log in reverse order, reverting changes made by transactions up to the point of failure.
'''
def recovery_script(log):
    global data_base
    for log_entry in reversed(log):
        if not log_entry.get('Committed'):
            before_state = log_entry['Before']
            for i, db_entry in enumerate(data_base):
                if db_entry[0] == before_state[0]:
                    data_base[i] = before_state
            break

    print("Calling your recovery script with DB_Log as an argument.")
    print("Recovery in process ...\n")
    pass


'''
Processes a transaction by updating an attribute for a record identified by its unique ID. 
On a simulated failure, it reverts changes and logs the transaction as unsuccessful; otherwise, it logs it as successful.
'''
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


'''
Sequentially processes transactions from the global list, updating the database. On failure, invokes recovery_script to revert changes and halts further processing, while providing transaction status feedback
''' 
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


'''
Reads a CSV file into a list of lists, each representing a row from the file, and prints the data before returning it.
''' 
def read_file(file_name:str)->list:
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


'''
Simulates randomly a failure, returning True or False, accordingly
'''
def is_there_a_failure()->bool:
    value = random.randint(0,1)
    if value == 1:
        result = True
    else:
        result = False
    return result


'''
Writes the database state to a CSV file, including all entries regardless of transaction status.
'''
def newTable(log, file_name):
    committed_transactions = [entry['After'] for entry in log if entry.get('Committed')]

    with open(file_name, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(data_base[0])
        for item in data_base:
            writer.writerow(item)


'''
Creates a CSV log file detailing each transaction's index, timestamp, and status (Committed/Rolled back).
'''
def transactionLog(log, file_name):
    with open(file_name, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Transaction', 'Time', 'Status'])
        
        for entry in log:
            transaction = entry.get('Transaction', '')
            status = 'Committed' if entry.get('Committed') else 'Rolled back'
            time = entry.get('Time', '')
            writer.writerow([transaction, time, status])


'''
The main entry point of the program. It loads the initial database state, processes transactions, and updates the database. 
It then logs the transactions and the final database state to CSV files. The function also provides feedback on the 
transaction outcomes and the state of the database before and after updates.
'''
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
    
    if all_transactions_successful:
        print("All transactions processed successfully. Updates have been committed to the database.")
    else:
        print("Not all transactions ended well. Please review the recovery actions taken.")

    print('The data entries AFTER updates -and RECOVERY, if necessary- are presented below:')
    for item in data_base:
        print(item)

        
main()
