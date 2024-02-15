# Adv DB Winter 2024 - 1

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


def recovery_script(log:list):  #<--- Your CODE
    global data_base
    for log_entry in reversed(log):
        if 'Before' in log_entry:
            entry = log_entry['Before']
            for db_entry in data_base[1:]:
                if db_entry[0] == entry[0]:
                    data_base[data_base.index(db_entry)] = entry
                    break
    '''
    Restore the database to stable and sound condition, by processing the DB log.
    '''
    print("Calling your recovery script with DB_Log as an argument.")
    print("Recovery in process ...\n")
    pass

# def transaction_processing(): #<-- Your CODE
#     global DB_Log
#     global data_base 
#     header = data_base[0]
#     for transaction in transactions:
#         uID, attribute, newValue = transaction
#         for db in data_base[1:]: 
#             if db[0] == uID:  
#                 DB_Log.append({'Before': db.copy()})  
#                 attribute_index = header.index(attribute)
#                 # attribute_index = data_base[0].index(attribute)  
#                 db[attribute_index] = newValue  
#                 DB_Log.append({'After': db.copy()})  
#                 break
#     '''
#     1. Process transaction in the transaction queue.
#     2. Updates DB_Log accordingly
#     3. This function does NOT commit the updates, just execute them
#     '''

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

def generate_log_filename():
    current_time = datetime.now()
    timestamp = current_time.strftime("%Y%m%d%H%M%S%f")
    log_filename = f'Transaction_Log_new_{timestamp}.csv'
    return log_filename

def is_there_a_failure()->bool:
    '''
    Simulates randomly a failure, returning True or False, accordingly
    '''
    failure_probability = 0.2  # 40% probability of failure
    if random.random() < failure_probability:
        return True
    else:
        return False

#Write a new CSV file when all of the transaction has been successfully commited and updated. 
def writeCommittedTransactionToCSV(log, file_name):
    committed_transactions = [entry['After'] for entry in log if entry.get('Committed')]

    with open(file_name, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(data_base[0])  
        for transaction in committed_transactions:
            writer.writerow(transaction)


def writeTransactionLogToCSV(log, file_name):
    with open(file_name, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Transaction', 'Time', 'Status'])
        for entry in log:
            status = 'Committed' if entry.get('Committed') else 'Rolled back'
            transaction = entry.get('Transaction', '')
            time = entry.get('Time', '')
            writer.writerow([transaction, time, status])

def main():
    global data_base
    global DB_Log
    number_of_transactions = len(transactions)
    must_recover = False
    data_base = read_file('Employees_DB_ADV.csv')
    print("Database Table Status Before Transactions:")
    for item in data_base:
        print(item)
    
    while True: 
       
        failure = is_there_a_failure() 
        if failure:
            must_recover = True
            print(f'There was a failure whilst processing transaction No. {failing_transaction_index}.')
            break  
        else:
            for index in range(number_of_transactions):
                print(f"\nProcessing transaction No. {index+1}.")
                print("UPDATES have not been committed yet...\n")
                transaction_processing()  
                print(f'Transaction No. {index+1} has been committed! Changes are permanent.')
            break  
    if must_recover:
        recovery_script(DB_Log)
        writeTransactionLogToCSV(DB_Log, 'FailLog.csv') 

    else:
        writeCommittedTransactionToCSV(DB_Log, 'Committed_Transactions.csv')
        writeTransactionLogToCSV(DB_Log, 'CommitLog.csv') 
        print("All transactions ended up well. Committed transactions are saved to 'Committed_Transactions.csv' file.")
        print("Updates to the database were committed!\n")
        print('The data entries AFTER updates -and RECOVERY, if necessary- are presented below:')
        
    for item in data_base:
        print(item)

main()
