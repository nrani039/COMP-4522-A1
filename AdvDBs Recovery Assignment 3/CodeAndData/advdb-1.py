# Adv DB Winter 2024 - 1

import random
import csv

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
    for transaction in transactions:
        uID, attribute, newValue = transaction
        for db in data_base[1:]:
            if db[0] == uID:
                DB_Log.append({'Before': db.copy()})
                attribute_index = data_base[0].index(attribute)
                db[attribute_index] = newValue
                # Mark this transaction as committed immediately after successful update
                committed_entry = db.copy()
                DB_Log.append({'After': committed_entry, 'Committed': True})
                break

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
        writer.writerow(['Transaction', 'Status'])
        for i, entry in enumerate(log):
            status = 'Committed' if entry.get('Committed') else 'Rolled back'
            writer.writerow([f'Transaction {i+1}', status])

def main():
    global data_base
    number_of_transactions = len(transactions)
    must_recover = False
    data_base = read_file('Employees_DB_ADV.csv')
    failing_transaction_index = None
    
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
    else:
        writeCommittedTransactionToCSV(DB_Log, 'Committed_Transactions.csv')
        writeTransactionLogToCSV(DB_Log, 'Log.csv') 
        print("All transactions ended up well. Committed transactions are saved to 'Committed_Transactions.csv' file.")
        print("Updates to the database were committed!\n")
        print('The data entries AFTER updates -and RECOVERY, if necessary- are presented below:')
        
    for item in data_base:
        print(item)

main()
