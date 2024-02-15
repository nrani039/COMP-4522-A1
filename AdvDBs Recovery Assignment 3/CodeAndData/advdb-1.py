import random

import csv

data_base = []  # Global binding for the Database contents
transactions = [['1', 'Department', 'Music'], ['5', 'Civil_status', 'Divorced'],
                ['15', 'Salary', '200000']]
DB_Log = []  # <-- You WILL populate this as you go
original_data_base = []  # <-- Store original state of database for recovery


def recovery_script(log: list):
    '''
    Restore the database to stable and sound condition, by processing the DB log.
    '''
    global data_base
    global original_data_base
    for entry in log:
        if entry[0] == 'UPDATE':
            trans_id = entry[1]
            attribute = entry[2]
            value = entry[3]
            for item in data_base:
                if item[0] == trans_id:
                    item[data_base[0].index(attribute)] = value
        elif entry[0] == 'REVERT':
            trans_id = entry[1]
            for item in data_base:
                if item[0] == trans_id:
                    item_index = data_base.index(item)
                    data_base[item_index] = original_data_base[item_index]  # Revert the changes


def transaction_processing():
    '''
    1. Process transaction in the transaction queue.
    2. Updates DB_Log accordingly
    3. This function does NOT commit the updates, just execute them
    '''
    global DB_Log
    global data_base
    global original_data_base
    DB_Log.clear()  # Clear the log before processing transactions
    original_data_base = [row[:] for row in data_base]  # Create a copy of the original database
    successful_transactions = 0
    
    for transaction in transactions:
        trans_id, attribute, value = transaction
        updated = False
        for entry in data_base:
            if entry[0] == trans_id:
                original_value = entry[data_base[0].index(attribute)]
                entry[data_base[0].index(attribute)] = value  # Update the database
                DB_Log.append(['UPDATE', trans_id, attribute, original_value, value])  # Log the update
                updated = True
                break
        if not updated:  # If transaction id does not exist in the database
            DB_Log.append(['IGNORE', trans_id, attribute, value])  # Log as ignored
        
        if is_there_a_failure():
            if successful_transactions == 0:
                # If failure occurs during the first transaction, revert everything
                recovery_script(DB_Log)
                print("Failure in the first transaction. Reverting all changes.")
                return
            else:
                # If failure occurs during subsequent transactions, save the committed transactions
                save_partial_commit(data_base[:successful_transactions + 1])
                print(f"Failure after transaction {successful_transactions + 1}. Saving committed transactions.")
                return
        successful_transactions += 1

    # If no failure occurs, optionally save the final state
    save_partial_commit(data_base, 'final_commit.csv')
    print("All transactions processed successfully. Final state saved.")


def save_partial_commit(data, file_name='partial_commit.csv'):
    with open(file_name, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(data)


def read_file(file_name: str) -> list:
    '''
    Read the contents of a CSV file line-by-line and return a list of lists
    '''
    data = []
    with open(file_name, 'r') as reader:
        line = reader.readline()
        while line != '':
            line = line.strip().split(',')
            data.append(line)
            line = reader.readline()

    size = len(data)
    print('The data entries BEFORE updates are presented below:')
    for item in data:
        print(item)
    print(f"\nThere are {size} records in the database, including one header.\n")
    return data


def is_there_a_failure() -> bool:
    '''
    Simulates randomly a failure, returning True or False, accordingly
    '''
    value = random.randint(0, 1)
    if value == 1:
        result = True
    else:
        result = False
    return result


def main():
    global data_base
    number_of_transactions = len(transactions)
    must_recover = False
    data_base = read_file('Employees_DB_ADV.csv')
    failure = False  # Initialize failure to False
    failing_transaction_index = None

    # Call transaction_processing to process transactions
    transaction_processing()

    for index in range(number_of_transactions):
        print(f"\nProcessing transaction No. {index + 1}.")
        print("UPDATES have not been committed yet...\n")
        if is_there_a_failure():
            must_recover = True
            failing_transaction_index = index + 1
            print(f'There was a failure whilst processing transaction No. {failing_transaction_index}.')
            break
        else:
            print(f'Transaction No. {index + 1} has been committed! Changes are permanent.')

    if must_recover:
        # Call your recovery script
        recovery_script(DB_Log)  # Call the recovery function to restore DB to sound state
        DB_Log.clear()  # Clear the log after recovery
    else:
        # All transactions ended up well
        print("All transactions ended up well.")
        print("Updates to the database were committed!\n")

    print('The data entries AFTER updates -and RECOVERY, if necessary- are presented below:')
    for item in data_base:
        print(item)


main()
