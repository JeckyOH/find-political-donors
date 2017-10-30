#! /usr/bin/env python

"""
This module is the running module.
"""

import argparse
from datetime import datetime
import os
import sys
import transaction_handling

# The position of specified columns in the input file.
CMTE_ID_POSITION = 0
ZIPCODE_POSITION = 10
TRANSACTION_DT_POSITION = 13
TRANSACTION_AMT_POSITION = 14
OTHER_ID_POSITION = 15

OUTPUT_FILE_NAME_BY_DATE = 'medianvals_by_date.txt'
OUTPUT_FILE_NAME_BY_ZIPCODE = 'medianvals_by_zip.txt'

def get_args():
    """ Parse user auguments from command line. """

    usage_desc = """ Used for outputing information of political contributions and donors.  """
    parser = argparse.ArgumentParser(description=usage_desc)

    parser.add_argument('-i', '--input', type = str, action = 'store', dest = 'input_file_path', default = '../input/itcont.txt', help = "The path of input file.")
    parser.add_argument('-o', '--output', type = str, action = 'store', dest = 'output_dir', default = '../output/', help = "Directory to put output files.")

    args = parser.parse_args()
    return args

def is_valid_date(date_str):
    """ Check whether a string is in valid date format.  """

    try:
        datetime.strptime(date_str, '%m%d%Y')
        return True
    except:
        return False

def is_number(s):
    """ Check whether a string is a number. """
    try:
        float(s) # for int, long and float
    except ValueError:
        return False
    return True

def read_file(input_file_path, transac_by_date_thread, transac_by_zip_thread):
    """ Read input file and feed data items into the task queue of transaction handling threads.  """
    input_file = open(input_file_path, 'r')
    for line in input_file:
        line_items = line.split('|')

        # If the OTHER_ID column is NOT empty, ignore entire record.
        if line_items[OTHER_ID_POSITION]:
            continue

        # If the CMTE_ID or TRANSACTION_AMT column is empty or TRANSACTION_AMT column is not a valid number, ignore entire record.
        if not line_items[CMTE_ID_POSITION] or not line_items[TRANSACTION_AMT_POSITION] or not is_number(line_items[TRANSACTION_AMT_POSITION]):
            continue

        distilled_data = {"cmte_id" : line_items[CMTE_ID_POSITION],
                          "zipcode" : line_items[ZIPCODE_POSITION],
                          "transaction_dt" : line_items[TRANSACTION_DT_POSITION],
                          "transaction_amt" : float(line_items[TRANSACTION_AMT_POSITION])}

        # If the TRANSACTION_DT colomn is well-formed......
        if distilled_data["transaction_dt"] and is_valid_date(distilled_data["transaction_dt"]):
            transac_by_date_thread.add_task(distilled_data)
        # If the ZIPCODE column is well-formed......
        if len(distilled_data["zipcode"]) >= 5:
            distilled_data["zipcode"] = distilled_data["zipcode"][0:5]
            transac_by_zip_thread.add_task(distilled_data)
    end_data = {"cmte_id": '', "zipcode": '', "transaction_dt": '', "transaction_amt": 0}
    transac_by_date_thread.add_task(end_data)
    transac_by_zip_thread.add_task(end_data)
    input_file.close()

if __name__ == '__main__':
    try:
        usr_args = get_args()
        if os.path.isfile(usr_args.input_file_path) and os.path.isdir(usr_args.output_dir):
            # Create 2 transaction handling threads, one for dealing by data, the other for dealing by zipcode.
            thread_transac_by_date = transaction_handling.TransactionByDateThread(output_file_path = os.path.join(usr_args.output_dir, OUTPUT_FILE_NAME_BY_DATE))
            thread_transac_by_zip = transaction_handling.TransactionByZipThread(output_file_path = os.path.join(usr_args.output_dir, OUTPUT_FILE_NAME_BY_ZIPCODE))

            read_file(usr_args.input_file_path, thread_transac_by_date, thread_transac_by_zip)

            # Wait for working threads
            thread_transac_by_date.wait_complete()
            thread_transac_by_zip.wait_complete()
        else:
            print ("The input file or ouput directory does NOT exist.\n")
    except:
        import sys
        sys.exit(1)
