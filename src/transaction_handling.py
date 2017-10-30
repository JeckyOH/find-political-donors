""" This module contains logics dealing with transactions by date.

In more detail, this module will maintain a thread, and use this thread to handle the data inserted
into the task queue.
Besides, required data structures are maintained as member variables.

The Algorithm to Get Running Median:
  1) Seperate the input data stream into 2 parts, each of which is stored into one heap.
     One part contains all data items which are lower than any of the another part.
     The important point is that Keep these 2 heaps balancing and the balance factor canNOT be over 1.
     For the lower part, use Max Heap so that the max data item can be easily got.
     For the higher part, use Min Heap to quickly get the lowest item in this part.
  2) When I want to get running median, what I just need to do is to
       Check whether these 2 heaps are in the equal size:
           If Yes, return  (max value in the lower part + min value in the higher part) / 2.0
           If No, return (root value in the heap which has more value.)
"""

import heap
import sys
from threading import Thread

if sys.version_info[0] == 2:
    from Queue import Queue
else:
    from queue import Queue

class TransactionThread(Thread):
    """ This class derives from threading.Thread class and is responsible for dealing with input data by transaction date.

    The thread will store input data and calculate median at the end. Finaly, it will write the output file 'medianvals_by_date.txt'.

    Attributes:
      task_queue: The queue of input data. Main thread feeds data into this queue and this thread consumes these data.
      total_amt_dict: The total amount of transactions which is identified by 'CMTE_ID|TRANSACTION_DT'
      transactions_dict: The list of transaction amounts which is identified by 'CMTE_ID|TRANSACTION_DT'.
                         This list should be sorted so that I can extract the median at the end.
    """
    def __init__(self, output_file_path):
        """ Constructor of transaction handler thread.
        Args:
          output_file_path: The path of output file. According to Challenge Instructions, it should be 'project_path/output/medianvals_by_[data|zip].txt'.
        """
        Thread.__init__(self)
        self.task_queue = Queue()
        self.total_amt_dict = {}
        self.transaction_amt_heaps = {}
        self.output_file = open(output_file_path, 'w')
        self.daemon = True
        self.start()

    def add_task(self, item):
        self.task_queue.put(item)

    def wait_complete(self):
        self.task_queue.join()

    def transaction_heaps_add_number(self, identifier, number):
        """ Add a number to one of "Balanced Heaps"(self.transaction_amt_heaps) which is identified by the identifier.

        Due to the transaction_amt_heaps is constructed in format '{"identifier": (lower_heap, higher_heap)}',
        the logic to add a number is:
          1) If lower_heap is empty or number < lower_heap.peek(), add number to the lower_heap;
          2) Otherwise, add number to the higher_heap.

        Args:
          identifier: Identify the transaction_amt_heaps to which the number is added. The format should be 'CMTE_ID|TRANSACTION_DT' or 'CMTE_ID|ZIPCODE'.
          number: (float) The number to be added.
        """
        if identifier not in self.transaction_amt_heaps:
            self.transaction_amt_heaps[identifier] = (heap.MaxHeap(), heap.MinHeap())
        # If lower_heap is empty or the number to be added is less than lower_heap.peek(), add the number to lower_heap.
        if self.transaction_amt_heaps[identifier][0].empty() or number < self.transaction_amt_heaps[identifier][0].peek():
            self.transaction_amt_heaps[identifier][0].push(number)
        # Otherwise, add the number to higher_heap.
        else:
            self.transaction_amt_heaps[identifier][1].push(number)

    def transaction_heaps_rebalance(self, identifier):
        """ Rebalance the transaction_amt_heaps which is identified by identifier.
        The goal is to make sure the difference of size of lower_heap and higher_heap is up to 1.
        Args:
          identifier: Identify whose lower_heap and higher_heap need to be rebalanced. The format should be 'CMTE_ID|TRANSACTION_DT' or 'CMTE_ID|ZIPCODE'.
        """
        try:
            # First, find the heap whose size is larger, and whose is smaller.
            if self.transaction_amt_heaps[identifier][0].size() > self.transaction_amt_heaps[identifier][1].size():
                bigger_heap = self.transaction_amt_heaps[identifier][0]
                smaller_heap = self.transaction_amt_heaps[identifier][1]
            else:
                bigger_heap = self.transaction_amt_heaps[identifier][1]
                smaller_heap = self.transaction_amt_heaps[identifier][0]
            # only when the difference of size is larger than 1, rebalancing is needed.
            if bigger_heap.size() - smaller_heap.size() >= 2:
                smaller_heap.push(bigger_heap.pop())
        except IndexError:
            print ("Error happened when rebalanced the non-exist heaps of 'CMTE_ID|TRANSACTION_DT' or 'CMTE_ID|ZIPCODE'.")
            return

    def transaction_heaps_get_median(self, identifier):
        """ Get the median of transaction amount which is indentified by indentifier.
        Args:
          identifier: Identify the transaction heaps from which the median is got. The format shoud be 'CMTE_ID|TRANSACTION_DT' or 'CMTE_ID|ZIPCODE'.
        """
        try:
            # First, find the heap whose size is larger, and whose is smaller.
            if self.transaction_amt_heaps[identifier][0].size() > self.transaction_amt_heaps[identifier][1].size():
                bigger_heap = self.transaction_amt_heaps[identifier][0]
                smaller_heap = self.transaction_amt_heaps[identifier][1]
            else:
                bigger_heap = self.transaction_amt_heaps[identifier][1]
                smaller_heap = self.transaction_amt_heaps[identifier][0]
            # Then, find the median.
            # If the size of 2 heap is equal, return the average value of "the max value of min heap and the min value of max heap".
            if bigger_heap.size() == smaller_heap.size():
                return int(round((bigger_heap.peek() + smaller_heap.peek())/2.0))
            else:
                return int(round(bigger_heap.peek()))
        except IndexError:
            print ("Error happened when got median from the non-exist heaps of 'CMTE_ID|TRANSACTION_DT' or 'CMTE_ID|ZIPCODE'.")
            return

    def handler(self, data):
        pass

    def run(self):
        while(True):
            data = self.task_queue.get()
            self.handler(data)
            self.task_queue.task_done()

class TransactionByDateThread(TransactionThread):
    """ The thread handles transaction by date.  """
    def __init__(self, output_file_path):
        TransactionThread.__init__(self, output_file_path)

    def handler(self, data):
        """ Store input data and store transaction amount into 2 heaps according to the algorithm shown above.
        Args:
          data: input data. Format: {"cmte_id" : string,
                                     "zipcode" : string,
                                     "transaction_dt" : 'MMDDYYYY'],
                                     "transaction_amt" : float}
        """
        # If data["cmte_id"] is empty, it indicates that all data has been inserted.
        # So, it is time to get median for each CMTE_ID and TRANSACTION_DT pair and write the output file.
        if not data["cmte_id"]:
            for identifier in self.transaction_amt_heaps:
                number_of_transactions = self.transaction_amt_heaps[identifier][0].size() + self.transaction_amt_heaps[identifier][1].size()
                median = self.transaction_heaps_get_median(identifier)
                total_transaction_amount = int(self.total_amt_dict[identifier])
                cmte_id = identifier.split('|')[0]
                transaction_dt = identifier.split('|')[1]
                self.output_file.write('%s|%s|%d|%d|%d\n' % (cmte_id, transaction_dt, median, number_of_transactions, total_transaction_amount))
            self.output_file.close()
            return
        # For valid data, store them and store transaction amount into "Balanced Heaps".
        identifier = '%s|%s' % (data["cmte_id"], data["transaction_dt"])
        if identifier not in self.total_amt_dict:
            self.total_amt_dict[identifier] = 0
        self.total_amt_dict[identifier] += data["transaction_amt"]
        self.transaction_heaps_add_number(identifier, data["transaction_amt"])
        self.transaction_heaps_rebalance(identifier)


class TransactionByZipThread(TransactionThread):
    """ The thread handles transaction by data.  """
    def __init__(self, output_file_path):
        TransactionThread.__init__(self, output_file_path)

    def handler(self, data):
        """ Store input data and store transaction amount into 2 heaps according to the algorithm shown above.
        Args:
          data: input data. Format: {"cmte_id" : string,
                                     "zipcode" : string,
                                     "transaction_dt" : 'MMDDYYYY'],
                                     "transaction_amt" : float}
        """
        # If data["cmte_id"] is empty, it represents the end of data stream.
        if not data["cmte_id"]:
            self.output_file.close()
            return
        # If it`s not end, get runnning median and write output file.
        identifier = '%s|%s' % (data["cmte_id"], data["zipcode"])
        if identifier not in self.total_amt_dict:
            self.total_amt_dict[identifier] = 0
        self.total_amt_dict[identifier] += data["transaction_amt"]
        self.transaction_heaps_add_number(identifier, data["transaction_amt"])
        self.transaction_heaps_rebalance(identifier)
        number_of_transactions = self.transaction_amt_heaps[identifier][0].size() + self.transaction_amt_heaps[identifier][1].size()
        running_median = self.transaction_heaps_get_median(identifier)
        total_transaction_amount = int(self.total_amt_dict[identifier])
        self.output_file.write('%s|%s|%d|%d|%d\n' % (data["cmte_id"], data["zipcode"], running_median, number_of_transactions, total_transaction_amount))
