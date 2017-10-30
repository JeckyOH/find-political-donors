""" This module defines the Heap data structure and corresponding operations.

In detail, though Python provides us heapq module to manipulate Min Heap,
it is not enough for my implementation of this challenge due to the use of Max Heap.
Besides, considering the simplicity and clearity in the codes, I wrap the codes of Heap into one module.
"""

import heapq

class MinHeap(object):
    """ Implementation of Min Heap by using heapq module.

    Attributes:
      heap: a list of elements in the heap format.
    """
    def __init__(self, elements = []):
        self.heap = [e for e in elements]
        heapq.heapify(self.heap)

    def push(self, value):
        """ Push an element into the heap.
        Args:
          value: The element to be inserted.
        """
        heapq.heappush(self.heap, value)

    def pop(self):
        """ Pop and return the root element.
        Returns:
          Root element. For Min Heap, it will be the element with lowest priority.
          If the heap is empty, return None.
        """
        if self.heap:
            return heapq.heappop(self.heap)
        return None

    def peek(self):
        """ Get the root element without poping it.
        Returns:
          Root Element. For Min Heap, it will be element with lowest priority.
          If the heap is empty, return None.
        """
        if self.heap:
            return self.heap[0]
        return None

    def size(self):
        """ Return the length of heap.  """
        return len(self.heap)

    def empty(self):
        """ Used for check whether heap is empty.  """
        if not self.heap:
            return True
        return False




class MaxHeap(object):
    """ Implementation of Max Heap by using heapq module.

    Attributes:
      heap: a list of elements in the heap format.
    """
    def __init__(self, elements = []):
        self.heap = [-e for e in elements]
        heapq.heapify(self.heap)

    def push(self, value):
        """ Push an element into the heap.
        Args:
          value: The element to be inserted.
        """
        heapq.heappush(self.heap, -value)

    def pop(self):
        """ Pop and return the root element.
        Returns:
          Root element. For Min Heap, it will be the element with lowest priority.
          If the heap is empty, return None.
        """
        if self.heap:
            return -heapq.heappop(self.heap)
        return None

    def peek(self):
        """ Get the root element without poping it.
        Returns:
          Root Element. For Min Heap, it will be element with lowest priority.
          If the heap is empty, return None.
        """
        if self.heap:
            return -self.heap[0]
        return None

    def size(self):
        """ Return the length of heap.  """
        return len(self.heap)

    def empty(self):
        """ Used for check whether heap is empty.  """
        if not self.heap:
            return True
        return False
