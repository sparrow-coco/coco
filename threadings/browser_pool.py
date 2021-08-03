import globals
import time, random

from threading import Thread
from typing import Callable

"""
multiprocessing.pool CANNOT be used here because webdrivers CANNOT BE PICKLED
"""
class Queue(object):
    def __init__(self) -> None:
        super().__init__()
        self._queue = []

    def empty(self):
        return len(self._queue) == 0
    
    def put(self, object):
        self._queue.append(object)

    # busy waiting
    def get(self):
        while True:
            if self.empty():
                time.sleep(random.random())
                continue
            return self._queue.pop()
    

class BrowserPool(object):
    def __init__(self, browser_list:list) -> None:
        super().__init__()
        self.queue = Queue()
        self.__queue = Queue() # this is used for clean up
        for browser in browser_list:
            self.queue.put(browser)
            self.__queue.put(browser)
        self.__thread_pool = []
    
    """
    Blocking get. Use it when you are multithreading
    """
    def get(self):
        return self.queue.get()

    def put(self, object):
        return self.queue.put(object)

    def empty(self):
        return self.queue.empty()

    def execute_with_browser(self, function:Callable, *args, nohang=True, **kargs):
        """wrapper for getting providing a Browser (driver) instance to a function

        Args:
            function (Callable): original callable function
            nohang (bool, optional): whether or not we should WAIT for getting an available driver. Defaults to True.
        
        Example:
            def function(a,b,c=None, browser=None, browser_pool:BrowserPool=None):
                # @browser will be provided by this function \n
                do_some_work() \n
                done() \n
                # @browser will be placed back automatically when done \n
                return
        """
        def wrapper_function(function:Callable):
            browser = self.get()
            new_kwargs = dict(**kargs)
            new_kwargs["browser"] = browser
            new_kwargs["browser_pool"] = self
            try:
                ret = function(*args, **new_kwargs)
            except Exception as e:
                self.put(browser)
                raise e
            self.put(browser)
            return ret
        if nohang:
            thread = Thread(target=wrapper_function, args=(function, ), daemon=True)
            self.__thread_pool.append(thread)
            thread.start()
        else:
            wrapper_function(function)
        print("Submitted new task with browser")
        return

    def join(self):
        print("Waiting for all subprocesses")
        for process in self.__thread_pool:
            process.join()
        print("All subprocesses returned")
        self.__thread_pool = []
        return

    def close(self):
        while not self.__queue.empty():
            browser = self.__queue.get()
            browser.quit()
        return