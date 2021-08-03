import functools, sys
import globals

from threading import Thread
from typing import Callable

seconds_tmp = -1

def timeout(seconds=-1, cleanup_function:Callable=None, **cleanup_kwargs):
    """timeout wrapper for a function

    Args:
        seconds (int, optional): seconds after which a function will be timeed out. Defaults to @globals.timeout_sec.
        cleanup_function (Callable, optional): function called when time is up and program has NOT YET terminated. Defaults to None.
        Note that the arguments to cleanup_function can ONLY BE **kwargs

    Raises:
        je: if a Thread for @timeout failed to start. Then obviously @timeout does not work
        ret: result or exception from the wrapped function. This is returned ONLY if the function returned within time

    Returns:
        decorator for a function
    """
    global seconds_tmp
    seconds_tmp = seconds
    def deco(func):        
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            global seconds_tmp
            # you can only alter seconds_tmp INSIDE wrapper. Otherwise seconds_tmp will be determined statically
            if seconds_tmp < 0:
                seconds_tmp = globals.timeout_sec
            msg = f"Timeout [{seconds_tmp} seconds] exceeded for {func.__name__}!"
            # using res = Exception(msg) would make @res in the function @newFunc not visible
            res = [msg]
            def newFunc():
                try:
                    res[0] = func(*args, **kwargs)
                except Exception as e:
                    res[0] = e
            t = Thread(target=newFunc)
            t.daemon = True
            try:
                t.start()
                t.join(seconds_tmp)
            except Exception as je:
                print ('error starting thread')
                raise je
            if cleanup_function is not None:
                cleanup_function(**cleanup_kwargs)
            if res[0] == msg:
                print(msg)
                sys.exit(0)
            if isinstance(res[0], BaseException):
                raise res[0]
            return res[0]
        return wrapper
    return deco