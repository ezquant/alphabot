from Common.Log.Logger import Logger
import time

def running_time(method):
    def time_cal(*args, **kwargs):
        start = time.time()
        method(*args, **kwargs)
        spent_time =  time.time() - start

        Logger.get_logger(__name__).debug(f"The func {method.__name__} cost time: {spent_time * 1000} ms")

        return spent_time

    return time_cal