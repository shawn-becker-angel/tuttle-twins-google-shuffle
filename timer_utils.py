from time import perf_counter

def s3_timer(func):
    # This function shows the execution time of 
    # the function object passed
    def wrap_func(*args, **kwargs):
        t1 = perf_counter()
        result = func(*args, **kwargs)
        elapsed = perf_counter() - t1
        print(f'{func.__name__!r} executed in {elapsed:.6f}s')
        return result
    return wrap_func
