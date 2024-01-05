import time
import dask
from dask.distributed import Client, LocalCluster
from concurrent.futures import ThreadPoolExecutor
from dask.delayed import Delayed
from dask.threaded import get

from parla import sleep_nogil, sleep_gil
import argparse

free_sleep = sleep_nogil
lock_sleep = sleep_gil

parser = argparse.ArgumentParser()
parser.add_argument("-workers", type=int, default=1)
parser.add_argument("-n", type=int, default=3)
parser.add_argument("-t", type=int, default=1000)
parser.add_argument("-accesses", type=int, default=1)
parser.add_argument("-frac", type=float, default=0)
parser.add_argument('-sweep', type=int, default=0)
parser.add_argument('-verbose', type=int, default=0)
parser.add_argument('-empty', type=int, default=0)
args = parser.parse_args()

kernel_time = args.t / args.accesses
free_time = kernel_time * (1 - args.frac)
lock_time = kernel_time * (args.frac)

def waste_time(i):
    if args.verbose:
        inner_start_t = time.perf_counter()
        print("Task", i, " | Start", flush=True)

    for k in range(args.accesses):
        free_sleep(free_time)
        lock_sleep(lock_time)

    if args.verbose:
        inner_end_t = time.perf_counter()
        print("Task", i, " | Inner Time: ",
              inner_end_t - inner_start_t, flush=True)

def create_graph(n):
    dsk = {}
    for i in range(n):
        dsk['task'+str(i)] = (waste_time, i)
    dsk['barrier'] = list(dsk.keys())
    return dsk

if __name__ == '__main__':

    print(', '.join([str('workers'), str('n'), str('task_time'), str(
        'accesses'), str('frac'), str('total_time')]), flush=True)
    
    with dask.config.set(pool=ThreadPoolExecutor(args.workers)):

        start_t = time.perf_counter()
        dsk = create_graph(args.n)
        get(dsk, 'barrier')
        end_t = time.perf_counter()
        elapsed_t = end_t - start_t
        
        print(', '.join([str(args.workers), str(args.n), str(args.t),
              str(args.accesses), str(args.frac), str(elapsed_t)]), flush=True)
