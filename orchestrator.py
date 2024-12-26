import multiprocessing
from multiprocessing.synchronize import Semaphore, Event
import time
import os
import argparse
from worker import backtest
from utils import get_cfg
from easydict import EasyDict

NUMBER_OF_PROCESSES = os.cpu_count() * 50 // 100  # Adjust this as desired

def with_continuous_multiprocessing(stop_event: Event):
    """Function to continuously spawn processes until interrupted.

    Args:
        stop_event (Event): Event to signal stopping
    """
    global NUMBER_OF_PROCESSES
    semaphore = multiprocessing.Semaphore(NUMBER_OF_PROCESSES)
    config = get_cfg() # TODO: Replace with fetching the configuration from a redis server

    while not stop_event.is_set():
        if stop_event.wait(0.1):
            break

        semaphore.acquire()
        process = multiprocessing.Process(target=worker, args=(config, semaphore, stop_event))
        process.start()

def worker(config: EasyDict, semaphore: Semaphore, stop_event: Event):
    """Wrapper for backtest to release the semaphore after completion.

    Args:
        config (EasyDict): Configuration dictionary for the backtester
        semaphore (Semaphore): Semaphore to control the number of active processes
        stop_event (Event): Event to signal stopping
    """
    try:
        if not stop_event.is_set():
            backtest(config)
            print("Worker completed.")
    except Exception as e:
        print(f"Error in worker: {e}")
    finally:
        semaphore.release()

def main():
    global NUMBER_OF_PROCESSES
    print("Number of processes:", NUMBER_OF_PROCESSES)
    print("Press Ctrl+C to stop the program.")
    
    stop_event = multiprocessing.Event()

    start_time = time.perf_counter()
    try:
        with_continuous_multiprocessing(stop_event)
    except KeyboardInterrupt:
        print("--- Interrupted by user ---")
        stop_event.set()
        for process in multiprocessing.active_children():
            process.terminate()
        print("--- All processes terminated ---")
    finally:
        elapsed_time = time.perf_counter() - start_time
        print("--- Time elapsed: %s seconds ---" % elapsed_time)

if __name__ == "__main__":
    args = argparse.ArgumentParser()
    args_group = args.add_mutually_exclusive_group()
    args_group.add_argument('-j', '--jobs', type=int, help='Number of processes to spawn, default = Half of CPU cores')
    args_group.add_argument('-p', '--percentage', type=int, help='Percentage of CPU cores to use, default = 50%%')
    args = args.parse_args()
    
    if args.jobs:
        NUMBER_OF_PROCESSES = args.jobs
    elif args.percentage:
        NUMBER_OF_PROCESSES = os.cpu_count() * args.percentage // 100
    
    if NUMBER_OF_PROCESSES < 1:
        print("Number of processes should be at least 1.")
        exit(1)
    
    main()
