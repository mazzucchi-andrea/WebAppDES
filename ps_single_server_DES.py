import csv
from datetime import datetime

import numpy as np

from rngs import select_stream, plant_seeds, get_seed
from rvgs import exponential
from rvms import idfStudent

ALPHA = 0.05
START = 0.0  # initial time
STOP = 57600.0  # terminal time
INFINITY = (100.0 * STOP)  # must be much larger than STOP
arrivalTemp = START


def get_arrival(arrival_rate):
    global arrivalTemp

    select_stream(0)
    arrivalTemp += exponential(1.0 / arrival_rate)
    return arrivalTemp


def get_service():
    select_stream(1)
    return exponential(0.7)


class Time:
    arrival_a = INFINITY  # next arrival time for jobs of type A1
    completion_a = INFINITY  # next completion time on server A
    current = INFINITY  # current time
    next = INFINITY  # next (most imminent) event time
    last = INFINITY  # last arrival_a time


class Track:
    def __init__(self):
        self.node = 0.0  # time integrated number in the node
        self.service = 0.0  # time integrated number in service

    def update(self, current_time, next_time, number):
        self.node += (next_time - current_time) * number
        self.service += (next_time - current_time)


class Job:
    def __init__(self, arrival):
        self.arrival = arrival
        self.remaining = get_service()


class Server:
    def __init__(self):
        self.jobs = []
        self.number = 0  # number in the node
        self.index = 0  # used to count departed jobs
        self.area = Track()
        self.last_event = 0

        # interarrival
        self.arrivals = 0
        self.last_arrival = 0
        self.avg_interarrival = 0
        self.interarrival_variance = 0

        # service
        self.avg_service = 0
        self.service_variance = 0

    def get_min_remaining_process_time(self):
        min_job = min(self.jobs, key=lambda job: job.remaining)
        return min_job.remaining

    def get_next_complete_process_time(self):
        return self.get_min_remaining_process_time() * self.number

    def reset_stats(self, current_time):
        self.index = 0
        self.area.node = 0
        self.area.service = 0
        self.last_event = current_time

        self.avg_interarrival = 0
        self.arrivals = 0
        self.interarrival_variance = 0
        self.last_arrival = 0

        self.avg_service = 0
        self.service_variance = 0

    def process_arrival(self, new_job):
        if self.number > 0:
            processed_time = (new_job.arrival - self.last_event) / self.number
            for job in self.jobs:
                job.remaining -= processed_time
                job.last_event = job.arrival
        self.last_event = new_job.arrival

        self.arrivals += 1
        d = new_job.arrival - self.last_arrival - self.avg_interarrival
        self.last_arrival = new_job.arrival
        self.interarrival_variance += d * d * (self.arrivals - 1) / self.arrivals
        self.avg_interarrival += d / self.arrivals

        self.jobs.append(new_job)
        self.number += 1

    def process_completion(self, completion_time):
        if self.number > 0:
            processed_time = (completion_time - self.last_event) / self.number
            for job in self.jobs:
                job.remaining -= processed_time
                job.last_event = completion_time
        self.last_event = completion_time

        self.jobs.sort(key=lambda x: x.remaining)
        completed_job = self.jobs.pop(0)
        self.index += 1
        self.number -= 1

        d = completion_time - completed_job.arrival - self.avg_service
        self.service_variance += d * d * (self.index - 1) / self.index
        self.avg_service += d / self.index

        return completed_job


def model(arrival_rate, b=0, k=0):
    global arrivalTemp
    arrivalTemp = START
    batch_enabled = b != 0 and k != 0

    server_a = Server()
    t = Time()

    t.current = START  # set the clock
    t.arrival_a = get_arrival(arrival_rate)  # schedule the first arrival_a

    arrivals_a = 0  # batch measure index
    means = []

    while t.arrival_a < STOP or server_a.number > 0:

        t.next = min(t.arrival_a, t.completion_a)  # next event time

        # update integrals
        if server_a.number > 0:
            server_a.area.update(t.current, t.next, server_a.number)

        t.current = t.next  # advance the clock

        # arrival_a
        if t.current == t.arrival_a:
            server_a.process_arrival(Job(t.arrival_a))

            t.arrival_a = get_arrival(arrival_rate)
            if t.arrival_a > STOP:
                t.last = t.current
                t.arrival_a = INFINITY
            t.completion_a = t.current + server_a.get_next_complete_process_time()

            arrivals_a += 1

        # completion_a
        elif t.current == t.completion_a:
            server_a.process_completion(t.completion_a)

            if server_a.number > 0:
                t.completion_a = t.current + server_a.get_next_complete_process_time()
            else:
                t.completion_a = INFINITY

        if batch_enabled and arrivals_a == b:
            means.append(
                [server_a.avg_interarrival, server_a.avg_service, server_a.area.node / t.current,
                 server_a.area.service / t.current, server_a.index])

            arrivals_a = 0
            t.completion_a -= t.current

            t.arrival_a -= t.current
            t.current = START
            arrivalTemp = START

            server_a.reset_stats(t.current)

            for j in server_a.jobs:
                j.arrival = t.current

            if len(means) == k:
                data = []
                for i in range(5):
                    mean = 0

                    for m in means:
                        mean += m[i]
                    mean /= k
                    data.append(mean)
                    n = 0
                    for m in means:
                        n += pow((m[i] - mean), 2)
                    data.append(idfStudent(k - 1, 1 - ALPHA / 2) * np.sqrt(n / (k - 1)) / np.sqrt(k - 1))

                return data

    return [server_a.avg_interarrival, server_a.avg_service, server_a.area.node / t.current,
            server_a.area.service / t.current, server_a.index]


def finite_horizon_simulation():
    global STOP

    start = datetime.now()
    seed = 123456789
    STOP = 28880
    print("Start Finite Horizon Simulation")
    with open('data_single_ps_finite_horizon.csv', 'w', newline='') as csvfile:
        fieldnames = ['seed', 'arrival_rate',
                      'interarrival_a', 'avg_service_a', 'avg_population_a', 'utilization_a', 'completion_a']
        writer = csv.writer(csvfile)
        writer.writerow(fieldnames)

        arrival_rates = [0.5, 0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95, 1.0, 1.05, 1.1, 1.15, 1.2]
        for _ in range(0, 8):
            for arrival_rate in arrival_rates:
                plant_seeds(seed)
                print(f"Finite Horizon: seed {seed}, arrival_rate {arrival_rate}")
                data = [seed, arrival_rate]
                data += model(arrival_rate)
                writer.writerow(data)
            seed = get_seed()

    end = datetime.now()

    print(f"Finite Horizon Simulation time: {end - start}\n")


def batch_means_simulation():
    start = datetime.now()
    seed = 123456789
    print("Start Batch Means Simulation")
    with open('data_single_ps_batch_means.csv', 'w', newline='') as csvfile:
        fieldnames = ['arrival_rate',
                      'interarrival_a', 'interarrival_a_ci',
                      'avg_service_a', 'avg_service_a_ci',
                      'avg_population_a', 'avg_population_a_ci',
                      'utilization_a', 'utilization_a_ci',
                      'completion_a', 'completion_a_ci']
        writer = csv.writer(csvfile)
        writer.writerow(fieldnames)

        arrival_rates = [0.5, 0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95, 1.0, 1.05, 1.1, 1.15, 1.2]
        for arrival_rate in arrival_rates:
            plant_seeds(seed)
            print(f"Batch Means: arrival_rate {arrival_rate}")
            data = [arrival_rate]
            data += model(arrival_rate, 8192, 64)
            writer.writerow(data)

    end = datetime.now()
    print(f"Batch Means Simulation time: {end - start}\n")


def main():
    start = datetime.now()

    # finite_horizon_simulation()
    batch_means_simulation()

    end = datetime.now()
    print(f"Total Simulation time: {end - start}\n")


if __name__ == "__main__":
    main()
