import csv
from datetime import datetime
from enum import Enum

import numpy as np

from rngs import select_stream, plant_seeds, get_seed
from rvgs import exponential, bernoulli
from rvms import idfStudent

ALPHA = 0.05
START = 0.0  # initial time
STOP = 5760000.0  # terminal time
INFINITY = (100.0 * STOP)  # must be much larger than STOP
arrivalTemp = START


def get_arrival(arrival_rate):
    global arrivalTemp

    select_stream(0)
    arrivalTemp += exponential(1.0 / arrival_rate)
    return arrivalTemp


def get_service(job_type, auth=1):
    avg_demand = -1
    if job_type == JobType.A1:
        select_stream(1)
        avg_demand = 0.2
    elif job_type == JobType.A2:
        select_stream(2)
        avg_demand = 0.4
    elif job_type == JobType.A3:
        select_stream(3)
        if auth == 1:
            avg_demand = 0.1
        else:
            avg_demand = 0.15
    elif job_type == JobType.B:
        select_stream(4)
        avg_demand = 0.4
    elif job_type == JobType.P:
        select_stream(5)
        if auth == 1:
            avg_demand = 0.4
        else:
            avg_demand = 0.7
    return exponential(avg_demand)

def round_robin():
    select_stream(6)
    return bernoulli(0.5)

class Time:
    arrival_a = INFINITY  # next arrival time for jobs of type A1
    arrival_p = INFINITY
    completion_a1 = INFINITY  # next completion time on server A1
    completion_a2 = INFINITY  # next completion time on server A2
    completion_b = INFINITY  # next completion time on server B
    completion_p = INFINITY  # next completion time on server P
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


class JobType(Enum):
    A1 = 1
    A2 = 2
    A3 = 3
    B = 4
    P = 5


class Job:
    def __init__(self, arrival, job_type, auth=1):
        self.arrival = arrival
        self.remaining = get_service(job_type, auth)
        self.job_type = job_type


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


def model(arrival_rate, auth, b=0, k=0):
    global arrivalTemp
    arrivalTemp = START
    batch_enabled = b != 0 and k != 0

    server_a1 = Server()
    server_a2 = Server()
    server_b = Server()
    server_p = Server()

    t = Time()

    t.current = START  # set the clock
    t.arrival_a = get_arrival(arrival_rate)  # schedule the first arrival_a

    a_arrivals = 0  # batch measure index
    means = []

    while t.arrival_a < STOP or server_a1.number > 0 or server_a2.number > 0 or server_b.number > 0 or server_p.number > 0:

        t.next = min(t.arrival_a, t.completion_a1, t.completion_a2, t.completion_b, t.completion_p)  # next event time

        # update integrals
        if server_a1.number > 0:
            server_a1.area.update(t.current, t.next, server_a1.number)
        if server_a2.number > 0:
            server_a2.area.update(t.current, t.next, server_a2.number)
        if server_b.number > 0:
            server_b.area.update(t.current, t.next, server_b.number)
        if server_p.number > 0:
            server_p.area.update(t.current, t.next, server_p.number)

        t.current = t.next  # advance the clock

        # arrival_a
        if t.current == t.arrival_a:
            if round_robin() == 1:
                server_a1.process_arrival(Job(t.current, JobType.A1))
                t.completion_a1 = t.current + server_a1.get_next_complete_process_time()
            else:
                server_a2.process_arrival(Job(t.current, JobType.A1))
                t.completion_a2 = t.current + server_a2.get_next_complete_process_time()

            t.arrival_a = get_arrival(arrival_rate)
            if t.arrival_a > STOP:
                t.last = t.current
                t.arrival_a = INFINITY

            a_arrivals += 1


        # completion_a1
        elif t.current == t.completion_a1:
            completed_job = server_a1.process_completion(t.current)

            if completed_job.job_type == JobType.A1:
                server_b.process_arrival(Job(t.current, JobType.B))
                t.completion_b = t.current + server_b.get_next_complete_process_time()
            elif completed_job.job_type == JobType.A2:
                server_p.process_arrival(Job(t.current, JobType.P, auth))
                t.completion_p = t.current + server_p.get_next_complete_process_time()

            if server_a1.number > 0:
                t.completion_a1 = t.current + server_a1.get_next_complete_process_time()
            else:
                t.completion_a1 = INFINITY

        # completion_a1
        elif t.current == t.completion_a2:
            completed_job = server_a2.process_completion(t.current)

            if completed_job.job_type == JobType.A1:
                server_b.process_arrival(Job(t.current, JobType.B))
                t.completion_b = t.current + server_b.get_next_complete_process_time()
            elif completed_job.job_type == JobType.A2:
                server_p.process_arrival(Job(t.current, JobType.P, auth))
                t.completion_p = t.current + server_p.get_next_complete_process_time()

            if server_a2.number > 0:
                t.completion_a2 = t.current + server_a2.get_next_complete_process_time()
            else:
                t.completion_a2 = INFINITY

        # completion_b
        elif t.current == t.completion_b:
            server_b.process_completion(t.completion_b)
            if round_robin():
                server_a1.process_arrival(Job(t.current, JobType.A2))
                t.completion_a1 = t.current + server_a1.get_next_complete_process_time()
            else:
                server_a2.process_arrival(Job(t.current, JobType.A2))
                t.completion_a2 = t.current + server_a2.get_next_complete_process_time()

            if server_b.number > 0:
                t.completion_b = t.current + server_b.get_next_complete_process_time()
            else:
                t.completion_b = INFINITY

        # completion_p
        elif t.current == t.completion_p:
            server_p.process_completion(t.completion_p)

            if round_robin():
                server_a1.process_arrival(Job(t.current, JobType.A3, auth))
                t.completion_a1 = t.current + server_a1.get_next_complete_process_time()
            else:
                server_a2.process_arrival(Job(t.current, JobType.A3, auth))
                t.completion_a2 = t.current + server_a2.get_next_complete_process_time()

            if server_p.number > 0:
                t.completion_p = t.current + server_p.get_next_complete_process_time()
            else:
                t.completion_p = INFINITY

        if batch_enabled and a_arrivals == b:
            (avg_population, avg_population_a1, avg_population_a2, avg_population_b, avg_population_p,
             avg_response_time, utilization_a1,
             utilization_a2, utilization_b, utilization_p) = (
                get_simulation_statistics(server_a1, server_a2, server_b, server_p, t.current))

            means.append(
                [server_a1.avg_interarrival, server_a1.avg_service, avg_population_a1, utilization_a1, server_a1.index,
                 server_a2.avg_interarrival, server_a2.avg_service, avg_population_a2, utilization_a2, server_a2.index,
                 server_b.avg_interarrival, server_b.avg_service, avg_population_b, utilization_b, server_b.index,
                 server_p.avg_interarrival, server_p.avg_service, avg_population_p, utilization_p, server_p.index,
                 avg_response_time, avg_population])

            t.completion_a1 -= t.current
            t.completion_a2 -= t.current
            t.completion_b -= t.current
            t.completion_p -= t.current
            t.arrival_a -= t.current
            t.current = START
            arrivalTemp = START
            a_arrivals = 0

            server_a1.reset_stats(t.current)
            server_a2.reset_stats(t.current)
            server_b.reset_stats(t.current)
            server_p.reset_stats(t.current)

            for j in server_a1.jobs:
                j.arrival = t.current
            for j in server_a2.jobs:
                j.arrival = t.current
            for j in server_b.jobs:
                j.arrival = t.current
            for j in server_p.jobs:
                j.arrival = t.current

            if len(means) == k:
                data = []
                for i in range(22):
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

    (avg_population,
     avg_population_a1, avg_population_a2, avg_population_b, avg_population_p,
     avg_response_time,
     utilization_a1, utilization_a2, utilization_b, utilization_p) = (
        get_simulation_statistics(server_a1, server_a2, server_b, server_p, t.current))

    return [server_a1.avg_interarrival, server_a1.avg_service, avg_population_a1, utilization_a1, server_a1.index,
            server_a2.avg_interarrival, server_a2.avg_service, avg_population_a2, utilization_a2, server_a2.index,
            server_b.avg_interarrival, server_b.avg_service, avg_population_b, utilization_b, server_b.index,
            server_p.avg_interarrival, server_p.avg_service, avg_population_p, utilization_p, server_p.index,
            avg_response_time, avg_population]


def get_simulation_statistics(server_a1, server_a2, server_b, server_p, current_time):
    avg_population_a1 = server_a1.area.node / current_time
    utilization_a1 = server_a1.area.service / current_time
    avg_population_a2 = server_a2.area.node / current_time
    utilization_a2 = server_a2.area.service / current_time
    avg_population_b = server_b.area.node / current_time
    utilization_b = server_b.area.service / current_time
    avg_population_p = server_p.area.node / current_time
    utilization_p = server_p.area.service / current_time
    avg_response_time = 3 * (server_a1.avg_service + server_a2.avg_service) / 2 + server_b.avg_service + server_p.avg_service
    avg_population = server_a1.area.node / current_time + server_a2.area.node / current_time + server_b.area.node / current_time + server_p.area.node / current_time
    return (avg_population,
            avg_population_a1, avg_population_a2, avg_population_b, avg_population_p,
            avg_response_time,
            utilization_a1, utilization_a2, utilization_b, utilization_p)


def batch_means_simulation():
    start = datetime.now()
    seed = 123456789
    print("Start Batch Means Simulation")
    with open('data_horizontalA_batch_means.csv', 'w', newline='') as csvfile:
        fieldnames = ['seed', 'arrival_rate',
                      'interarrival_a1', 'interarrival_a1_ci',
                      'avg_service_a1', 'avg_service_a1_ci',
                      'avg_population_a1', 'avg_population_a1_ci',
                      'utilization_a1', 'utilization_a1_ci',
                      'completion_a1', 'completion_a1_ci',
                      'interarrival_a2', 'interarrival_a2_ci',
                      'avg_service_a2', 'avg_service_a2_ci',
                      'avg_population_a2', 'avg_population_a2_ci',
                      'utilization_a2', 'utilization_a2_ci',
                      'completion_a2', 'completion_a2_ci',
                      'interarrival_b', 'interarrival_b_ci',
                      'avg_service_b', 'avg_service_b_ci',
                      'avg_population_b', 'avg_population_b_ci',
                      'utilization_b', 'utilization_b_ci',
                      'completion_b', 'completion_b_ci',
                      'interarrival_p', 'interarrival_p_ci',
                      'avg_service_p', 'avg_service_p_ci',
                      'avg_population_p', 'avg_population_p_ci',
                      'utilization_p', 'utilization_p_ci',
                      'completion_p', 'completion_p_ci',
                      'avg_response_time', 'avg_response_time_ci',
                      'avg_population', 'avg_population_ci']
        writer = csv.writer(csvfile)
        writer.writerow(fieldnames)

        arrival_rates = [0.5, 0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95, 1.0, 1.05, 1.1, 1.15, 1.2, 1.25, 1.3, 1.35, 1.4]
        for arrival_rate in arrival_rates:
            plant_seeds(seed)
            print(f"Batch Means: seed {seed}, arrival_rate {arrival_rate}")
            data = [seed, arrival_rate]
            data += model(arrival_rate, 1, 8192, 64)
            writer.writerow(data)

    end = datetime.now()
    print(f"Batch Means Simulation time: {end - start}\n")


def main():
    start = datetime.now()

    batch_means_simulation()

    end = datetime.now()
    print(f"Total Simulation time: {end - start}\n")


if __name__ == "__main__":
    main()
