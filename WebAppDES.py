import csv
from datetime import datetime
from enum import Enum

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


class JobType(Enum):
    A1 = 1
    A2 = 2
    A3 = 3
    B = 4
    P = 5


def get_service(job_type, auth=1, b_improvement=False):
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
        if b_improvement:
            avg_demand = 0.4
        else:
            avg_demand = 0.8
    elif job_type == JobType.P:
        select_stream(5)
        if auth == 1:
            avg_demand = 0.4
        else:
            avg_demand = 0.7
    return exponential(avg_demand)


class Track:
    def __init__(self):
        self.node = 0.0  # time integrated number in the node
        self.service = 0.0  # time integrated number in service

    def update(self, current_time, next_time, number):
        self.node += (next_time - current_time) * number
        self.service += (next_time - current_time)


class Time:
    arrival_a1 = INFINITY  # next arrival time for jobs of type A1
    arrival_a2 = INFINITY  # next arrival time for jobs of type A2
    arrival_a3 = INFINITY  # next arrival time for jobs of type A3
    arrival_b = INFINITY  # next arrival_b time
    arrival_p = INFINITY
    completion_a = INFINITY  # next completion time on server A
    completion_b = INFINITY  # next completion time on server B
    completion_p = INFINITY  # next completion time on server P
    current = INFINITY  # current time
    next = INFINITY  # next (most imminent) event time
    last = INFINITY  # last arrival_a time


class Job:
    def __init__(self, arrival, job_type, auth=1, b_improvement=False):
        self.arrival = arrival
        self.remaining = get_service(job_type, auth, b_improvement)
        self.job_type = job_type


class Server:
    def __init__(self):
        self.jobs = []
        self.number = 0  # number in the node
        self.index = 0  # used to count departed jobs
        self.area = Track()
        self.last_event = 0

    # def process_arrival(self, job, current_time):
    #     if self.number > 0:
    #         processed_time = (current_time - self.last_event) / self.number
    #         for job in self.jobs:
    #             job.remaining -= processed_time
    #     self.last_event = current_time
    #
    #     self.jobs.append(job)
    #     self.number += 1

    def get_min_remaining_process_time(self):
        min_job = min(self.jobs, key=lambda job: job.remaining)
        return min_job.remaining

    def get_next_complete_process_time(self):
        return self.get_min_remaining_process_time() * self.number


def update_jobs_remaining_service_time(server, current_time):
    if server.number > 0:
        processed_time = (current_time - server.last_event) / server.number
        for job in server.jobs:
            job.remaining -= processed_time
            job.last_event = current_time
    server.last_event = current_time


def model(arrival_rate, auth, b=0, k=0, b_improvement=False):
    global arrivalTemp
    arrivalTemp = START
    batch_enabled = b != 0 and k != 0

    server_a = Server()
    server_b = Server()
    server_p = Server()

    t = Time()

    t.current = START  # set the clock
    t.arrival_a1 = get_arrival(arrival_rate)  # schedule the first arrival_a

    a3_completed_jobs = 0  # batch measure index
    means = []

    while ((t.arrival_a1 < STOP or t.arrival_a2 != INFINITY or t.arrival_a3 != INFINITY or t.arrival_b != INFINITY or
            t.arrival_p != INFINITY or server_a.number > 0 or server_b.number > 0 or server_p.number > 0)):

        t.next = min(t.arrival_a1, t.arrival_a2, t.arrival_a3, t.arrival_b, t.arrival_p, t.completion_a, t.completion_b,
                     t.completion_p)  # next event time

        # update integrals
        if server_a.number > 0:
            server_a.area.update(t.current, t.next, server_a.number)
        if server_b.number > 0:
            server_b.area.update(t.current, t.next, server_b.number)
        if server_p.number > 0:
            server_p.area.update(t.current, t.next, server_p.number)

        t.current = t.next  # advance the clock

        # arrival_a1
        if t.current == t.arrival_a1:
            update_jobs_remaining_service_time(server_a, t.current)

            server_a.jobs.append(Job(t.arrival_a1, JobType.A1))
            server_a.number += 1

            t.arrival_a1 = get_arrival(arrival_rate)

            if t.arrival_a1 > STOP:
                t.last = t.current
                t.arrival_a1 = INFINITY

            t.completion_a = t.current + server_a.get_next_complete_process_time()

        # arrival_a2
        elif t.current == t.arrival_a2:
            update_jobs_remaining_service_time(server_a, t.current)

            server_a.jobs.append(Job(t.arrival_a2, JobType.A2))
            server_a.number += 1

            t.arrival_a2 = INFINITY

            t.completion_a = t.current + server_a.get_next_complete_process_time()

        # arrival_a3
        elif t.current == t.arrival_a3:
            update_jobs_remaining_service_time(server_a, t.current)

            server_a.jobs.append(Job(t.arrival_a3, JobType.A3, auth))
            server_a.number += 1

            t.arrival_a3 = INFINITY

            t.completion_a = t.current + server_a.get_next_complete_process_time()

        # arrival_b
        elif t.current == t.arrival_b:
            update_jobs_remaining_service_time(server_b, t.current)

            server_b.jobs.append(Job(t.arrival_b, JobType.B, b_improvement=b_improvement))
            server_b.number += 1

            t.arrival_b = INFINITY

            t.completion_b = t.current + server_b.get_next_complete_process_time()

        # arrival_p
        elif t.current == t.arrival_p:
            update_jobs_remaining_service_time(server_p, t.current)

            server_p.jobs.append(Job(t.arrival_p, JobType.P, auth))
            server_p.number += 1

            t.arrival_p = INFINITY

            t.completion_p = t.current + server_p.get_next_complete_process_time()

        # completion_a
        elif t.current == t.completion_a:
            processed_time = server_a.get_min_remaining_process_time()
            completed_job = None

            for job in server_a.jobs:
                job.remaining -= processed_time

            server_a.last_event = t.current

            for job in server_a.jobs:
                if job.remaining == 0:
                    completed_job = job
                    server_a.jobs.remove(job)
                    break

            server_a.index += 1
            server_a.number -= 1

            if completed_job.job_type == JobType.A1:
                t.arrival_b = t.current
            elif completed_job.job_type == JobType.A2:
                t.arrival_p = t.current
            else:
                a3_completed_jobs += 1

            if server_a.number > 0:
                t.completion_a = t.current + server_a.get_next_complete_process_time()
            else:
                t.completion_a = INFINITY

        # completion_b
        elif t.current == t.completion_b:
            processed_time = server_b.get_min_remaining_process_time()

            for job in server_b.jobs:
                job.remaining -= processed_time

            server_b.last_event = t.current

            for job in server_b.jobs:
                if job.remaining == 0:
                    server_b.jobs.remove(job)
                    break

            server_b.index += 1
            server_b.number -= 1

            t.arrival_a2 = t.current

            if server_b.number > 0:
                t.completion_b = t.current + server_b.get_next_complete_process_time()
            else:
                t.completion_b = INFINITY

        # completion_p
        elif t.current == t.completion_p:
            processed_time = server_p.get_min_remaining_process_time()

            for job in server_p.jobs:
                job.remaining -= processed_time

            server_p.last_event = t.current

            for job in server_p.jobs:
                if job.remaining == 0:
                    server_p.jobs.remove(job)
                    break

            server_p.index += 1
            server_p.number -= 1

            t.arrival_a3 = t.current

            if server_p.number > 0:
                t.completion_p = t.current + server_p.get_next_complete_process_time()
            else:
                t.completion_p = INFINITY

        if batch_enabled and a3_completed_jobs == b:
            avg_population, avg_population_a, avg_population_b, avg_population_p, avg_response_time, avg_service_a, avg_service_b, avg_service_p, interarrival_a, interarrival_b, interarrival_p, utilization_a, utilization_b, utilization_p = get_simulation_statistics(
                server_a, server_b, server_p, t.current, t.current)

            means.append([interarrival_a, avg_service_a, avg_population_a, utilization_a, server_a.index,
                          interarrival_b, avg_service_b, avg_population_b, utilization_b, server_b.index,
                          interarrival_p, avg_service_p, avg_population_p, utilization_p, server_p.index,
                          avg_response_time, avg_population])

            a3_completed_jobs = 0
            t.completion_a -= t.current
            t.completion_b -= t.current
            t.completion_p -= t.current
            t.arrival_a1 -= t.current
            t.arrival_a2 -= t.current
            t.arrival_a3 -= t.current
            t.arrival_b -= t.current
            t.arrival_p -= t.current
            t.current = START
            arrivalTemp = START

            server_a.last_event = t.current
            server_a.index = 0
            server_a.area.node = 0
            server_a.area.service = 0
            server_b.last_event = t.current
            server_b.index = 0
            server_b.area.node = 0
            server_b.area.service = 0
            server_p.last_event = t.current
            server_p.index = 0
            server_p.area.node = 0
            server_p.area.service = 0

            if len(means) == k:
                data = []
                for i in range(17):
                    mean = 0

                    for m in means:
                        mean += m[i]
                    mean /= k
                    data.append(mean)
                    n = 0
                    for m in means:
                        n += pow((m[i] - data[i]), 2)
                    data.append(idfStudent(k - 1, 1 - ALPHA / 2) * np.sqrt(n / (k - 1)) / np.sqrt(k - 1))

                return data

    avg_population, avg_population_a, avg_population_b, avg_population_p, avg_response_time, avg_service_a, avg_service_b, avg_service_p, interarrival_a, interarrival_b, interarrival_p, utilization_a, utilization_b, utilization_p = get_simulation_statistics(
        server_a, server_b, server_p, t.last, t.current)

    return [interarrival_a, avg_service_a, avg_population_a, utilization_a, server_a.index,
            interarrival_b, avg_service_b, avg_population_b, utilization_b, server_b.index,
            interarrival_p, avg_service_p, avg_population_p, utilization_p, server_p.index,
            avg_response_time, avg_population]


def get_simulation_statistics(server_a, server_b, server_p, last_time, current_time):
    interarrival_a = last_time / server_a.index
    avg_service_a = server_a.area.node / server_a.index
    avg_population_a = server_a.area.node / current_time
    utilization_a = server_a.area.service / current_time
    interarrival_b = last_time / server_b.index
    avg_service_b = server_b.area.node / server_b.index
    avg_population_b = server_b.area.node / current_time
    utilization_b = server_b.area.service / current_time
    interarrival_p = last_time / server_p.index
    avg_service_p = server_p.area.node / server_p.index
    avg_population_p = server_p.area.node / current_time
    utilization_p = server_p.area.service / current_time
    avg_response_time = 3 * (server_a.area.node / server_a.index) + server_b.area.node / server_b.index + server_p.area.node / server_p.index
    avg_population = server_a.area.node / current_time + server_b.area.node / current_time + server_p.area.node / current_time
    return avg_population, avg_population_a, avg_population_b, avg_population_p, avg_response_time, avg_service_a, avg_service_b, avg_service_p, interarrival_a, interarrival_b, interarrival_p, utilization_a, utilization_b, utilization_p


def finite_horizon_simulation():
    global STOP

    start = datetime.now()
    seed = 123456789
    STOP = 28880
    print("Start Finite Horizon Simulation")
    with open('data_finite_horizon.csv', 'w', newline='') as csvfile:
        fieldnames = ['seed', 'auth', 'b_improvement', 'arrival_rate',
                      'interarrival_a', 'avg_service_a', 'avg_population_a', 'utilization_a', 'completion_a',
                      'interarrival_b', 'avg_service_b', 'avg_population_b', 'utilization_b', 'completion_b',
                      'interarrival_p', 'avg_service_p', 'avg_population_p', 'utilization_p', 'completion_p',
                      'avg_response_time', 'avg_population']
        writer = csv.writer(csvfile)
        writer.writerow(fieldnames)

        auth_types = [1, 2]
        arrival_rates = [0.5, 0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95, 1.0, 1.05, 1.1, 1.15, 1.2]
        for _ in range(0, 64):
            for auth in auth_types:
                for b_improvement in [True, False]:
                    for arrival_rate in arrival_rates:
                        plant_seeds(seed)
                        # print(f"Finite Horizon: seed {seed}, arrival_rate {arrival_rate} and auth type {auth}")
                        data = [seed, auth, b_improvement, arrival_rate]
                        data += model(arrival_rate, auth, b_improvement=b_improvement)
                        writer.writerow(data)
            seed = get_seed()

    end = datetime.now()

    print(f"Finite Horizon Simulation time: {end - start}\n")


def batch_means_simulation():
    start = datetime.now()
    seed = 123456789
    print("Start Batch Means Simulation")
    with open('data_batch_means.csv', 'w', newline='') as csvfile:
        fieldnames = ['seed', 'auth', 'b_improvement', 'arrival_rate',
                      'interarrival_a', 'interarrival_a_ci',
                      'avg_service_a', 'avg_service_a_ci',
                      'avg_population_a', 'avg_population_a_ci',
                      'utilization_a', 'utilization_a_ci',
                      'completion_a', 'completion_a_ci',
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

        auth_types = [1, 2]
        arrival_rates = [0.5, 0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95, 1.0, 1.05, 1.1, 1.15, 1.2]
        for auth in auth_types:
            for b_improvement in [True, False]:
                for arrival_rate in arrival_rates:
                    plant_seeds(seed)
                    # print(f"Batch Means: seed {seed}, arrival_rate {arrival_rate} and auth type {auth}")
                    data = [seed, auth, b_improvement, arrival_rate]
                    data += model(arrival_rate, auth, 8192, 128, b_improvement)
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
