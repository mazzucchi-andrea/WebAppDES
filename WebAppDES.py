import csv
from datetime import datetime
from enum import Enum

from rngs import select_stream, plant_seeds, get_seed
from rvgs import exponential

START = 0.0  # initial time
STOP = 86400.0  # terminal time
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


def get_service(job_type, auth):
    rate = -1
    if job_type == JobType.A1:
        select_stream(1)
        rate = 0.2
    elif job_type == JobType.A2:
        select_stream(2)
        rate = 0.4
    elif job_type == JobType.A3:
        select_stream(3)
        if auth == 1:
            rate = 0.1
        else:
            rate = 0.15
    elif job_type == JobType.B:
        select_stream(4)
        rate = 0.8
    elif job_type == JobType.P:
        select_stream(5)
        if auth == 1:
            rate = 0.4
        else:
            rate = 0.7
    return exponential(rate)


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
    def __init__(self, arrival, job_type, auth):
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


def get_min_remaining_process_time(jobs):
    min_job = min(jobs, key=lambda job: job.remaining)
    return min_job.remaining


def update_jobs_remaining_service_time(server, current_time):
    if server.number > 0:
        processed_time = (current_time - server.last_event) / server.number
        for job in server.jobs:
            job.remaining -= processed_time
            job.last_event = current_time
    server.last_event = current_time


def model(arrival_rate, auth):
    global arrivalTemp
    arrivalTemp = START

    server_a = Server()
    server_b = Server()
    server_p = Server()

    t = Time()

    t.current = START  # set the clock
    t.arrival_a1 = get_arrival(arrival_rate)  # schedule the first arrival_a

    while (t.arrival_a1 < STOP or t.arrival_a2 != INFINITY or t.arrival_a3 != INFINITY or t.arrival_b != INFINITY or
           t.arrival_p != INFINITY or server_a.number > 0 or server_b.number > 0 or server_p.number > 0):

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

            server_a.jobs.append(Job(t.arrival_a1, JobType.A1, auth))
            server_a.number += 1

            t.arrival_a1 = get_arrival(arrival_rate)

            if t.arrival_a1 > STOP:
                t.last = t.current
                t.arrival_a1 = INFINITY

            t.completion_a = t.current + get_min_remaining_process_time(server_a.jobs) * server_a.number

        # arrival_a2
        elif t.current == t.arrival_a2:
            update_jobs_remaining_service_time(server_a, t.current)

            server_a.jobs.append(Job(t.arrival_a2, JobType.A2, auth))
            server_a.number += 1

            t.arrival_a2 = INFINITY

            t.completion_a = t.current + get_min_remaining_process_time(server_a.jobs) * server_a.number

        # arrival_a3
        elif t.current == t.arrival_a3:
            update_jobs_remaining_service_time(server_a, t.current)

            server_a.jobs.append(Job(t.arrival_a3, JobType.A3, auth))
            server_a.number += 1

            t.arrival_a3 = INFINITY

            t.completion_a = t.current + get_min_remaining_process_time(server_a.jobs) * server_a.number

        # arrival_b
        elif t.current == t.arrival_b:
            update_jobs_remaining_service_time(server_b, t.current)

            server_b.jobs.append(Job(t.arrival_b, JobType.B, auth))
            server_b.number += 1

            t.arrival_b = INFINITY

            t.completion_b = t.current + (get_min_remaining_process_time(server_b.jobs) * server_b.number)

        # arrival_p
        elif t.current == t.arrival_p:
            update_jobs_remaining_service_time(server_p, t.current)

            server_p.jobs.append(Job(t.arrival_p, JobType.P, auth))
            server_p.number += 1

            t.arrival_p = INFINITY

            t.completion_p = t.current + (get_min_remaining_process_time(server_p.jobs) * server_p.number)

        # completion_a
        elif t.current == t.completion_a:
            processed_time = get_min_remaining_process_time(server_a.jobs)
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

            if completed_job.job_type == JobType.A2:
                t.arrival_p = t.current

            if server_a.number > 0:
                t.completion_a = t.current + get_min_remaining_process_time(server_a.jobs) * server_a.number
            else:
                t.completion_a = INFINITY

        # completion_b
        elif t.current == t.completion_b:
            processed_time = get_min_remaining_process_time(server_b.jobs)

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
                t.completion_b = t.current + get_min_remaining_process_time(server_b.jobs) * server_b.number
            else:
                t.completion_b = INFINITY

        # completion_p
        elif t.current == t.completion_p:
            processed_time = get_min_remaining_process_time(server_p.jobs)

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
                t.completion_p = t.current + get_min_remaining_process_time(server_p.jobs) * server_p.number
            else:
                t.completion_p = INFINITY

    interarrival_a = t.last / server_a.index
    avg_service_a = server_a.area.node / server_a.index
    avg_population_a = server_a.area.node / t.current
    utilization_a = server_a.area.service / t.current

    interarrival_b = t.last / server_b.index
    avg_service_b = server_b.area.node / server_b.index
    avg_population_b = server_b.area.node / t.current
    utilization_b = server_b.area.service / t.current

    interarrival_p = t.last / server_p.index
    avg_service_p = server_p.area.node / server_p.index
    avg_population_p = server_p.area.node / t.current
    utilization_p = server_p.area.service / t.current

    # print("Server A statistics")
    # print("for {0} jobs".format(server_a.index))
    # print("\taverage interarrival time = {0:6.6f}".format(t.last / server_a.index))
    # print("\taverage service time .... = {0:6.6f}".format(server_a.area.node / server_a.index))
    # print("\taverage # in the node ... = {0:6.6f}".format(server_a.area.node / t.current))
    # print("\tutilization ............. = {0:6.6f}".format(server_a.area.service / t.current))
    #
    # print("Server B statistics")
    # print("for {0} jobs".format(server_b.index))
    # print("\taverage interarrival time = {0:6.6f}".format(t.last / server_b.index))
    # print("\taverage service time .... = {0:6.6f}".format(server_b.area.node / server_b.index))
    # print("\taverage # in the node ... = {0:6.6f}".format(server_b.area.node / t.current))
    # print("\tutilization ............. = {0:6.6f}".format(server_b.area.service / t.current))
    #
    # print("Server P statistics")
    # print("for {0} jobs".format(server_p.index))
    # print("\taverage interarrival time = {0:6.6f}".format(t.last / server_p.index))
    # print("\taverage service time .... = {0:6.6f}".format(server_p.area.node / server_p.index))
    # print("\taverage # in the node ... = {0:6.6f}".format(server_p.area.node / t.current))
    # print("\tutilization ............. = {0:6.6f}".format(server_p.area.service / t.current))
    #
    avg_response_time = server_a.area.node / server_a.index + server_b.area.node / server_b.index + server_p.area.node / server_p.index
    avg_population = server_a.area.node / t.current + server_b.area.node / t.current + server_p.area.node / t.current
    # print()
    # print("Average Response Time = {0:6.6f}".format(avg_response_time))
    # print("Average Population = {0:6.6f}".format(avg_population))

    return [interarrival_a, avg_service_a, avg_population_a, utilization_a, server_a.index,
            interarrival_b, avg_service_b, avg_population_b, utilization_b, server_b.index,
            interarrival_p, avg_service_p, avg_population_p, utilization_p, server_p.index,
            avg_response_time, avg_population]


def main():
    seed = 123456789
    start = datetime.now()
    with open('data.csv', 'w', newline='') as csvfile:
        fieldnames = ['seed', 'auth', 'arrival_rate',
                      'interarrival_a', 'avg_service_a', 'avg_population_a', 'utilization_a', 'completion_a',
                      'interarrival_b', 'avg_service_b', 'avg_population_b', 'utilization_b', 'completion_b',
                      'interarrival_p', 'avg_service_p', 'avg_population_p', 'utilization_p', 'completion_p',
                      'avg_response_time', 'avg_population']
        writer = csv.writer(csvfile)
        writer.writerow(fieldnames)

        auth_types = [1, 2]
        arrival_rates = [0.5, 0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95, 1.0, 1.05, 1.1, 1.15, 1.2]
        for _ in range(0, 5):
            plant_seeds(seed)
            for auth in auth_types:
                for arrival_rate in arrival_rates:
                    print(f"Simulate seed {seed}, arrival_rate {arrival_rate} and auth type {auth}")
                    data = [seed, auth, arrival_rate]
                    data += model(arrival_rate, auth)
                    print()
                    writer.writerow(data)
            seed = get_seed()

    end = datetime.now()

    print(f"Simulation time: {end - start}")


if __name__ == "__main__":
    main()
