import csv
from datetime import datetime
from enum import Enum


from rngs import select_stream, plant_seeds, get_seed
from rvgs import exponential

ALPHA = 0.05
START = 0.0  # initial time
STOP = 86400.0  # terminal time
INFINITY = (100.0 * STOP)  # must be much larger than STOP
arrivalTemp = START
CAMP_INTERVAL = 300


def get_arrival(arrival_rate):
    global arrivalTemp

    select_stream(0)
    arrivalTemp += exponential(1.0 / arrival_rate)
    return arrivalTemp


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


class Time:
    arrival_a1 = INFINITY  # next arrival time for jobs of type A1
    arrival_p = INFINITY
    completion_a = INFINITY  # next completion time on server A
    completion_b = INFINITY  # next completion time on server B
    completion_p = INFINITY  # next completion time on server P
    current = INFINITY  # current time
    next = INFINITY  # next (most imminent) event time
    last = INFINITY  # last arrival_a time
    camp = INFINITY


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


def model(arrival_rate,  writer, auth, b_improvement=False,):
    global arrivalTemp
    arrivalTemp = START

    server_a = Server()
    server_b = Server()
    server_p = Server()

    t = Time()

    t.current = START  # set the clock
    t.arrival_a1 = get_arrival(arrival_rate)  # schedule the first arrival_a
    t.camp = CAMP_INTERVAL

    while t.arrival_a1 < STOP or server_a.number > 0 or server_b.number > 0 or server_p.number > 0:

        t.next = min(t.arrival_a1, t.completion_a, t.completion_b, t.completion_p, t.camp)  # next event time

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
            server_a.process_arrival(Job(t.arrival_a1, JobType.A1))

            t.arrival_a1 = get_arrival(arrival_rate)
            if t.arrival_a1 > STOP:
                t.last = t.current
                t.arrival_a1 = INFINITY
            t.completion_a = t.current + server_a.get_next_complete_process_time()

        # completion_a
        elif t.current == t.completion_a:
            completed_job = server_a.process_completion(t.completion_a)

            if completed_job.job_type == JobType.A1:
                server_b.process_arrival(Job(t.completion_a, JobType.B, b_improvement=b_improvement))
                t.completion_b = t.current + server_b.get_next_complete_process_time()
            elif completed_job.job_type == JobType.A2:
                server_p.process_arrival(Job(t.current, JobType.P, auth))
                t.completion_p = t.current + server_p.get_next_complete_process_time()

            if server_a.number > 0:
                t.completion_a = t.current + server_a.get_next_complete_process_time()
            else:
                t.completion_a = INFINITY

        # completion_b
        elif t.current == t.completion_b:
            server_b.process_completion(t.completion_b)
            server_a.process_arrival(Job(t.current, JobType.A2))
            t.completion_a = t.current + server_a.get_next_complete_process_time()

            if server_b.number > 0:
                t.completion_b = t.current + server_b.get_next_complete_process_time()
            else:
                t.completion_b = INFINITY

        # completion_p
        elif t.current == t.completion_p:
            server_p.process_completion(t.completion_p)

            server_a.process_arrival(Job(t.current, JobType.A3, auth))
            t.completion_a = t.current + server_a.get_next_complete_process_time()

            if server_p.number > 0:
                t.completion_p = t.current + server_p.get_next_complete_process_time()
            else:
                t.completion_p = INFINITY

        elif t.current == t.camp:
            writer.writerow((t.current, server_a.avg_service, server_b.avg_service, server_p.avg_service, 3 * server_a.avg_service + server_b.avg_service + server_p.avg_service))
            t.camp += CAMP_INTERVAL


def get_simulation_statistics(server_a, server_b, server_p, current_time):
    avg_population_a = server_a.area.node / current_time
    utilization_a = server_a.area.service / current_time
    avg_population_b = server_b.area.node / current_time
    utilization_b = server_b.area.service / current_time
    avg_population_p = server_p.area.node / current_time
    utilization_p = server_p.area.service / current_time
    avg_response_time = 3 * server_a.avg_service + server_b.avg_service + server_p.avg_service
    avg_population = server_a.area.node / current_time + server_b.area.node / current_time + server_p.area.node / current_time
    return (avg_population,
            avg_population_a, avg_population_b, avg_population_p,
            avg_response_time,
            utilization_a, utilization_b, utilization_p)


def obj_1_2_finite_horizon_simulation():
    global STOP

    start = datetime.now()
    seed = 123456789
    auth_types = [1, 2]
    arrival_rates = [0.5, 0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95, 1.0, 1.05, 1.1, 1.15, 1.2]
    print("Start Finite Horizon Simulation")
    for _ in range(0, 8):
        for auth in auth_types:
            for b_improvement in [True, False]:
                for arrival_rate in arrival_rates:
                    with open(f'conv/data_{seed}_{arrival_rate}_{auth}_{b_improvement}_conv.csv', 'w', newline='') as csvfile:
                        fieldnames = ['time', 'avg_service_a', 'avg_service_b', 'avg_service_p', 'avg_service']
                        writer = csv.writer(csvfile)
                        writer.writerow(fieldnames)
                        plant_seeds(seed)
                        print(f"Finite Horizon: seed {seed}, arrival_rate {arrival_rate},  auth type {auth}, b improvement {b_improvement}")
                        model(arrival_rate, writer, auth, b_improvement)
        seed = get_seed()


    end = datetime.now()

    print(f"Finite Horizon Simulation time: {end - start}\n")


def main():
    start = datetime.now()

    obj_1_2_finite_horizon_simulation()

    end = datetime.now()
    print(f"Total Simulation time: {end - start}\n")


if __name__ == "__main__":
    main()
