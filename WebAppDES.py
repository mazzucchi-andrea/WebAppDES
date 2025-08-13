import csv
from enum import Enum

from rngs import select_stream, plant_seeds
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
    arrival_a1 = -1  # next arrival time for jobs of type A1
    arrival_a2 = -1  # next arrival time for jobs of type A2
    arrival_a3 = -1  # next arrival time for jobs of type A3
    arrival_b = -1  # next arrival_b time
    arrival_p = -1
    completion_a = -1  # next completion time on server A
    completion_b = -1  # next completion time on server B
    completion_p = -1  # next completion time on server P
    current = -1  # current time
    next = -1  # next (most imminent) event time
    last = -1  # last arrival_a time


class Job:
    def __init__(self, arrival, job_type, auth):
        self.arrival = arrival
        self.remaining = get_service(job_type, auth)
        self.job_type = job_type
        self.last_event = arrival


def get_min_remaining_process_time(jobs):
    min_job = min(jobs, key=lambda job: job.remaining)
    return min_job.remaining


def is_there_a_completion(jobs, process_time):
    min_job = min(jobs, key=lambda job: job.remaining)
    min_remaining = min_job.remaining
    if min_remaining <= process_time:
        return True, min_remaining
    return False, 0


def model(seed, arrival_rate, auth):
    global arrivalTemp
    arrivalTemp = START

    index_a = 0  # used to count departed jobs
    index_b = 0
    index_p = 0

    number_a = 0  # number in the node
    number_b = 0
    number_p = 0

    area_a = Track()
    area_b = Track()
    area_p = Track()

    t = Time()

    plant_seeds(seed)

    jobs_a = []
    jobs_b = []
    jobs_p = []

    t.current = START  # set the clock
    t.arrival_a1 = get_arrival(arrival_rate)  # schedule the first arrival_a
    t.arrival_a2 = INFINITY
    t.arrival_a3 = INFINITY
    t.arrival_b = INFINITY
    t.arrival_p = INFINITY
    t.completion_a = INFINITY
    t.completion_b = INFINITY
    t.completion_p = INFINITY

    while (t.arrival_a1 < STOP or t.arrival_a2 != INFINITY or t.arrival_a3 != INFINITY or t.arrival_b != INFINITY or
           t.arrival_p != INFINITY or number_a > 0 or number_b > 0 or number_p > 0):

        t.next = min(t.arrival_a1, t.arrival_a2, t.arrival_a3, t.arrival_b, t.arrival_p, t.completion_a, t.completion_b,
                     t.completion_p)  # next event time

        # update integrals
        if number_a > 0:
            area_a.update(t.current, t.next, number_a)
        if number_b > 0:
            area_b.update(t.current, t.next, number_b)
        if number_p > 0:
            area_p.update(t.current, t.next, number_p)

        t.current = t.next  # advance the clock

        # arrival_a1
        if t.current == t.arrival_a1:
            # print(f"Arrival a1, number_a = {number_a}")
            if number_a > 0:
                processed_time = (t.current - jobs_a[-1].last_event) / number_a
                for job in jobs_a:
                    job.remaining -= processed_time
                    job.last_event = t.current
                    if job.remaining <= 0:
                        print("Something went wrong")
                        return []

            jobs_a.append(Job(t.arrival_a1, JobType.A1, auth))
            number_a += 1

            t.arrival_a1 = get_arrival(arrival_rate)

            if t.arrival_a1 > STOP:
                t.last = t.current
                t.arrival_a1 = INFINITY
                service_time = min(get_min_remaining_process_time(jobs_a) * number_a,
                                   (min(t.completion_b, t.completion_p) - t.current) * number_a)
            else:
                service_time = (min(t.arrival_a1, t.completion_b, t.completion_p) - t.current) * number_a

            complete, remaining = is_there_a_completion(jobs_a, service_time)

            if complete:
                t.completion_a = t.current + remaining * number_a
            else:
                t.completion_a = INFINITY

        # arrival_a2
        elif t.current == t.arrival_a2:
            # print(f"Arrival a2, number_a = {number_a}")
            if number_a > 0:
                processed_time = (t.current - jobs_a[-1].last_event) / number_a
                for job in jobs_a:
                    job.remaining -= processed_time
                    job.last_event = t.current
                    if job.remaining <= 0:
                        print("Something went wrong")
                        return []

            jobs_a.append(Job(t.arrival_a2, JobType.A2, auth))
            number_a += 1

            t.arrival_a2 = INFINITY

            service_time = get_min_remaining_process_time(jobs_a) * number_a

            complete, remaining = is_there_a_completion(jobs_a, service_time)

            if complete:
                t.completion_a = t.current + remaining * number_a
            else:
                t.completion_a = INFINITY

        # arrival_a3
        elif t.current == t.arrival_a3:
            # print(f"Arrival a3, number_a = {number_a}")
            if number_a > 0:
                processed_time = (t.current - jobs_a[-1].last_event) / number_a
                for job in jobs_a:
                    job.remaining -= processed_time
                    job.last_event = t.current
                    if job.remaining <= 0:
                        print("Something went wrong")
                        return []

            jobs_a.append(Job(t.arrival_a3, JobType.A3, auth))
            number_a += 1

            t.arrival_a3 = INFINITY

            service_time = get_min_remaining_process_time(jobs_a) * number_a

            complete, remaining = is_there_a_completion(jobs_a, service_time)

            if complete:
                t.completion_a = t.current + remaining * number_a
            else:
                t.completion_a = INFINITY

        # arrival_b
        elif t.current == t.arrival_b:
            # print(f"Arrival b, number_b = {number_b}")
            if number_b > 0:
                processed_time = (t.current - jobs_b[-1].last_event) / number_b
                for job in jobs_b:
                    job.remaining -= processed_time
                    job.last_event = t.current

            jobs_b.append(Job(t.arrival_b, JobType.B, auth))
            number_b += 1

            t.arrival_b = INFINITY

            t.completion_b = t.current + (get_min_remaining_process_time(jobs_b) * number_b)


        # arrival_p
        elif t.current == t.arrival_p:
            # print(f"Arrival p, number_p = {number_p}")
            if number_p > 0:
                processed_time = (t.current - jobs_p[-1].last_event) / number_p
                for job in jobs_p:
                    job.remaining -= processed_time
                    job.last_event = t.current

            jobs_p.append(Job(t.arrival_p, JobType.P, auth))
            number_p += 1

            t.arrival_p = INFINITY

            t.completion_p = t.current + (get_min_remaining_process_time(jobs_p) * number_p)

        # completion_a
        elif t.current == t.completion_a:
            # print(f"Completion a, number_a = {number_a}")
            processed_time = get_min_remaining_process_time(jobs_a)
            completed_job = None

            for job in jobs_a:
                job.remaining -= processed_time
                job.last_event = t.current

            for job in jobs_a:
                if job.remaining == 0:
                    completed_job = job
                    jobs_a.remove(job)
                    break

            index_a += 1
            number_a -= 1

            if completed_job.job_type == JobType.A1:
                t.arrival_b = t.current

            if completed_job.job_type == JobType.A2:
                t.arrival_p = t.current

            if number_a > 0:
                service_time = get_min_remaining_process_time(jobs_a) * number_a
                complete, remaining = is_there_a_completion(jobs_a, service_time)

                if complete:
                    t.completion_a = t.current + remaining * number_a
                else:
                    t.completion_a = INFINITY

            else:
                t.completion_a = INFINITY

        # completion_b
        elif t.current == t.completion_b:
            # print(f"Completion b, number_b = {number_b}")
            processed_time = get_min_remaining_process_time(jobs_b)

            for job in jobs_b:
                job.remaining -= processed_time
                job.last_event = t.current

            for job in jobs_b:
                if job.remaining == 0:
                    jobs_b.remove(job)
                    break

            index_b += 1
            number_b -= 1

            t.arrival_a2 = t.current

            if number_b > 0:
                if t.arrival_b == INFINITY:
                    service_time = get_min_remaining_process_time(jobs_b) * number_b
                else:
                    service_time = (t.arrival_b - t.current) * number_b

                complete, remaining = is_there_a_completion(jobs_b, service_time)

                if complete:
                    t.completion_b = t.current + remaining * number_b
                else:
                    t.completion_b = INFINITY
            else:
                t.completion_b = INFINITY

        # completion_p
        elif t.current == t.completion_p:
            # print(f"Completion p, number_p = {number_p}")
            processed_time = get_min_remaining_process_time(jobs_p)

            for job in jobs_p:
                job.remaining -= processed_time
                job.last_event = t.current

            for job in jobs_p:
                if job.remaining == 0:
                    jobs_p.remove(job)
                    break

            index_p += 1
            number_p -= 1

            t.arrival_a3 = t.current

            if number_p > 0:
                if t.arrival_p == INFINITY:
                    service_time = get_min_remaining_process_time(jobs_p) * number_p
                else:
                    service_time = (t.arrival_p - t.current) * number_p

                complete, remaining = is_there_a_completion(jobs_p, service_time)

                if complete:
                    t.completion_p = t.current + remaining * number_p
                else:
                    t.completion_p = INFINITY
            else:
                t.completion_p = INFINITY

    interarrival_a = t.last / index_a
    avg_service_a = area_a.node / index_a
    avg_population_a = area_a.node / t.current
    utilization_a = area_a.service / t.current

    interarrival_b = t.last / index_b
    avg_service_b = area_b.node / index_b
    avg_population_b = area_b.node / t.current
    utilization_b = area_b.service / t.current

    interarrival_p = t.last / index_p
    avg_service_p = area_p.node / index_p
    avg_population_p = area_p.node / t.current
    utilization_p = area_p.service / t.current

    print("Server A statistics")
    print("for {0} jobs".format(index_a))
    print("\taverage interarrival time = {0:6.6f}".format(t.last / index_a))
    print("\taverage service time .... = {0:6.6f}".format(area_a.node / index_a))
    print("\taverage # in the node ... = {0:6.6f}".format(area_a.node / t.current))
    print("\tutilization ............. = {0:6.6f}".format(area_a.service / t.current))

    print("Server B statistics")
    print("for {0} jobs".format(index_b))
    print("\taverage interarrival time = {0:6.6f}".format(t.last / index_b))
    print("\taverage service time .... = {0:6.6f}".format(area_b.node / index_b))
    print("\taverage # in the node ... = {0:6.6f}".format(area_b.node / t.current))
    print("\tutilization ............. = {0:6.6f}".format(area_b.service / t.current))

    print("Server P statistics")
    print("for {0} jobs".format(index_p))
    print("\taverage interarrival time = {0:6.6f}".format(t.last / index_p))
    print("\taverage service time .... = {0:6.6f}".format(area_p.node / index_p))
    print("\taverage # in the node ... = {0:6.6f}".format(area_p.node / t.current))
    print("\tutilization ............. = {0:6.6f}".format(area_p.service / t.current))

    avg_response_time = area_a.node / index_a + area_b.node / index_b + area_p.node / index_p
    avg_population = area_a.node / t.current + area_b.node / t.current + area_p.node / t.current
    print()
    print("Average Response Time = {0:6.6f}".format(avg_response_time))
    print("Average Population = {0:6.6f}".format(avg_population))

    return [interarrival_a, avg_service_a, avg_population_a, utilization_a, index_a,
            interarrival_b, avg_service_b, avg_population_b, utilization_b, index_b,
            interarrival_p, avg_service_p, avg_population_p, utilization_p, index_p,
            avg_response_time, avg_population]


def main():
    with open('data.csv', 'w', newline='') as csvfile:
        fieldnames = ['seed', 'auth', 'arrival_rate',
                      'interarrival_a', 'avg_service_a', 'avg_population_a', 'utilization_a', 'completion_a',
                      'interarrival_b', 'avg_service_b', 'avg_population_b', 'utilization_b', 'completion_b',
                      'interarrival_p', 'avg_service_p', 'avg_population_p', 'utilization_p', 'completion_p',
                      'avg_response_time', 'avg_population']
        writer = csv.writer(csvfile)
        writer.writerow(fieldnames)

        auth_types = [1, 2]
        seeds = [123456789, 987654321, 1593574826, 7539514862, 765555555]
        arrival_rates = [0.5, 0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95, 1.0, 1.05, 1.1, 1.15, 1.2]
        for auth in auth_types:
            for seed in seeds:
                for arrival_rate in arrival_rates:
                    print(f"Simulate seed {seed}, arrival_rate {arrival_rate} and auth type {auth}")
                    data = [seed, auth, arrival_rate]
                    data += model(seed, arrival_rate, auth)
                    print()
                    writer.writerow(data)


if __name__ == "__main__":
    main()
