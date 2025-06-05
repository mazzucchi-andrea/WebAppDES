from enum import Enum

from rngs import selectStream, plantSeeds
from rvgs import Exponential

START = 0.0  # initial time                   
STOP = 20000.0  # terminal time
INFINITY = (100.0 * STOP)  # must be much larger than STOP  
arrivalTemp = START


def get_arrival():
    # ---------------------------------------------
    # generate the next arrival time, with rate 1.2
    # ---------------------------------------------

    global arrivalTemp

    selectStream(0)
    arrivalTemp += Exponential(1.2)
    return arrivalTemp


class JobType(Enum):
    A1 = 1
    A2 = 2
    A3 = 3
    B = 4
    P = 5


def get_service(job_type):
    rate = -1
    if job_type == JobType.A1:
        selectStream(1)
        rate = 0.2
    elif job_type == JobType.A2:
        selectStream(2)
        rate = 0.4
    elif job_type == JobType.A3:
        selectStream(3)
        rate = 0.1
    elif job_type == JobType.B:
        selectStream(4)
        rate = 0.8
    elif job_type == JobType.P:
        selectStream(5)
        rate = 0.4
    return Exponential(rate)


class Track:
    node = 0.0  # time integrated number in the node  
    queue = 0.0  # time integrated number in the queue 
    service = 0.0  # time integrated number in service   


class Time:
    arrival = -1  # next arrival time                   
    completion = -1  # next completion time                
    current = -1  # current time                        
    next = -1  # next (most imminent) event time     
    last = -1  # last arrival time                   


class Job:
    def __init__(self, arrival, job_type):
        self.arrival = arrival
        self.remaining = get_service(job_type)
        self.job_type = job_type


def get_min_remaining_process_time(jobs):
    min_remaining = jobs[0].remaining
    for job in jobs:
        if job.remaining < min_remaining:
            min_remaining = job.remaining
    return min_remaining


def is_there_a_completion(jobs, process_time):
    for job in jobs:
        if job.remaining <= process_time:
            return True, job.remaining
    return False, 0


def main():
    global arrivalTemp

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

    plantSeeds(123456789)

    jobs_a = []
    jobs_b = []
    jobs_p = []
    t.current = START  # set the clock                         
    t.arrival = get_arrival()  # schedule the first arrival            
    t.completion = INFINITY

    while (t.arrival < STOP) or (number_a > 0):
        t.next = min(t.arrival, t.completion)  # next event time   

        if number_a > 0:  # update integrals
            area_a.node += (t.next - t.current) * number_a
            area_a.queue += (t.next - t.current) * (number_a - 1)
            area_a.service += (t.next - t.current)

        if number_b > 0:
            area_b.node += (t.next - t.current) * number_b
            area_b.queue += (t.next - t.current) * (number_b - 1)
            area_b.service += (t.next - t.current)

        if number_p > 0:
            area_p.node += (t.next - t.current) * number_p
            area_p.queue += (t.next - t.current) * (number_p - 1)
            area_p.service += (t.next - t.current)

        t.current = t.next  # advance the clock
        if t.current == t.arrival:  # process an arrival
            print("Process arrival")
            jobs_a.append(Job(t.arrival, JobType.A1))
            number_a += 1
            print(f"Job arrival at {t.arrival} with service time {jobs_a[number_a - 1].remaining}")
            print(f"Job in center: {number_a}")
            t.arrival = get_arrival()
            print(f"Next arrival at {t.arrival}")

            if t.arrival > STOP:
                t.last = t.current
                t.arrival = INFINITY
                process_time = get_min_remaining_process_time(jobs_a) * number_a
            else:
                process_time = (t.arrival - t.current) / number_a

            complete, remaining = is_there_a_completion(jobs_a, process_time)

            if complete:
                t.completion = t.current + remaining
                print(f"Completion at {t.completion}")
                process_time = remaining
            else:
                t.completion = INFINITY

            for job in jobs_a:
                job.remaining -= process_time

        else:
            print("Process completion")
            index_a += 1
            number_a -= 1
            print(f"Job completed at {t.completion}")
            print(f"Job in center: {number_a}")

            completed_job = None
            for job in jobs_a:
                if job.remaining == 0:
                    completed_job = job
                    jobs_a.remove(job)
                    break
            if completed_job.job_type == JobType.A1:
                jobs_b.append(Job(t.current, JobType.B))
                number_b += 1

            if number_a > 0:
                if t.arrival > STOP or t.arrival == INFINITY:
                    process_time = get_min_remaining_process_time(jobs_a) * number_a
                else:
                    process_time = (t.arrival - t.current) / number_a

                complete, remaining = is_there_a_completion(jobs_a, process_time)

                if complete:
                    t.completion = t.current + remaining
                    process_time = remaining
                else:
                    t.completion = INFINITY

                for job in jobs_a:
                    job.remaining -= process_time
            else:
                t.completion = INFINITY

    print("Server A statistics")
    print("for {0} jobs".format(index_a))
    print("\taverage interarrival time = {0:6.2f}".format(t.last / index_a))
    print("\taverage wait ............ = {0:6.2f}".format(area_a.node / index_a))
    print("\taverage delay ........... = {0:6.2f}".format(area_a.queue / index_a))
    print("\taverage service time .... = {0:6.2f}".format(area_a.service / index_a))
    print("\taverage # in the node ... = {0:6.2f}".format(area_a.node / t.current))
    print("\taverage # in the queue .. = {0:6.2f}".format(area_a.queue / t.current))
    print("\tutilization ............. = {0:6.2f}".format(area_a.service / t.current))


if __name__ == "__main__":
    main()
