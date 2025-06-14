from enum import Enum

from rngs import select_stream, plant_seeds
from rvgs import exponential

START = 0.0  # initial time                   
STOP = 3000.0  # terminal time
INFINITY = (100.0 * STOP)  # must be much larger than STOP  
arrivalTemp = START


def get_arrival():
    # ---------------------------------------------
    # generate the next arrival_a time, with rate 1.2
    # ---------------------------------------------

    global arrivalTemp

    select_stream(0)
    arrivalTemp += exponential(1.2)
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
        select_stream(1)
        rate = 0.2
    elif job_type == JobType.A2:
        select_stream(2)
        rate = 0.4
    elif job_type == JobType.A3:
        select_stream(3)
        rate = 0.1
    elif job_type == JobType.B:
        select_stream(4)
        rate = 0.8
    elif job_type == JobType.P:
        select_stream(5)
        rate = 0.4
    return exponential(rate)


class Track:
    node = 0.0  # time integrated number in the node  
    queue = 0.0  # time integrated number in the queue 
    service = 0.0  # time integrated number in service   


class Time:
    arrival_a1 = -1  # next arrival_a time
    arrival_a2 = -1
    arrival_b = -1  # next arrival_b time
    completion_a = -1  # next completion time on server A
    completion_b = -1  # next completion time on server A
    completion_p = -1  # next completion time on server A
    current = -1  # current time                        
    next = -1  # next (most imminent) event time     
    last = -1  # last arrival_a time                   


class Job:
    def __init__(self, arrival, job_type):
        self.arrival = arrival
        self.remaining = get_service(job_type)
        self.job_type = job_type
        self.last_event = arrival


def get_min_remaining_process_time(jobs):
    min_remaining = jobs[0].remaining
    for job in jobs:
        if job.remaining < min_remaining:
            min_remaining = job.remaining
    return min_remaining


def is_there_a_completion(jobs, process_time):
    _min = jobs[0].remaining
    for job in jobs:
        if job.remaining <= _min:
            _min = job.remaining
    if _min <= process_time:
        return True, _min
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

    plant_seeds(123456789)

    jobs_a = []
    jobs_b = []
    jobs_p = []
    t.current = START  # set the clock                         
    t.arrival_a1 = get_arrival()  # schedule the first arrival_a
    t.arrival_a2 = INFINITY
    t.arrival_b = INFINITY
    t.completion_a = INFINITY
    t.completion_b = INFINITY
    t.completion_p = INFINITY

    while t.arrival_a1 < STOP or t.arrival_a2 != INFINITY or t.arrival_b != INFINITY or number_a > 0 or number_b > 0:
        t.next = min(t.arrival_a1, t.arrival_a2, t.arrival_b, t.completion_a, t.completion_b, t.completion_p)  # next event time

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
        # arrival_a1
        if t.current == t.arrival_a1:
            if t.completion_a > t.arrival_a1 and t.completion_a != INFINITY and number_a > 0:
                processed_time = (t.current - jobs_a[-1].last_event) / number_a
                for job in jobs_a:
                    job.remaining -= processed_time
                    job.last_event = t.current

            jobs_a.append(Job(t.arrival_a1, JobType.A1))
            number_a += 1
            t.arrival_a1 = get_arrival()

            if t.arrival_a1 > STOP:
                t.last = t.current
                t.arrival_a1 = INFINITY
                process_time = get_min_remaining_process_time(jobs_a) * number_a
            else:
                process_time = (t.arrival_a1 - t.current) / number_a

            complete, remaining = is_there_a_completion(jobs_a, process_time)

            if complete:
                t.completion_a = t.current + remaining
            else:
                t.completion_a = INFINITY

        elif t.current == t.arrival_a2:
            if t.completion_a > t.arrival_a2 and t.completion_a != INFINITY and number_a > 0:
                processed_time = (t.current - jobs_a[-1].last_event) / number_a
                for job in jobs_a:
                    job.remaining -= processed_time
                    job.last_event = t.current

            jobs_a.append(Job(t.arrival_a1, JobType.A2))
            number_a += 1

            t.arrival_a2 = INFINITY

            process_time = get_min_remaining_process_time(jobs_a) * number_a

            complete, remaining = is_there_a_completion(jobs_a, process_time)

            if complete:
                t.completion_a = t.current + remaining
            else:
                t.completion_a = INFINITY


        elif t.current == t.arrival_b: # process arrival_b
            print("Process arrival_b")
            print(f"Job in B: {number_b}")

            if t.completion_b > t.arrival_b and t.completion_b != INFINITY:
                processed_time = (t.current - jobs_b[-1].last_event) / number_b
                for job in jobs_b:
                    job.remaining -= processed_time
                    job.last_event = t.current

            jobs_b.append(Job(t.arrival_b, JobType.B))
            number_b += 1

            t.arrival_b = INFINITY

            process_time = get_min_remaining_process_time(jobs_b) * number_b

            complete, remaining = is_there_a_completion(jobs_b, process_time)

            if complete:
                t.completion_b = t.current + remaining
                process_time = remaining
            else:
                t.completion_b = INFINITY

        elif t.current == t.completion_a:
            print("Process completion_a")
            print(f"Job in A: {number_b}")

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
            
            if completed_job.job_type == JobType.A1:
                index_a += 1
                number_a -= 1
                t.arrival_b = t.current
                
            if completed_job.job_type == JobType.A2:
                index_a += 1
                number_a -= 1
                #jobs_b.append(Job(t.current, JobType.P))
                #number_p += 1
                
            if completed_job.job_type == JobType.A3:
                index_a += 1
                number_a -= 1
        
            if number_a > 0:
                if t.arrival_a1 > STOP or t.arrival_a1 == INFINITY:
                    process_time = get_min_remaining_process_time(jobs_a) * number_a
                else:
                    process_time = (t.arrival_a1 - t.current) / number_a

                complete, remaining = is_there_a_completion(jobs_a, process_time)

                if complete:
                    t.completion_a = t.current + remaining
                    process_time = remaining
                else:
                    t.completion_a = INFINITY

            else:
                t.completion_a = INFINITY

        # completion B
        elif t.current == t.completion_b:
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
                    process_time = get_min_remaining_process_time(jobs_b) * number_b
                else:
                    process_time = (t.arrival_b - t.current) / number_b

                complete, remaining = is_there_a_completion(jobs_b, process_time)

                if complete:
                    t.completion_b = t.current + remaining
                else:
                    t.completion_b = INFINITY
            else:
                t.completion_b = INFINITY



    print("Server A statistics")
    print("for {0} jobs".format(index_a))
    print("\taverage interarrival time = {0:6.2f}".format(t.last / index_a))
    print("\taverage wait ............ = {0:6.2f}".format(area_a.node / index_a))
    print("\taverage delay ........... = {0:6.2f}".format(area_a.queue / index_a))
    print("\taverage service time .... = {0:6.2f}".format(area_a.service / index_a))
    print("\taverage # in the node ... = {0:6.2f}".format(area_a.node / t.current))
    print("\taverage # in the queue .. = {0:6.2f}".format(area_a.queue / t.current))
    print("\tutilization ............. = {0:6.2f}".format(area_a.service / t.current))

    print("Server B statistics")
    print("for {0} jobs".format(index_b))
    print("\taverage interarrival time = {0:6.2f}".format(t.last / index_b))
    print("\taverage wait ............ = {0:6.2f}".format(area_b.node / index_b))
    print("\taverage delay ........... = {0:6.2f}".format(area_b.queue / index_b))
    print("\taverage service time .... = {0:6.2f}".format(area_b.service / index_b))
    print("\taverage # in the node ... = {0:6.2f}".format(area_b.node / t.current))
    print("\taverage # in the queue .. = {0:6.2f}".format(area_b.queue / t.current))
    print("\tutilization ............. = {0:6.2f}".format(area_b.service / t.current))


if __name__ == "__main__":
    main()
