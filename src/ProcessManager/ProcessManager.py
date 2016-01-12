__author__ = 'tmy'


class ProcessManager():
    def __init__(self, n_processes):
        self.__max_processes = n_processes
        self.processes = []

    def has_free_process_slot(self):
        self.processes = [x for x in self.processes if x.is_alive()]
        return len(self.processes) < self.__max_processes

    def add(self, process):
        if self.has_free_process_slot():
            self.processes.append(process)
        else:
            raise (OccupiedError(process))


class OccupiedError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return "Process manager is full: " + str(self.value)
