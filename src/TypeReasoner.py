__author__ = 'tmy'

import os
from datetime import datetime
from multiprocessing import Process
import time

from .RemoteSparql.PymanticRemoteSparql import PymanticRemoteSparql
from .RemoteSparql.RequestsRemoteSparql import RequestsRemoteSparql
from .Worker.Worker import materialize_reason
from .NtripleParser.NtripleParser import NtripleParser
from .ProcessManager.ProcessManager import ProcessManager
from .ProcessManager.ProcessManager import OccupiedError
from .Utilities.Logger import log
from .Utilities.Utilities import log_progress


PYMANTIC_SUPPORTED_ENDPOINTS = []


class TypeReasoner(object):
    def __init__(self, server, user, password, n_processes, log_level):
        log.setLevel(log_level)
        self.subjectParser = NtripleParser(" ")
        self.processManager = ProcessManager(n_processes)

        # Create server instance
        if any([x in server for x in PYMANTIC_SUPPORTED_ENDPOINTS]):
            self.__server = PymanticRemoteSparql(server)
        else:
            self.__server = RequestsRemoteSparql(server, user, password)

    def reason(self, file, target):
        # Make directory
        if not os.path.exists(target):
            os.makedirs(target)

        # Iterate through file
        with open(file) as input_file:
            target_file = target + str(file).split("/")[-1][:-3] + str("_reasoned.nt")
            tmp_subject = ""
            cur_time = datetime.now()
            types = set([])
            for line_num, line in enumerate(input_file):
                triple = self.subjectParser.get_triple(line)
                if not triple:
                    continue
                log_progress(line_num, 100)

                # Todo Event based?
                # Check every 0.1 seconds if we can continue
                while not self.processManager.has_free_process_slot():
                    time.sleep(0.1)

                if not triple["subject"] == tmp_subject:
                    p = Process(target=materialize_reason,
                                kwargs=dict(instance=tmp_subject, types=types, target=target_file,
                                            server=self.__server))
                    p.daemon = True
                    try:
                        self.processManager.add(p)
                    except OccupiedError as e:
                        return 2
                    else:
                        p.start()
                        tmp_subject = triple["subject"]
                        # create a set
                        types = {triple["object"]}
                else:
                    types.add(triple["object"])

        log.info("Done in: " + str(datetime.now() - cur_time))

