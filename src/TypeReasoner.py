__author__ = 'tmy'

import os
from datetime import datetime
from multiprocessing import Process
import time

from .Materializer.Materializer import materialize_to_file, materialize_to_service
from .NTripleLineParser.src.NTripleLineParser import NTripleLineParser
from .SparqlInterface.src import ClientFactory
from .ProcessManager.ProcessManager import ProcessManager
from .ProcessManager.ProcessManager import OccupiedError
from .Utilities.Logger import log
from .Utilities.Utilities import log_progress


class TypeReasoner(object):

    def __init__(self, server, user, password, n_processes, log_level):
        log.setLevel(log_level)
        self.nt_parser = NTripleLineParser(" ")
        if n_processes:
            self.processManager = ProcessManager(n_processes)
        self.__server = ClientFactory.make_client(server=server, user=user, password=password)

    def reason(self, in_file=None, target="./reasoned/", in_service=False):
        if in_service:
            target = None
        else:
            # Make directory
            if not os.path.exists(target):
                os.makedirs(target)

        cur_time = datetime.now()
        if in_file:
            log.info("Reasoning from file")
            self.__reason_from_file(in_file, target)
        else:
            log.info("Reasoning from service")
            self.__reason_from_service(target)
        log.info("Done in: " + str(datetime.now() - cur_time))

    def __reason_from_service(self, target):
        target_file = None
        offset = 0
        step = 10000
        while True:
            rdf_instances = self.__server.query(
                """
            SELECT DISTINCT ?instance
            WHERE {?instance rdf:type ?x}
            LIMIT {}
            OFFSET {}
            """.format(step, offset))
            if len(rdf_instances) < 1:
                break
            for t in rdf_instances:
                log_progress(offset, 100)
                t = t["type"]["value"]
                if target:
                    if not target_file:
                        target_file = target + str(self.__server.server).split("/")[-2] + str("_reasoned.nt")
                    self.__spawn_daemon(materialize_to_file, dict(rdf_instance=t, target=target_file,
                                                                  server=self.__server))
                else:
                    self.__spawn_daemon(materialize_to_service, dict(rdf_instance=t, server=self.__server))

    def __reason_from_file(self, f, target):
        target_file = None
        # Iterate through file
        with open(f) as input_file:
            tmp_instance = ""
            types = set([])
            for line_num, line in enumerate(input_file):
                triple = self.nt_parser.get_triple(line)
                if not triple:
                    continue
                log_progress(line_num, 100)

                if not triple['subject'] == tmp_instance:
                    if target:
                        if not target_file:
                            target_file = target + str(self.__server.server).split("/")[-2] + str("_reasoned.nt")
                        self.__spawn_daemon(materialize_to_file, dict(instance=tmp_instance, types=types, target=target_file,
                                                                      server=self.__server))
                    else:
                        self.__spawn_daemon(materialize_to_service, dict(instance=tmp_instance, types=types, server=self.__server))
                    tmp_instance = triple['subject']
                    types = {triple["object"]}

    def __spawn_daemon(self, target, kwargs):
        # Todo Event based?
        # Check every 0.1 seconds if we can continue
        if hasattr(self, "processManager"):
            while not self.processManager.has_free_process_slot():
                time.sleep(0.1)

        p = Process(target=target, kwargs=kwargs)
        p.daemon = True
        if hasattr(self, "processManager"):
            try:
                self.processManager.add(p)
            except OccupiedError as e:
                log.critical(e)
                return 2
            else:
                p.start()
        else:
            p.start()

