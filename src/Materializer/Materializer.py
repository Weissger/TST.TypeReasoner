__author__ = 'tmy'

from src.SparqlInterface.src.Interfaces.AbstractClient import SparqlConnectionError
from src.Utilities.Logger import log


def materialize_to_file(instance=None, types=None, target=None, server=None):
    rdf_types = __get_all_parents(types, server)
    with open(target, "a+") as f:
        for rdf_type in rdf_types:
            f.write("<" + instance + ">  <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <" + rdf_type + ">.\n")


def materialize_to_service(instance=None, types=None, server=None):
    rdf_types = __get_all_parents(types, server)
    for rdf_type in rdf_types:
        server.insert_triple("<" + instance + ">", " <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
                             "<" + rdf_type + ">")


def __get_all_parents(in_types, server):
    tmp = set([])
    for t in in_types:
        if t not in tmp:
            parents = []
            retries = 0
            tmp.add(t)
            while True:
                try:
                    parents = server.get_types(t)
                except SparqlConnectionError as e:
                    if retries == 0:
                        log.warn("Error on query for: " + str(t) + "\n" + str(e) + "\n")
                    retries += 1
                    log.debug("Error on: " + str(t) + " - try number: " + str(retries) + "\n" + str(e) + "\n")
                    if retries > 5:
                        break
                else:
                    if retries > 0:
                        log.info("Success on retry for:" + str(t) + "\n")
                    break
            for parent in parents:
                if parent not in tmp:
                    tmp.add(parent)
    return tmp