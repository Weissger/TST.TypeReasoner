__author__ = 'tmy'

from src.SparqlInterface.src.Interfaces.AbstractClient import SparqlConnectionError
from src.Utilities.Logger import log

def materialize_to_file(instance=None, target=None, server=None):
    rdf_types = __get_all_types(instance, server)
    with open(target, "a+") as f:
        for rdf_type in rdf_types:
            log.debug("DONE {}".format(rdf_type))
            f.write("<" + instance + ">  <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <" + rdf_type + ">.\n")


def materialize_to_service(instance=None, server=None):
    rdf_types = __get_all_types(instance, server)
    for rdf_type in rdf_types:
        server.insert_triple("<" + instance + ">", " <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
                             "<" + rdf_type + ">")

def __get_all_types(instance, server):
    all_types = set([])
    retries = 0
    while True:
        try:
            types = server.get_types(instance)
            for type in types:
                log.debug("LOOK {}".format(type))
                if type not in all_types:
                    log.debug("DO {}".format(type))
                    current = server.get_all_class_parents(type).append(type)
                    all_types.union(set(current))
        except SparqlConnectionError as e:
            if retries == 0:
                log.warn("Error on query for: " + str(instance) + "\n" + str(e) + "\n")
            retries += 1
            log.debug("Error on: " + str(instance) + " - try number: " + str(retries) + "\n" + str(e) + "\n")
            if retries > 5:
                break
        else:
            if retries > 0:
                log.info("Success on retry for:" + str(instance) + "\n")
            break
    log.debug("ALL {}".format(all_types))
    return all_types