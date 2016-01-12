__author__ = 'tmy'

from src.RemoteSparql.AbstractRemoteSparql import AbstractRemoteSparql
from src.RemoteSparql.AbstractRemoteSparql import SparqlConnectionError
from src.Utilities.Logger import log


def materialize_reason(instance=None, types=None, target=None, server=AbstractRemoteSparql):
    with open(target, "a+") as f:
        tmp = set([])
        for t in types:
            if t not in tmp:
                retries = 0
                parents = []
                while True:
                    try:
                        parents = server.get_parents(t)
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
                        tmp.add(t)
                        break
                for parent in parents:
                    if parent not in tmp:
                        tmp.add(parent)
                        f.write(
                            "<" + instance + "> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <" + parent + ">.\n")