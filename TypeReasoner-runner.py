from src.TypeReasoner import TypeReasoner
import click

@click.command()
@click.option('--server', '-s', default="http://localhost:8585/bigdata/sparql",
              help='Uri to the sparql endpoint which stores the RDFS SubClass Information.')
@click.option('--user', '-u', default="admin", help='User for the sparql endpoint.')
@click.option('--password', '-p', default="dev98912011", help='Password for the sparql endpoint.')
@click.option('--n-processes', '-x', default="11",
              help='Number of processes to spawn simultaneously.')
@click.option('--file', '-f', default=None,
              help='Input file to reason about.')
@click.option('--target', '-t', default="./reasoned/",
              help='Output folder to save reasoned data to.')
@click.option('--log-level', '-l', default="WARN")
@click.option('--offset', '-o', default="0")
def main(server, user, password, log_level, n_processes, file, target, offset):

    reasoner = TypeReasoner(server=server, user=user, password=password, n_processes=int(n_processes), log_level=log_level)

    if type(file) is list:
        for f in file:
            reasoner.reason(f, target, offset)
    else:
        reasoner.reason(file, target, offset)

if __name__ == '__main__':
    main()