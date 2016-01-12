__author__ = 'tmy'


from .Logger import log


def log_progress(line_num, every_n_lines):
    if line_num % every_n_lines == every_n_lines-1:
        log.info("At line: " + str(line_num))