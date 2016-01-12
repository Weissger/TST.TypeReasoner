__author__ = 'tmy'

import logging
import inspect
import types


def get_caller_str():
    """
    Get name of caller method and context
    """
    stack = inspect.stack()
    if 'self' in stack[2][0].f_locals:
        # for class methods
        the_class = stack[2][0].f_locals["self"].__class__
        the_method = stack[2][0].f_code.co_name
        class_string = str(the_class)[8:-2]
        out = "{}.{}: ".format(class_string[class_string.rfind('.') + 1:], the_method)
    else:
        # for static methods
        frame_info = inspect.getframeinfo(stack[2][0])
        the_file = frame_info[0][frame_info[0].rfind('/') + 1:]
        the_method = frame_info[2]
        out = "{}:{}: ".format(the_file, the_method)
    return out


def debug(self, msg, *args, **kwargs):
    if self.isEnabledFor(logging.DEBUG):
        self.debug_help(get_caller_str() + str(msg), *args, **kwargs)
    else:
        self.debug_help(msg, *args, **kwargs)


def info(self, msg, *args, **kwargs):
    if self.isEnabledFor(logging.INFO):
        self.info_help(get_caller_str() + str(msg), *args, **kwargs)
    else:
        self.info_help(msg, *args, **kwargs)


def warning(self, msg, *args, **kwargs):
    if self.isEnabledFor(logging.WARN):
        self.warning_help(get_caller_str() + str(msg), *args, **kwargs)
    else:
        self.warning_help(msg, *args, **kwargs)


def error(self, msg, *args, **kwargs):
    if self.isEnabledFor(logging.ERROR):
        self.error_help(get_caller_str() + str(msg), *args, **kwargs)
    else:
        self.error_help(msg, *args, **kwargs)

logging.basicConfig(level="CRITICAL")
log = logging.getLogger("TypeReasoner")

log.debug_help = log.debug
log.debug = types.MethodType(debug, log)

log.warning_help = log.warning
log.warning = types.MethodType(warning, log)

log.error_help = log.error
log.error = types.MethodType(error, log)

log.info_help = log.info
log.info = types.MethodType(info, log)