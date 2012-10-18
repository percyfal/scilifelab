"""
log module
"""
from logbook import Logger, FileHandler

file_format = "{record.time} ({record.level_name}) {record.channel} : {record.message}"

def minimal_logger(namespace):
    """Make and return a minimal logger

    :param namespace: namspace of logger
    """
    return Logger(namespace)

def file_logger(fn, namespace, level=2, fmt_string=file_format):
    """Make and return a file logger.

    :param fn: file name to which log messages are written
    :param namespace: namespace of logger
    """
    file_handler = FileHandler(fn,
                               format_string=fmt_string,
                               level = level,
                               bubble = False,
                               )
    return file_handler
