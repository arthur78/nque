import logging

_loggers_suppressed = False


def suppress_loggers():
    """
    Suppress expected loggers for not polluting tests output.

    Call this function just once in any test module.
    """
    global _loggers_suppressed
    if not _loggers_suppressed:
        for logger_name in ('nque', ):
            logger = logging.getLogger(logger_name)
            logger.setLevel(logging.CRITICAL + 1)
        _loggers_suppressed = True
        print('[Loggers suppressed in tests/__init__.py]')
