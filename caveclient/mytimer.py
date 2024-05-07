import logging
from time import time

logger = logging.getLogger(__name__)


indent = 0


class MyTimeIt:
    def __init__(self, message="", *args, **kwargs):
        self._message = message
        self._args = args
        self._kwargs = kwargs
        self._start = None

    def __enter__(self):
        logger.debug(f"start {self._message}")
        global indent
        if self._args:
            args_str = " ".join(str(x) for x in self._args)
            print(args_str)
        if self._kwargs:
            kwargs_str = " ".join(f"{k}:{v}" for k, v in self._kwargs)
            print(kwargs_str)
        self._start = time()

    def __exit__(self, *args):
        global indent
        indent -= 2
        logger.debug(f"end {self._message} -- {time()-self._start}")
