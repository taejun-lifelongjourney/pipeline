from multiprocessing import Value
from multiprocessing import Lock


class AtomicCounter(object):
    def __init__(self, init_value=0):
        self._val = Value('i', init_value)

    def increase(self, incr=1):
        with self._val.get_lock():
            self._val.value += incr
            return self._val.value

    def decrease(self, decr=1):
        with self._val.get_lock():
            self._val.value -= decr
            return self._val.value

    @property
    def value(self):
        with self._val.get_lock():
            return self._val.value

    @property
    def lock(self):
        return self._val.get_lock()
