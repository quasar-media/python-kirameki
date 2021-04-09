class PoolError(Exception):
    pass


class PoolClosed(PoolError):
    pass


class PoolTimeout(PoolError):
    pass


class PoolDeadlocked(PoolError):
    pass


__all__ = ["PoolError", "PoolClosed", "PoolTimeout", "PoolDeadlocked"]
