import asyncio

from math import trunc


def background(f):  # pragma: no cover
    def wrapped(*args, **kwargs):
        return asyncio.get_event_loop().run_in_executor(None, f, *args, **kwargs)

    return wrapped


async def gather_with_concurrency(number_of_workers, tasks):  # pragma: no cover
    semaphore = asyncio.Semaphore(number_of_workers)

    async def sem_task(task, counter):  # pragma: no cover
        async with semaphore:
            return await task

    return await asyncio.gather(*[sem_task(task, counter) for counter, task in enumerate(tasks)],
                                return_exceptions=True)


def patch_https_connection_pool(**constructor_kwargs):
    """
    This allows to override the default parameters of the
    HTTPConnectionPool constructor.
    For example, to increase the poolsize to fix problems
    with "HttpSConnectionPool is full, discarding connection"
    call this function with maxsize=16 (or whatever size
    you want to give to the connection pool)
    """
    from urllib3 import connectionpool, poolmanager

    class MyHTTPSConnectionPool(connectionpool.HTTPSConnectionPool):
        def __init__(self, *args,**kwargs):
            kwargs.update(constructor_kwargs)
            super(MyHTTPSConnectionPool, self).__init__(*args,**kwargs)
    poolmanager.pool_classes_by_scheme['https'] = MyHTTPSConnectionPool


def patch_http_connection_pool(**constructor_kwargs):
    """
    This allows to override the default parameters of the
    HTTPConnectionPool constructor.
    For example, to increase the poolsize to fix problems
    with "HttpConnectionPool is full, discarding connection"
    call this function with maxsize=16 (or whatever size
    you want to give to the connection pool)
    """
    from urllib3 import connectionpool, poolmanager

    class MyHTTPConnectionPool(connectionpool.HTTPConnectionPool):
        def __init__(self, *args,**kwargs):
            kwargs.update(constructor_kwargs)
            super(MyHTTPConnectionPool, self).__init__(*args,**kwargs)
    poolmanager.pool_classes_by_scheme['http'] = MyHTTPConnectionPool