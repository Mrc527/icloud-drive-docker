import asyncio


def background(f):  # pragma: no cover
    def wrapped(*args, **kwargs):
        return asyncio.get_event_loop().run_in_executor(None, f, *args, **kwargs)

    return wrapped


async def gather_with_concurrency(n, *tasks):  # pragma: no cover
    semaphore = asyncio.Semaphore(n)

    async def sem_task(task):  # pragma: no cover
        async with semaphore:
            return await task

    return await asyncio.gather(*(sem_task(task) for task in tasks))
