import asyncio

from math import trunc


def background(f):  # pragma: no cover
    def wrapped(*args, **kwargs):
        return asyncio.get_event_loop().run_in_executor(None, f, *args, **kwargs)

    return wrapped


async def gather_with_concurrency(number_of_workers, total_tasks, *tasks):  # pragma: no cover
    semaphore = asyncio.Semaphore(number_of_workers)

    async def sem_task(task, counter):  # pragma: no cover
        perc = trunc(((counter+1)/total_tasks)*100)
        print(f"Done {perc}%.")
        async with semaphore:
            return await task

    return await asyncio.gather(*[sem_task(task, counter) for counter, task in enumerate(tasks)])
