import asyncio
import multiprocessing as mp
from concurrent.futures import ThreadPoolExecutor
from threading import Thread

import pytest
from context_tracer.utils.async_mp_queue import AsyncMultiProcessQueue


def test_AsyncMultiProcessQueue_init():
    assert AsyncMultiProcessQueue()


def test_AsyncMultiProcessQueue_put_get():
    queue = AsyncMultiProcessQueue()
    assert queue.empty()
    queue.put(1)
    assert queue.get() == 1
    assert queue.empty()


@pytest.mark.asyncio
async def test_AsyncMultiProcessQueue_put_get_async():
    queue = AsyncMultiProcessQueue()
    assert queue.empty()
    await queue.put_async(1)
    assert await queue.get_async() == 1
    assert queue.empty()


@pytest.mark.asyncio
async def test_AsyncMultiProcessQueue_put_get_async_multiple():
    nb_tasks = 10
    input_vars = list(range(nb_tasks))
    queue = AsyncMultiProcessQueue()
    result_future = asyncio.gather(*[queue.get_async() for _ in input_vars])
    asyncio.gather(*[queue.put_async(i) for i in input_vars])
    result = await result_future
    assert set(result) == set(input_vars)


def test_AsyncMultiProcessQueue_put_new_thread():
    queue = AsyncMultiProcessQueue()
    # Run in new thread
    thread = Thread(target=queue.put_nowait, args=(1,), daemon=True)
    thread.start()
    # Test Get
    assert queue.get() == 1
    thread.join()
    assert queue.empty()


def test_AsyncMultiProcessQueue_put_thread_pool():
    queue = AsyncMultiProcessQueue()
    nb_tasks = 10
    input_vars = list(range(nb_tasks))
    with ThreadPoolExecutor() as pool:
        for i in input_vars:
            pool.submit(queue.put_nowait, i)
        results = list(pool.map(lambda _: queue.get(), input_vars))
    assert set(results) == set(input_vars)


def test_AsyncMultiProcessQueue_put_new_process_fork():
    ctx = mp.get_context("fork")
    queue = AsyncMultiProcessQueue(ctx=ctx)
    # Run in new forked process
    proc = ctx.Process(target=queue.put_nowait, args=(1,), daemon=True)
    proc.start()
    # Test Get
    assert queue.get() == 1
    proc.join()
    assert queue.empty()


def test_AsyncMultiProcessQueue_put_new_process_spawn():
    ctx = mp.get_context("spawn")
    queue = AsyncMultiProcessQueue(ctx=ctx)
    # Run in new spawned process
    proc = ctx.Process(target=queue.put_nowait, args=(1,), daemon=True)
    proc.start()
    # Test Get
    assert queue.get() == 1
    proc.join()
    assert queue.empty()


def test_AsyncMultiProcessQueue_get_new_process_spawn():
    ctx = mp.get_context("spawn")
    queue = AsyncMultiProcessQueue(ctx=ctx)
    # Run in new spawned process
    proc = ctx.Process(target=queue.get, args=(), daemon=True)
    proc.start()
    queue.put_nowait(1)
    proc.join()
    assert queue.empty()


def test_AsyncMultiProcessQueue_get_put_multipe_processes():
    ctx = mp.get_context("spawn")
    queue = AsyncMultiProcessQueue(ctx=ctx)
    # Run in new spawned process
    proc1 = ctx.Process(target=queue.get, args=(), daemon=True)
    proc1.start()
    proc2 = ctx.Process(target=queue.get, args=(), daemon=True)
    proc2.start()
    # Test Get
    results = []
    for i in range(2):
        results.append(queue.put_nowait(i))
    proc1.join()
    proc2.join()
    assert queue.empty()
