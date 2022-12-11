from asyncio import (
    create_subprocess_exec as run, create_task, gather, run as run_main,
    get_running_loop, StreamReader, StreamReaderProtocol, Semaphore, sleep)
from asyncio.subprocess import PIPE
from sys import stdin, argv

host_i, n_instance = int(argv[1]), int(argv[2])

async def spawn_instance(sem, i):
    p = await run("./peer", "entropy", str(host_i), str(n_instance), str(i), stdout=PIPE, stderr=PIPE)
    while not p.stdout.at_eof():
        line = await p.stdout.readline()
        print(line.decode(), end='')
        # if line.decode().strip() == "Ready":
        #     sem.release()
    _, error_lines = await p.communicate()
    if error_lines := error_lines.decode().strip():
        print(error_lines)


async def shutdown():
    p = await run("pkill", "-INT", "peer")
    await p.wait()

async def main():
    tasks = []
    sem = Semaphore(0)
    for i in range(n_instance):
        tasks.append(create_task(spawn_instance(sem, i + 1)))
        # if (i + 1) % 10 == 0:
        # print(f"{i + 1} / {n_instance}")
        # await sleep(0.01)
    # for _ in range(n_instance):
    #     await sem.acquire()
    
    print("Press ENTER to exit ")
    reader = StreamReader(loop=get_running_loop())
    protocol = StreamReaderProtocol(reader)
    await get_running_loop().connect_read_pipe(lambda: protocol, stdin)
    await reader.readline()

    await shutdown()
    await gather(*tasks)

run_main(main())
