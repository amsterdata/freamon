import command
import time

for pipeline in ['overhead-bench--reviews.py']:#['overhead-bench--credit.py']:
    for k in [1, 5, 10, 20]:
        start = time.time()
        res = command.run(["python", pipeline, str(k)])
        duration = time.time() - start

        print(pipeline, k, duration * 1000)
