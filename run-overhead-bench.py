import command
import time

for pipeline in ['overhead-bench--reviews.py']:
    for k in [1, 5, 10, 20]:

        start = time.time()
        res = command.run(["python", pipeline, str(k)])
        noninstrumented_time = (time.time() - start) * 1000

        start = time.time()
        res = command.run(["python", "overhead-bench--instrumented.py", pipeline, str(k)])
        instrumented_time = (time.time() - start) * 1000

        #times = eval(res.output)

        #line = f'{pipeline},{k},{noninstrumented_time},{times["instrumentation"]},' + \
        #    f'{times["provenance_conversion"]},{times["train_source_extraction"]},' + \
        #    f'{times["test_source_extraction"]},{times["feature_matrix_extraction"]},' + \
        #    f'{times["label_and_prediction_extraction"]}'
        #
        #print(line)

        print(f'k={k},noinst={noninstrumented_time},inst={instrumented_time}')





