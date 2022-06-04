import command
import time


print('pipeline,k,non_instrumented,instrumented,mlinspect,provenance_conv,train_source,test_source,feature_matrix,predictions')

num_repetitions = 7
for _ in range(num_repetitions):
    for pipeline in ['overhead-bench--dspipes.py', 'overhead-bench--openml.py',
                     'overhead-bench--reviews.py', 'overhead-bench--credit.py']:
        for k in [1, 5, 10, 20, 35, 50]:

            start = time.time()
            res = command.run(["python", pipeline, str(k)])
            noninstrumented_time = (time.time() - start) * 1000

            start = time.time()
            res = command.run(["python", "overhead-bench--instrumented.py", pipeline, str(k)])
            instrumented_time = (time.time() - start) * 1000

            times = eval(res.output)

            line = f'{pipeline},{k},{int(noninstrumented_time)},{int(instrumented_time)},' + \
                f'{int(times["instrumentation"])},' + \
                f'{int(times["provenance_conversion"])},{int(times["train_source_extraction"])},' + \
                f'{int(times["test_source_extraction"])},{int(times["feature_matrix_extraction"])},' + \
                f'{int(times["label_and_prediction_extraction"])}'

            print(line)

            #print(f'k={k},noinst={noninstrumented_time},inst={instrumented_time}')

