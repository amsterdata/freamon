import sys
import time
import warnings

from freamon.adapters.mlinspect.provenance import from_py_file
from overhead_bench_dspipes import run_pipeline as run_dspipes
from overhead_bench_credit import run_pipeline as run_credit
from overhead_bench_openml import run_pipeline as run_openml
from overhead_bench_reviews import run_pipeline as run_reviews

warnings.filterwarnings("ignore")

if not len(sys.argv) > 1:
    print("Specify pipeline file!")
    sys.exit(-1)

# 'overhead_bench_dspipes.py', 'overhead_bench_openml.py', 'overhead_bench_reviews.py', 'overhead_bench_credit.py'
pipeline_file = sys.argv[1]

pipeline_runners = {
    'overhead_bench_dspipes.py': run_dspipes,
    'overhead_bench_credit.py': run_credit,
    'overhead_bench_openml.py': run_openml,
    'overhead_bench_reviews.py': run_reviews,
}

print('pipeline,k,non_instrumented,instrumented')

num_repetitions = 7

# Warm-up run to ignore effect of imports
_view_gen = from_py_file(pipeline_file, cmd_args='1')

for k in [1, 5, 10, 20, 35, 50]:

    for _ in range(num_repetitions):

        start = time.time()
        pipeline_runners[pipeline_file](k)
        noninstrumented_time = (time.time() - start) * 1000

        start = time.time()
        _view_gen = from_py_file(pipeline_file, cmd_args=[str(k)])
        instrumented_time = (time.time() - start) * 1000

        line = f'{pipeline_file},{k},{int(noninstrumented_time)},{int(instrumented_time)}'
        print(line)
