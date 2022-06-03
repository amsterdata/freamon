import warnings
import sys

warnings.filterwarnings("ignore")

from freamon import Freamon

pipeline = sys.argv[1]
k = sys.argv[2]


cmd_args = [k]
pipeline, runtimes = Freamon().pipeline_from_py_file(pipeline, cmd_args=cmd_args)

print(pipeline)
for step, runtime in runtimes.items():
    print(step, runtime)
