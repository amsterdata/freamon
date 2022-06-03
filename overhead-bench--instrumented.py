import warnings
import sys

warnings.filterwarnings("ignore")

from freamon import Freamon

pipeline_file = sys.argv[1]
k = sys.argv[2]


cmd_args = [k]
pipeline, runtimes = Freamon().pipeline_from_py_file(pipeline_file, cmd_args=cmd_args)
pipeline, runtimes = Freamon().pipeline_from_py_file(pipeline_file, cmd_args=cmd_args)


output = '{' + ','.join([f'"{step}": {runtime}' for step, runtime in runtimes.items()]) + '}'

print(output)

