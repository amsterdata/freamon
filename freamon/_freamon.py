import mlflow
from freamon.extraction import with_mlinspect


class Freamon:

    def __init__(self, series_id, artifact_storage_uri):
        self.mlflow = mlflow
        self.mlflow.set_tracking_uri(artifact_storage_uri)
        self.mlflow.set_experiment(series_id)

    def pipeline_from_py_file(self, pyfile, cmd_args=[]):
        return with_mlinspect.from_py_file(pyfile, cmd_args)
