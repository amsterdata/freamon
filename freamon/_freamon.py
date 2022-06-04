from freamon.extraction import with_mlinspect


class Freamon:
    def pipeline_from_py_file(self, pyfile, cmd_args=[]):
        return with_mlinspect.from_py_file(pyfile, cmd_args)
