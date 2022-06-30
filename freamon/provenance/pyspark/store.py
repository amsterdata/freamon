def singleton(class_):
    instances = {}

    def getinstance(*args, **kwargs):
        if class_ not in instances:
            instances[class_] = class_(*args, **kwargs)
        return instances[class_]

    return getinstance


@singleton
class SingletonProvStore:

    def __init__(self):
        self.sources = {}
        self.predictions = None
        self.train_provenance = None
        self.test_provenance = None
