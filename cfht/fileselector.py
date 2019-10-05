from trigger import FileSelector, SingleFileSelector


class CfhtFileSelector(FileSelector):
    def __init__(self):
        super().__init__(CfhtSingleFileSelector)


class CfhtSingleFileSelector(SingleFileSelector):
    def is_desired_etype(cls, checker, steps):
        if (steps.database or steps.distribute or steps.distraw) and cls.has_object_extension(checker.file):
            return True
        return super().is_desired_etype(checker, steps)
