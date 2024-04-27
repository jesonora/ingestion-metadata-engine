import luigi
from luigi import Task
from luigi.contrib.spark import SparkSubmitTask


class TaskTemplate(Task):
    """This class creates the dependencies ofthe pipeline. While reading the dictionary of tasks from the
    trigger class, it builds the graphs using the available tasks in the catalogue.


    :ivar metadata:

    """
    metadata = luigi.DictParameter()

class SparkTemplate(SparkSubmitTask, TaskTemplate):
    """
    This class creates the dependencies ofthe pipeline. While reading the dictionary of tasks from the
    trigger class, it builds the graphs using the available tasks in the catalogue.
    """

    def app_options(self):
        return [
            self.metadata
        ]
