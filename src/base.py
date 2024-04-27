import luigi
from luigi.contrib.spark import SparkSubmitTask


class TaskTemplate(luigi.Task):
    """
    This class defines a template for Luigi tasks that serve as dependencies in the pipeline.
    It builds the dependency graph by reading tasks from a dictionary provided as metadata.

    :param metadata: A dictionary containing task dependencies.
    :type metadata: dict
    """

    params = luigi.DictParameter()
    close_date = luigi.DateHourParameter()
    name = luigi.Parameter()
    logs_root = luigi.Parameter(
        default="/opt/workspace/ingestion-metadata-engine/data/logs"
    )

    def __init__(self, *args, **kwargs):
        """
        Initializes the TaskTemplate instance.

        :param args: Positional arguments.
        :param kwargs: Keyword arguments.
        """
        super().__init__(*args, **kwargs)
        self.yyyymmdd_string = self.close_date.strftime("%Y%m%d")


class SparkTemplate(SparkSubmitTask, TaskTemplate):
    """
    This class extends SparkSubmitTask and serves as a template for Spark jobs within the pipeline.
    It builds the dependency graph by reading tasks from a dictionary provided as metadata.
    """

    def app_options(self):
        """
        Returns a list of options to be passed to the Spark application.

        :return: A list of Spark application options.
        :rtype: list
        """
        return [self.params, self.logs_root, self.yyyymmdd_string, self.name]
