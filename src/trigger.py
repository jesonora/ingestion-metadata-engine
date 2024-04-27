import logging
import warnings
from datetime import date, datetime, timedelta

import luigi
import yaml

from src.tasks.transform_task import TransformInput

warnings.filterwarnings("ignore")

logger = logging.getLogger(__name__)


class TriggerPipeline(luigi.WrapperTask):
    """Wrap up all the tasks for the pipeline into a single task
    So we can run this pipeline by calling this dummy task.

    The task transforms the config.yml file into a dictionary. Also passes the authentication details as a dictionary
    to the next task.

    Usage::
        $ python
        -m luigi
        --module main TriggerPipeline

    :ivar config_path:
        Path to the config file, default value: '/local/application/conf/config.yml'.
    :ivar year:
        Year to process, default value: datetime.date.today().year.
    :ivar month:
        Month to process, default value: datetime.date.today().month.
    :ivar day:
        Day to process, default value: datetime.date.today().day.
    """

    config_path = luigi.Parameter(default="/opt/workspace/ingestion-metadata-engine/config/config_dev.yml")
    year = luigi.IntParameter(default=date.today().year)
    month = luigi.IntParameter(default=date.today().month)
    day = luigi.IntParameter(default=date.today().day)

    def requires(self):
        """
        Build all the dependencies.
        """

        input_date = datetime(year=self.year, month=self.month, day=self.day)
        close_date = input_date - timedelta(days=1)

        with open(self.config_path) as f:
            config_dict = yaml.safe_load(f)

        tasks = []

        for dataflow in config_dict.items():
            kw_args = {
                "close_date": close_date,
                "params": dataflow[1],
                "transformation_name": dataflow[0],
            }
            tasks.append(TransformInput(**kw_args))

        return tasks
