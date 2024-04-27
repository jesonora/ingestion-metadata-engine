import luigi
import yaml
import logging
from datetime import datetime, timedelta, date

from luigi import IntParameter, Parameter

from src.tasks.process_task import PreProcessRawFile

import warnings

warnings.filterwarnings("ignore")

logger = logging.getLogger(__name__)


class TriggerPipeline(luigi.WrapperTask):
    """Wrap up all the tasks for the pipeline into a single task
    So we can run this pipeline by calling this dummy task.

    The task transform the config.yml file into a dictionary. Also passes the authentication details as a dictionary
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

    config_path = Parameter(default="/app/config/config_prod.yml")

    year = IntParameter(default=date.today().year)
    month = IntParameter(default=date.today().month)
    day = IntParameter(default=date.today().day)

    def requires(self):
        """
        Build all the dependencies.
        """

        input_date = datetime(year=self.year, month=self.month, day=self.day)

        close_date = input_date - timedelta(days=1)

        with open(self.config_path) as f:
            config_dict = yaml.safe_load(f)

        tasks = list()

        for dataflow in config_dict.items():

            tasks.append(PreProcessRawFile(dataflow[1]))

        yield tasks
