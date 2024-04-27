import logging
import os

from luigi import LocalTarget

from src.base import SparkTemplate

logger = logging.getLogger(__name__)


class TransformInput(SparkTemplate):
    """
    This class represents a Luigi task for preprocessing raw files. It initiates the preprocessing task by calling
    the main function of the file: `src.process.preprocess.launch_preprocess`.
    """

    app = os.path.join(os.path.dirname(__file__), "transform.py")

    def output(self):
        """
        Defines the output target of the preprocessing task.

        :return: A LocalTarget object representing the output file.
        :rtype: luigi.LocalTarget
        """
        output_file = os.path.join(
            self.logs_root, f"{self.transformation_name}_{self.yyyymmdd_string}_OK.logs"
        )
        return LocalTarget(output_file)
