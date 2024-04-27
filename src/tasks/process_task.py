import os
import logging

from luigi import LocalTarget
from src.base import SparkTemplate

logger = logging.getLogger(__name__)


class PreProcessRawFile(SparkTemplate):
    """
    This class start the preprocess task, calling the main function of the file,
    :func:`src.process.preprocess.launch_preprocess`
    """

    app = os.path.join(os.path.dirname(__file__), "preprocess.py")

    def output(self):
        """
        Return the outputs.

        """
        return LocalTarget('words.txt')
