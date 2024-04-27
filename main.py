# NOTE: Top level script to make the application runnable with the luigi-workflow template.
import luigi
from src.trigger import TriggerPipeline

if __name__ == '__main__':
    luigi.run()
