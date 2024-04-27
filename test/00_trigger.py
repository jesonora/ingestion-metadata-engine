import os

import luigi

from src.trigger import TriggerPipeline

if __name__ == "__main__":
    luigi_config_file = os.path.join("..", "config", "luigi.cfg")
    config_file = os.path.join("..", "config", "config_dev.yml")
    logging_file = os.path.join("..", "config", "logging.cfg")

    luigi.configuration.add_config_path(luigi_config_file)

    year = 2023
    month = 8
    day = 23

    luigi_run_result = luigi.build(
        [
            TriggerPipeline(
                year=year,
                month=month,
                day=day,
                config_path=config_file,
            ),
        ],
        logging_conf_file=logging_file,
        workers=1,
        local_scheduler=True,
    )
