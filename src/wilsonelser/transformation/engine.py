"""The framework's single entry point: one engine per pipeline, created from its config, used by id."""
from typing import Optional, Union

from wilsonelser.transformation.pipeline_config import PipelineConfig
from wilsonelser.transformation.table_writer import TableWriter


class TransformationEngine(TableWriter):
    """
    Runs a pipeline config: create it once, then work by id.

        engine = TransformationEngine("use_cases/sales/pipeline.yml", env_config_file="auto")
        engine.write_table("silver_sales")            # reads, transforms, combines and writes
        engine.read_source_table("customers")         # a source, with its transformation_id applied
        engine.apply_combine("customer_sales")        # a join or union
        engine.apply_transformations("clean", df)     # a transformation on any DataFrame

    The config is loaded, resolved and validated once, from a YAML file, a folder of YAML files, or an
    already loaded PipelineConfig (e.g. one shared by several engines, or built with PipelineConfig.from_dict).
    """

    def __init__(self, config: Union[str, PipelineConfig], env_config_file: Optional[str] = None):
        """
        :param config: The path of a YAML file or folder, or a loaded PipelineConfig.
        :param env_config_file: Values for ${...} placeholders when `config` is a path: a file, "auto" for the
                                current workspace's environment, or None for environment variables only.
        """
        super().__init__(config, env_config_file)
