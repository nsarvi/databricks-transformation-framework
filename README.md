#  Integration Framework

This framework performs reads, transformes and writes to a Delta table based on the connfigurations provided

## Build

To develop and build a binary distribution of the framework, follow the steps

git checkout <azure-repo-url-for--integration>

Setup the Python Virtual environment

source .venv/bin/activate

python -m build --wheel

This should generate  _ingestion_framework-1.0.0-py3-none-any.whl

## Usage

Notebook scoped Usage

%pip install /Workspace/Common-Libs/_ingestion_framework-1.0.0-py3-none-any.whl

%restart_python

## Configurations

//Todo

## Source Table Reads

from .integration._data_transformer import DataTransformer
from .integration._table_writer import TableWriter
from .integration._base_data_transformer import BaseDataTransformer

//Todo
## Transformations

Todo

## Joins

src_path = (Path.cwd() ).as_posix()
sys.path.append(src_path)
print(src_path)
config_path = Path(src_path) / "<>"
transformer = DataTransformer(config_path.as_posix())

result_df = transformer.apply_joins("combine_id_1")

## Target Table Writes

Todo

## Unit Tests

Todo
