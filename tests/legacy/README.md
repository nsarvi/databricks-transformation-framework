# Legacy tests and configs

Kept for reference only. pytest ignores this folder (`collect_ignore` in `tests/conftest.py`).

These tests and configs came from the old ADM workspace and read client tables that don't exist in
the Wilson-Elser workspace (`bronze_dev01.*`, `silver_dev01.*`, `sandbox.spoadm_*`):

- `use_case_test.py` with `configs/use_case/`: R2R open-items pipelines, including a custom function
  from `use_cases.custom_transformations`
- `p2c_sales_order_invoice_test.py` with `configs/joins/sales_order_invoice.yml`: P2C sales order invoice joins
- `configs/adm_src_configs/` and `configs/silver_store_config.yaml`: unused ADM configs, previously in `src/configs/`
- `configs/joins/joins_multiple_tables_config copy.yaml`: stray copy of `tests/configs/joins/joins_multiple_tables_config.yaml`

To bring one back, move it into `tests/`, point it at `${catalog}.${schema}` tables that the test creates,
and remove its skip marker.
