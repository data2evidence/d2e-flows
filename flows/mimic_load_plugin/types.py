from pydantic import BaseModel

DuckDBCreateSql = 'flows/mimic_load_plugin/external/duckdb_create_mimic.sql'
PostgresCreateSql = 'flows/mimic_load_plugin/external/create.sql'
PostgresLoadSql = 'flows/mimic_load_plugin/external/load.sql'
PostgresConstraintSql = 'flows/mimic_load_plugin/external/constraint.sql'
PostgresIndexSql = 'flows/mimic_load_plugin/external/index.sql'


class MimicLoadOptionsType(BaseModel):
    mimic_dir: str
    database_code: str

    @property
    def use_cache_db(self) -> str:
        return False