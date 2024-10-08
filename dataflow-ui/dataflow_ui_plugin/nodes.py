
import os
import json
import logging
import importlib
import pandas as pd
import traceback as tb
from functools import partial
from typing import List, Dict

import sqlalchemy
from sqlalchemy.sql import select
from sqlalchemy import MetaData, Table, create_engine

from prefect import task, flow

from dataflow_ui_plugin.hooks import *
from dataflow_ui_plugin.flowutils import *


class Node:
    def __init__(self, node):
        self.id = node["id"]
        self.type = node["type"]
        self.ro = importlib.import_module('rpy2.robjects')
        self.use_cache_db = False


class Flow(Node):
    def __init__(self, _node):
        self.graph = _node["graph"]
        self.executor_type = _node["executor_options"]["executor_type"]
        self.executor_host = _node["executor_options"]["executor_address"]["host"]
        self.executor_port = _node["executor_options"]["executor_address"]["port"]
        self.ssl = _node["executor_options"]["executor_address"]["ssl"]
        self.sorted_nodes = get_node_list(self.graph)


class Result:
    def __init__(self, error: bool, data, node: Node, task_run_context):
        self.error = error
        self.node = node
        self.task_run_id = str(task_run_context.get("id"))
        self.task_run_name = str(task_run_context.get("name"))
        self.flow_run_id = str(task_run_context.get("flow_run_id"))
        self.data = {
            "result": data,
            "error": self.error,
            "errorMessage": data if self.error else None,
            "nodeName": self.node.id
        }


class SqlNode(Node):
    def __init__(self, _node):
        super().__init__(_node)
        self.tables = _node["tables"]
        self.sql = _node["sql"]

    def task(self, _input: Dict[str, Result], task_run_context):
        try:
            dask_sql_module = importlib.import_module("dask_sql")
            c = dask_sql_module.Context()
            for tablename in self.tables.keys():
                input_element = _input
                for path in self.tables[tablename]:
                    input_element = input_element[path].data
                c.create_table(tablename, input_element)
            ddf = c.sql(self.sql).compute()
            return Result(False,  ddf, self, task_run_context)
        except Exception as e:
            return Result(True, tb.format_exc(), self, task_run_context)

    def test(self, _input: Dict[str, Result], task_run_context):
        return self.task(_input, task_run_context)


class PythonNode(Node):
    def __init__(self, _node):
        super().__init__(_node)
        self.source_code = _node["python_code"] + '\noutput = exec(myinput)'
        self.test_source_code = _node["python_code"]+'\noutput = test_exec(myinput)'
        
    def test(self, _input: Dict[str, Result], task_run_context):
        params = {"myinput": _input, "output": {}}
        testcode = compile(self.test_source_code, '<string>', 'exec')
        e = exec(testcode, params)
        return params["output"]

    def task(self, _input: Dict[str, Result], task_run_context):
        params = {"myinput": _input, "output": {}}
        try:
            code = compile(self.source_code, '<string>', 'exec')
            data = exec(code, params)
            return Result(False,  params["output"], self, task_run_context)
        except Exception as e:
            return Result(True, tb.format_exc(), self, task_run_context)


class RNode(Node):
    def __init__(self, _node):
        super().__init__(_node)
        self.r_code = ''''''+_node["r_code"]+''''''

    def test(self, _input: Dict[str, Result], task_run_context):
        
        with self.ro.conversion.localconverter(self.ro.default_converter):
            r_inst = self.ro.r(self.r_code)
            r_test_exec = self.ro.globalenv['test_exec']
            global_params = {"r_test_exec": r_test_exec, "convert_R_to_py": convert_R_to_py,
                             "myinput": convert_py_to_R(_input), "output": {}}
            e = exec(
                f'output = convert_R_to_py(r_test_exec(myinput))', global_params)
        output = global_params["output"]
        return output

    def task(self, _input: Dict[str, Result], task_run_context):
        try:
            
            with self.ro.self.ro.conversion.localconverter(self.ro.default_converter):
                r_inst = self.ro.r(self.r_code)
                r_exec = self.ro.globalenv['exec']
                global_params = {"r_exec": r_exec, "convert_R_to_py": convert_R_to_py,
                                 "myinput": convert_py_to_R(_input), "output": {}}
                e = exec(f'output = convert_R_to_py(r_exec(myinput))',
                         global_params)
            output = global_params["output"]
            return Result(False,  output, self, task_run_context)
        except Exception as e:
            return Result(True, tb.format_exc(), self, task_run_context)


class CsvNode(Node):
    def __init__(self, _node):
        super().__init__(_node)
        self.file = _node["file"]
        self.name = _node["name"]
        self.delimiter = _node["delimiter"]
        self.names = _node["columns"]
        self.hasheader = _node["hasheader"]
        # self.types = _node["datatypes"]

    def _load_dask_dataframe(self):
        dd = importlib.import_module("dask.dataframe")
        if self.hasheader:
            # dtype=self.types
            dd_df = dd.read_csv(self.file, delimiter=self.delimiter)
            return dd_df
        else:
            dd_df = dd.read_csv(self.file, header=None, names=self.names,
                                delimiter=self.delimiter)  # dtype=self.types
            return dd_df

    def test(self, task_run_context):
        ddf = self._load_dask_dataframe()
        return ddf

    def task(self, task_run_context):
        try:
            ddf = self._load_dask_dataframe()
            return Result(False,  ddf, self, task_run_context)
        except Exception as e:
            return Result(True, tb.format_exc(), self, task_run_context)


class DbWriter(Node):
    def __init__(self, _node):
        super().__init__(_node)
        self.tablename = _node["dbtablename"]
        self.database = _node["database"] 
        self.dataframe = _node["dataframe"]
        self.use_cache_db = _node["use_cache_db"]

    def test(self, _input: Dict[str, Result], task_run_context):
        return False

    def task(self, _input: Dict[str, Result], task_run_context):
        input_element = _input
        
        dbutils_module = importlib.import_module("utils.DBUtils")
        admin_user = importlib.import_module("utils.types").UserType.ADMIN_USER
        dbutils = dbutils_module.DBUtils(use_cache_db=self.use_cache_db,
                                         database_code=self.database)
        dbconn = dbutils.create_database_engine(user_type=admin_user)

        try:
            for path in self.dataframe:
                input_element = input_element[path].data
            result = input_element.to_sql(
                self.tablename, dbconn, if_exists='replace')
            return Result(False,  result, self, task_run_context)
        except Exception as e:
            return Result(True, tb.format_exc(), self, task_run_context)


class DbQueryReader(Node):
    def __init__(self, _node):
        super().__init__(_node)
        self.sqlquery = _node["sqlquery"]
        self.database = _node["database"]
        self.testdata = {
            "columns": _node["columns"], "data": _node["testdata"]}
        self.use_cache_db = _node["use_cache_db"]

    def test(self, task_run_context):
        dd = importlib.import_module("dask.dataframe")
        return Result(False,  dd.from_pandas(pd.read_json(json.dumps(self.testdata), orient="split"), npartitions=1), self, task_run_context)

    def task(self, task_run_context):
        dd = importlib.import_module("dask.dataframe")
        # return dd.read_sql_query(sqlalchemy.select(sqlalchemy.text(self.sqlquery)), self.dbconn, self.index_col, divisions=self.divisions)
        dbutils_module = importlib.import_module("utils.DBUtils")
        read_user = importlib.import_module("utils.types").UserType.READ_USER
        dbutils = dbutils_module.DBUtils(use_cache_db=self.use_cache_db,
                                         database_code=self.database)
        dbconn = dbutils.create_database_engine(user_type=read_user)

        try:
            ddf = dd.from_pandas(pd.read_sql_query(
                self.sqlquery, dbconn), npartitions=1)
            return Result(False,  ddf, self, task_run_context)
        except Exception as e:
            return Result(True, tb.format_exc(), self, task_run_context)


class SqlQueryNode(Node):
    def __init__(self, _node):
        super().__init__(_node)
        self.sqlquery = _node["sqlquery"]
        if "testsqlquery" in _node:
            self.testsqlquery = _node["testsqlquery"]
        else:
            self.testsqlquery = _node["sqlquery"]
        self.params = {}
        self._is_select = _node["is_select"]
        if "params" in _node:
            self.params = _node["params"]
        self.database = _node["database"]
        self.use_cache_db = _node["use_cache_db"]

    def _map_input(self, _input):
        _params = {}
        for paramname in self.params.keys():
            input_element = _input
            for path in self.params[paramname]:
                input_element_a = input_element[path].data
            _params[paramname] = input_element_a
        return _params

    def _exec(self, _input: Dict[str, Result], sqlquery):
        _params = self._map_input(_input)
        res = None

        dbutils_module = importlib.import_module("utils.DBUtils")
        admin_user = importlib.import_module("utils.types").UserType.ADMIN_USER
        dbutils = dbutils_module.DBUtils(use_cache_db=self.use_cache_db,
                                         database_code=self.database)
        dbconn = dbutils.create_database_engine(user_type=admin_user)

        with dbconn.connect() as connection:
            if self._is_select:
                res = connection.execute(sqlalchemy.text(sqlquery), _params)
            else:
                connection.execute(sqlalchemy.text(sqlquery), _params)
            if res:
                rows = res.fetchall()
                query_results = [dict(row) for row in rows]
                return query_results
        return None

    def test(self, _input: Dict[str, Result], task_run_context):
        return self._exec(_input, self.testsqlquery, task_run_context)

    def task(self, _input: Dict[str, Result], task_run_context):
        try:
            df = self._exec(_input, self.sqlquery)
            return Result(False,  df, self, task_run_context)
        except Exception as e:
            return Result(True, tb.format_exc(), self, task_run_context)


# To do: link up with JSON from UI
class DataMappingNode(Node):
    def __init__(self, _node):
        super().__init__(_node)
        self.data_mapping = _node["data_mapping"]
        self.parent_table = _node["parent_table"]
        self.table_joins = _node["table_joins"]
        self.source_node_dfs = _node["tables"]

    def _create_query(self, _input):
        dask_utils = importlib.import_module("dask.utils")
        with dask_utils.tmpfile() as f:
            db = "sqlite:///%s" % f

            engine = create_engine(db, echo=False)
            metadata = MetaData()

            sqlalchemy_source_tables_dict = {}
            table_join_params = None
            select_params = []

            for mapping in self.data_mapping:
                source_table_name = mapping["input_table"]
                source_node_df = self.source_node_dfs[source_table_name]

                try:  # insert df into sqlite db
                    source_df = _input[source_node_df].data
                    source_df.to_sql(source_table_name,
                                     db,
                                     if_exists="replace",
                                     index=False)
                except Exception as e:
                    print(
                        f"Error inserting {source_node_df} df into sqlite db: {e}")

                # Create sqlalchemy table obj by autoloading from sqlite db
                sqlalchemy_table_obj = Table(
                    source_table_name, metadata, autoload_with=engine)

                if source_table_name == self.parent_table:
                    table_join_params = sqlalchemy_table_obj

                sqlalchemy_source_tables_dict[source_table_name] = sqlalchemy_table_obj

                # chain field mappings
                field_mappings = mapping["fields"]
                for field_mapping in field_mappings:
                    source_field_name = field_mapping["source_field"]
                    target_field_name = field_mapping["target_field"]
                    sqlalchemy_field_obj = getattr(
                        sqlalchemy_table_obj.c, source_field_name).label(target_field_name)
                    select_params.append(sqlalchemy_field_obj)

            # chain join mappings
            for join in self.table_joins:
                if join["right_table_name"] != self.parent_table:  # need to determine order of join
                    table_to_join = sqlalchemy_source_tables_dict[join["right_table_name"]]
                    table_join_against = sqlalchemy_source_tables_dict[join["left_table_name"]]
                    col_to_join_on = getattr(
                        table_to_join.c, join["right_table_join_on"])
                    col_to_join_against = getattr(
                        table_join_against.c, join["left_table_join_on"])
                    table_join_params = table_join_params.join(
                        table_to_join, col_to_join_on == col_to_join_against)
            query = select(*select_params).select_from(table_join_params)
            compiled_sql_query = str(query.compile(
                compile_kwargs={"literal_binds": True}))
            return str(compiled_sql_query)

    def test(self, _input: Dict[str, Result], task_run_context):
        return self.task(_input, task_run_context)

    def task(self, _input: Dict[str, Result], task_run_context):  # executes the retrieved sql query
        try:
            sql_query = self._create_query(_input)
            dask_sql_module = importlib.import_module("dask_sql")
            context = dask_sql_module.Context()
            for table_name in self.source_node_dfs.keys():
                context.create_table(
                    table_name, _input[self.source_node_dfs[table_name]].data)
            # temporarily log sql query because no UI
            result_df = context.sql(sql_query).compute()
        except Exception as e:
            return Result(True, tb.format_exc(), self, task_run_context)
        else:
            return Result(False,  result_df, self, task_run_context)


@flow(name="generate-nodes",
      flow_run_name="generate-nodes-flowrun",
      log_prints=True)
def generate_nodes_flow(graph, sorted_nodes):
    for nodename in sorted_nodes:
        node = graph["nodes"][nodename]
        nodetype = node["type"]

        # check if node is a subflow
        if nodetype == "subflow":
            subflow_obj = Flow(node)
            graph["nodes"][nodename]["nodeobj"] = subflow_obj
            for subflow_nodename in subflow_obj.sorted_nodes:
                subflow_nodegraph = subflow_obj.graph["nodes"][subflow_nodename]
                subflow_nodetype = subflow_nodegraph["type"]
                # create task run to generate node obj for each subflow node
                subflow_node_obj = generate_node_task(
                    subflow_nodename, subflow_nodegraph, subflow_nodetype)
                graph["nodes"][nodename]["graph"]["nodes"][subflow_nodename]["nodeobj"] = subflow_node_obj
        else:
            node_task_generation_wo = generate_node_task.with_options(
                on_completion=[partial(
                    node_task_generation_hook, **dict(nodename=nodename, nodetype=nodetype))],
                on_failure=[partial(node_task_generation_hook,
                                    **dict(nodename=nodename, nodetype=nodetype))]
            )

            nodeobj = node_task_generation_wo(nodename, node, nodetype)

            graph["nodes"][nodename]["nodeobj"] = nodeobj
    return graph


@task(task_run_name="generate-node-taskrun-{nodename}",
      log_prints=True
      )
def generate_node_task(nodename, node, nodetype):
    nodeobj = None
    # TODO: nodetype to make global variable
    match nodetype:
        case "csv_node":
            nodeobj = CsvNode(node)
        case "sql_node":
            nodeobj = SqlNode(node)
        case "python_node":
            nodeobj = PythonNode(node)
        case "r_node":
            nodeobj = RNode(node)
        case "db_writer_node":
            nodeobj = DbWriter(node)
        case "db_reader_node":
            nodeobj = DbQueryReader(node)
        case "sql_query_node":
            nodeobj = SqlQueryNode(node)
        case "data_mapping_node":
            nodeobj = DataMappingNode(node)
        case "time_at_risk_node":
            nodeobj = TimeAtRiskNode(node)
        case "cohort_diagnostic_node":
            nodeobj = CohortDiagnosticsModuleSpecNode(node)
        case "cohort_generator_node":
            nodeobj = CohortGeneratorSpecNode(node)
        case "characterization_node":
            nodeobj = CharacterizationModuleSpecNode(node)
        case "negative_control_outcome_cohort_node":
            nodeobj = NegativeControlOutcomeCohortSharedResource(node)
        case "target_comparator_outcomes_node":
            nodeobj = TargetComparatorOutcomes(node)
        case "cohort_method_analysis_node":
            nodeobj = CohortMethodAnalysis(node)
        case "default_covariate_settings_node":
            nodeobj = DefaultCovariateSettingsNode(node)
        case "study_population_settings_node":
            nodeobj = StudyPopulationArgs(node)
        case "cohort_incidence_target_cohorts_node":
            nodeobj = OutcomeDef(node)
        case "cohort_incidence_node":
            nodeobj = CohortIncidenceModuleSpec(node)
        case "cohort_definition_set_node":
            nodeobj = CohortDefinitionSharedResource(node)
        case "outcomes_node":
            nodeobj = CMOutcomes(node)
        case "cohort_method_node":
            nodeobj = CohortMethodModuleSpecNode(node)
        case "era_covariate_settings_node":
            nodeobj = EraCovariateSettings(node)
        case "seasonality_covariate_settings_node":
            nodeobj = SeasonalityCovariateSettingsNode(node)
        case "calendar_time_covariate_settings_node":
            nodeobj = CalendarCovariateSettingsNode(node)
        case "nco_cohort_set_node":
            nodeobj = NegativeControlCohortSet(node)
        case "self_controlled_case_series_analysis_node":
            nodeobj = SCCSAnalysis(node)
        case "self_controlled_case_series_node": 
            nodeobj = SCCSModuleSpec(node)
        case "patient_level_prediction_node":
            nodeobj = PLPModuleSpec(node)
        case "exposure_node":
            nodeobj = ExposuresOutcome(node)
        case "strategus_node":
            nodeobj = StrategusNode(node)
        case _:
            logging.error("ERR: Unknown Node "+node["type"])
            logging.error(tb.StackSummary())
    return nodeobj

def get_results_by_class_type(results: Dict[str, Result], nodeType: Node):
    return [results[o].data for o in results if not results[o].error and isinstance(results[o].node, nodeType)]

def get_input_nodes_by_class_type_from_results(inputs: Dict[str, Result], nodeType: Node) -> List[Node]:
    return [inputs[o].node for o in inputs if not inputs[o].error and isinstance(inputs[o].node, nodeType)]

def serialize_result_to_json(result: Result):
    return serialize_to_json(result.data)
