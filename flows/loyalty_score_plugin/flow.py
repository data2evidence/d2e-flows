from flows.loyalty_score_plugin.types import *
from flows.loyalty_score_plugin.features import *

from sklearn.linear_model import Lasso
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score
from shared_utils.dao.DBDao import DBDao
import sqlalchemy as sql
from datetime import datetime
import pandas as pd

from prefect import flow, task
from prefect.logging import get_run_logger

@flow(log_prints=True)
def loyalty_score_plugin(options:LoyaltyPluginType):
    logger = get_run_logger("")
    match options.mode:
        case FlowActionType.LOYALTY_SCORE:
            calculate_loyalty_score(options)
        case FlowActionType.RETRAIN_ALGO:
            if options.returnYears > 0:
                retrain_algo(options)
            else:
                error_msg = f"'return_years' should > 0 when select 'retrain_algo'"
                logger.error(error_msg)    

def load_coef_table(conn, coeff_table_name, schema_name):
    if coeff_table_name:
        coef = pd.read_sql_table(
                table_name = coeff_table_name,
                con = conn,
                schema = schema_name
                )
        coef.set_index('Feature',inplace=True)
    else:         
        coef = pd.read_json(Coefficeints, orient='index')
    feature = list(coef.index.values)
    feature.remove('Intercept')
    return coef, feature

def calculate_loyalty_score(options:LoyaltyPluginType):
    logger = get_run_logger()
    loyalty_cohort_table = options.loyaltycohortTableName
    coeff_table_name = options.coeffTableName
    index_date = options.indexDate
    lookback_years =  options.lookbackYears
    database_code = options.databaseCode
    schema_name = options.schemaName
    use_cache_db = options.use_cache_db

    index_datetime = datetime.fromisoformat(index_date)
    cal_st = index_datetime.replace(year=index_datetime.year-lookback_years).strftime("%Y-%m-%d")
    cal_ed = index_datetime.strftime("%Y-%m-%d")
    data, conn, engine = data_prep(cal_st, cal_ed, database_code, schema_name, use_cache_db)
    coef, feature = load_coef_table(conn, coeff_table_name, schema_name)
    data['loyalty_score'] = data[feature].dot(coef.loc[feature]) + coef.loc['Intercept']
    logger.info(f'Loyalty score calculation completed')
    logger.info(f'The loyalty cohort is stored {schema_name}.{loyalty_cohort_table}')
    data.to_sql(name = loyalty_cohort_table,
                con = engine,
                schema = schema_name,
                if_exists = 'replace',
                index = False,
                chunksize = 32,
                )
    conn.close()
        
def retrain_algo(options:LoyaltyPluginType):
    logger = get_run_logger()
    retrain_coeff_table_name = options.retrainCoeffTableName
    index_date = options.indexDate
    lookback_years =  options.lookbackYears
    return_years = options.returnYears
    database_code = options.databaseCode
    schema_name = options.schemaName
    use_cache_db = options.use_cache_db
    test_ratio = options.testRatio

    index_datetime = datetime.fromisoformat(index_date)
    train_st = index_datetime.replace(year=index_datetime.year-lookback_years-return_years).strftime("%Y-%m-%d")
    train_ed = index_datetime.replace(year=index_datetime.year-return_years).strftime("%Y-%m-%d")
    data, conn, engine = data_prep(train_st, train_ed, database_code, schema_name, use_cache_db)
    feature = list(set(data.columns) - set(['person_id']))
    # Achieve gold labels
    var_sql = sql.text(f'''
    SELECT person_id, count(*)
    FROM {schema_name}.visit_occurrence
    where visit_start_date > '{train_ed}' and visit_end_date <= '{index_date}'
    group by person_id
    ''')
    record = conn.execute(var_sql).fetchall()
    data['Return'] = 0
    for person_id, visit_count in record:
        data.loc[data.person_id==person_id, 'Return'] = int(visit_count>=1)

    X_train, X_test, y_train, y_test = train_test_split(data[feature], data.Return, test_size=test_ratio, 
                                                        random_state=42, shuffle=True,
                                                        stratify=data.Return)
    lasso = Lasso(alpha=0.005, random_state=42)
    lasso.fit(X_train, y_train)
    coef_retrain = pd.DataFrame(data=lasso.coef_, index=feature, columns=['coeff'])
    coef_retrain.loc['Intercept'] = lasso.intercept_
    logger.info(f'Algorithm retrain completed')
    logger.info(f'Retrain coefficients are stored at {schema_name}.{retrain_coeff_table_name}')
    coef_retrain.to_sql(name = retrain_coeff_table_name,
                con = engine,
                schema = schema_name,
                if_exists = 'replace',
                index = True,
                index_label='Feature',
                chunksize = 32,
                )
    y_pred = lasso.predict(X_test)
    auc_roc = roc_auc_score(y_test, y_pred)
    summary_table = pd.DataFrame.from_dict({'auc_roc_retrain': auc_roc},orient='index', columns=['value'])
    summary_table.to_sql(name = f'{retrain_coeff_table_name}_summary_table',
                con = engine,
                schema = schema_name,
                if_exists = 'replace',
                chunksize = 32,
                index=True,
                index_label='Metric'
                )
    logger.info(f'Retrain auc roc is stored at {schema_name}.{retrain_coeff_table_name}_summary_table')
    conn.close()


@task(log_prints=True)
def data_prep(index_st, index_ed, database_code, schema_name, use_cache_db):
    index_st_datetime = datetime.fromisoformat(index_st)
    age18 = index_st_datetime.replace(year=index_st_datetime.year-18).strftime("%Y-%m-%d")
    logger = get_run_logger()
    logger.info("Start the connection to database")
    dbdao = DBDao(use_cache_db=use_cache_db,
                  database_code=database_code, 
                  schema_name=schema_name)
    engine = dbdao.engine
    conn = engine.connect()
    basic_para = {'conn': conn, 
                'schema_name': schema_name, 
                'index_st':index_st,
                'index_ed':index_ed}
    data = eligible_person(age18=age18, **basic_para)
    diagonis(data, **basic_para)
    medications(data, **basic_para)
    visits(data, **basic_para)
    same_MD(data, **basic_para)
    data = routine(data, **basic_para).run()
    logger.info('Data preparation completed')
    return data, conn, engine

@task(log_prints=True)
def eligible_person(conn, schema_name, index_st, index_ed, age18):
    exclude_sql = sql.text(f'''
        SELECT DISTINCT age.person_id
        FROM (
            SELECT p.person_id
            FROM {schema_name}.person p
            LEFT JOIN {schema_name}.death d ON p.person_id = d.person_id
            WHERE 
                TO_DATE(concat(year_of_birth, '-', month_of_birth, '-', day_of_birth), 'yyyy-mm-dd') < '{age18}'
                AND
                (d.person_id is null) or (d.death_date > '{index_ed}')
            ) age
        INNER JOIN (
        Select person_id, count(*)
        from {schema_name}.visit_occurrence 
        where visit_start_date < '{index_ed}' and visit_end_date > '{index_st}'
        group by person_id
        having count(*) >= 1
        ) v
        ON age.person_id = v.person_id
        ''')

    record = conn.execute(exclude_sql).fetchall()
    person_id = [x[0] for x in record]
    data = pd.DataFrame(person_id,columns=['person_id'])
    return data

if __name__ == '__main__':
    database_name = "alpdev_pg"
    schema_name = "cdmdefault"
    mode = "calculate_loyalty_score"
    loyalty_cohort_table = 'debug_cohort_table'
    coefficeint_table = 'debug'
    retrain_Coef_Name = 'debug_retrain_coef'

    index_date = '2011-11-11'
    lookback_years = 2
    test_ratio = 0.2

    options = LoyaltyPluginType(
        schemaName = schema_name,
        databaseCode = database_name,
        indexDate = index_date,
        lookbackYears = lookback_years,
        # returnYears = 0,
        testRatio = test_ratio,
        coeffTableName = coefficeint_table,
        loyaltycohortTableName = loyalty_cohort_table,
        retrainCoeffTableName = retrain_Coef_Name

    )
    loyalty_score_plugin(options)


