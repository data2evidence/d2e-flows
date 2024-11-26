from flows.loyalty_score_plugin.types import Concept_Standard

import sqlalchemy as sql
import pandas as pd

# from prefect import flow, task
# from prefect.logging import get_run_logger

concept_code = pd.read_csv(Concept_Standard)

def getcodes(feature:str):
    concept_ls = concept_code.loc[concept_code.variable==feature,'standard_concept'].astype(str).values
    concept_ls = "'" + "','".join(concept_ls) + "'"
    return concept_ls

# @task(log_prints=True)
def diagonis(data, conn, schema_name, index_st, index_ed):
    var_sql = sql.text(f'''
    SELECT person_id, count(person_id)
    FROM {schema_name}.condition_occurrence
    where condition_start_date < '{index_ed}' and condition_end_date > '{index_st}'
    group by person_id
    ''')
    record = conn.execute(var_sql).fetchall()
    data['Diagnosis_1'] = 0
    data['Diagnosis_2'] = 0
    for person_id, diag_count in record:    
        data.loc[data.person_id==person_id, 'Diagnosis_1'] = int(diag_count==1)
        data.loc[data.person_id==person_id, 'Diagnosis_2'] = int(diag_count>=2)

def medications(data, conn, schema_name, index_st, index_ed):
    var_sql = sql.text(f'''
    SELECT person_id, count(person_id)
    FROM {schema_name}.drug_exposure
    where drug_exposure_start_date < '{index_ed}' and drug_exposure_end_date > '{index_st}'
    group by person_id
    ''')
    record = conn.execute(var_sql).fetchall()
    data['Medication_1'] = 0
    data['Medication_2'] = 0
    for person_id, med_count in record:    
        data.loc[data.person_id==person_id, 'Medication_1'] = int(med_count==1)
        data.loc[data.person_id==person_id, 'Medication_2'] = int(med_count>=2)

def visits(data, conn, schema_name, index_st, index_ed):
    # TODO: improve coding
    # ED_visit
    concept_ids = getcodes('ED Visit')
    var_sql = sql.text(f'''
    SELECT person_id, count(person_id)
    FROM {schema_name}.visit_occurrence
    where visit_concept_id in ({concept_ids})
    and visit_start_date < '{index_ed}' and visit_end_date > '{index_st}'
    group by person_id
    ''')
    record = conn.execute(var_sql).fetchall()
    data['ED_Visit'] = 0
    for person_id, visit_count in record:
        data.loc[data.person_id==person_id, 'ED_Visit'] = int(visit_count>=1)

    # In/OutPatien_visit
    concept_ids = ','.join([getcodes('Inpatient Visit'), getcodes('Outpatient Visit'), getcodes('Either In/Outpatient Visit')])
    var_sql = sql.text(f'''
    SELECT person_id, count(person_id)
    FROM {schema_name}.visit_occurrence
    where visit_concept_id in ({concept_ids})
    and visit_start_date < '{index_ed}' and visit_end_date > '{index_st}'
    group by person_id
    ''')
    record = conn.execute(var_sql).fetchall()
    data['InOutpatient_1'] = 0
    for person_id, visit_count in record:
        data.loc[data.person_id==person_id, 'InOutpatient_1'] = int(visit_count>=1)

    # Outpatient_visit
    concept_ids_op = getcodes('Outpatient Visit')
    concept_ids_ep = getcodes('Either In/Outpatient Visit')
    var_sql = sql.text(f'''
    SELECT person_id, count(person_id)
    FROM {schema_name}.visit_occurrence
    where (visit_concept_id in ({concept_ids_op})) or (visit_concept_id in ({concept_ids_ep}) and visit_start_date = visit_end_date)
    and visit_start_date < '{index_ed}' and visit_end_date > '{index_st}'
    group by person_id
    ''')
    record = conn.execute(var_sql).fetchall()
    data['Outpatient_2'] = 0
    for person_id, visit_count in record:
        data.loc[data.person_id==person_id, 'Outpatient_2'] = int(visit_count>=2)

def same_MD(data, conn, schema_name, index_st, index_ed):
    concept_ids = ','.join([getcodes('Inpatient Visit'), 
                        getcodes('Outpatient Visit'), 
                        getcodes('Either In/Outpatient Visit'), 
                        getcodes('Insurance Examine Visit'),
                        getcodes('MD Visit'),
                        getcodes('ED Visit')
                       ])

    # same_MD_2
    var_sql = sql.text(f'''
    SELECT person_id
    FROM (SELECT person_id, provider_id, count(person_id)
        FROM {schema_name}.visit_occurrence
        where visit_concept_id in ({concept_ids})
        and visit_start_date < '{index_ed}' and visit_end_date > '{index_st}'
        group by person_id, provider_id
        ) as visit
    WHERE visit.count=2
    ''')
    record = conn.execute(var_sql).fetchall()
    data['Visit_Same_MD_2'] = 0
    for person_id in record:
        data.loc[data.person_id==person_id, 'Visit_Same_MD_2'] = 1

    # same_MD_3
    var_sql = sql.text(f'''
    SELECT person_id
    FROM (SELECT person_id, provider_id, count(person_id)
        FROM {schema_name}.visit_occurrence
        where visit_concept_id in ({concept_ids})
        and visit_start_date < '{index_ed}' and visit_end_date > '{index_st}'
        group by person_id, provider_id
        ) as visit
    WHERE visit.count>=3
    ''')
    record = conn.execute(var_sql).fetchall()
    data['Visit_Same_MD_3'] = 0
    for person_id in record:
        data.loc[data.person_id==person_id, 'Visit_Same_MD_3'] = 1

class routine():
    def __init__(self, data, conn, schema_name, index_st, index_ed):
        self.rountine_facts = ['A1C', 'BMI', 'Colonoscopy', 'Fecal_Occult_Test', 'Flu_Shot',
       'Mammography', 'Medical_Exam', 'Pap_Test', 'Pneumococcal_Vaccine',
       'PSA_Test']
        self.conn = conn
        self.data = data
        self.index_st = index_st
        self.index_ed = index_ed
        self.schema_name = schema_name
        
    def get_routine(self, fact):
        concept_ids = getcodes(fact)

        if fact == 'BMI':
            var_sql = sql.text(f'''
            SELECT COALESCE(o.person_id, m.person_id) as person_id
            FROM 
                (SELECT DISTINCT person_id
                FROM {self.schema_name}.measurement 
                Where measurement_concept_id in ({concept_ids})
                and measurement_date between '{self.index_st}' and '{self.index_ed}') m
            FULL OUTER JOIN 
                (SELECT DISTINCT person_id
                FROM {self.schema_name}.observation
                Where observation_concept_id in ({concept_ids})
                and observation_date between '{self.index_st}' and '{self.index_ed}') o
            ON o.person_id = m.person_id
            ''')
        elif fact in ['Colonoscopy','Mammography','Medical_Exam','Pap_Test']:
            var_sql = sql.text(f'''
            SELECT COALESCE(p.person_id, m.person_id) as person_id
            FROM 
                (SELECT DISTINCT person_id
                FROM {self.schema_name}.measurement 
                Where measurement_concept_id in ({concept_ids})
                and measurement_date between '{self.index_st}' and '{self.index_ed}') m
            FULL OUTER JOIN 
                (SELECT DISTINCT person_id
                FROM {self.schema_name}.procedure_occurrence
                Where procedure_concept_id in ({concept_ids})
                and procedure_date between '{self.index_st}' and '{self.index_ed}') p
            ON p.person_id = m.person_id
            ''')
        elif fact in ['Flu_Shot','Pneumococcal_Vaccine']:
            var_sql = sql.text(f'''
            SELECT COALESCE(o.person_id, m.person_id) as person_id
            FROM 
                (SELECT DISTINCT person_id
                FROM {self.schema_name}.drug_exposure
                Where drug_concept_id in ({concept_ids})
                and drug_exposure_start_date < '{self.index_ed}' and drug_exposure_end_date > '{self.index_st}') m
            FULL OUTER JOIN 
                (SELECT DISTINCT person_id
                FROM {self.schema_name}.observation
                Where observation_concept_id in ({concept_ids})
                and observation_date between '{self.index_st}' and '{self.index_ed}') o
            ON o.person_id = m.person_id
            ''')
        else:
            var_sql = sql.text(f'''
            SELECT DISTINCT person_id
            FROM {self.schema_name}.measurement 
            Where measurement_concept_id in ({concept_ids})
            and measurement_date between '{self.index_st}' and '{self.index_ed}'
            ''')
        
        record = self.conn.execute(var_sql).fetchall()
        self.data[fact] = 0
        for person_id in record:
            self.data.loc[self.data.person_id==person_id, fact] = 1

    def run(self):
        for fact in self.rountine_facts:
            self.get_routine(fact)

        self.data['Routine_Care_2'] = 0
        self.data.loc[self.data[self.rountine_facts].sum(axis=1)>=2, 'Routine_Care_2'] = 1
        return self.data