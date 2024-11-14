def get_version_from_tag(tag: str) -> str:
    return tag[1:4].replace(".", "-")

def path_to_ant(tag: str) -> str:
    return f"/app/i2b2-data-{tag[1:]}/edu.harvard.i2b2.data/Release_{get_version_from_tag(tag)}"

def get_patient_count(dbdao) -> str:
    try:
        patient_count = dbdao.get_distinct_count("patient_dimension", "patient_num")
    except Exception as e:
        error_msg = f"Error retrieving patient count"
        print(f"{error_msg}: {e}")
        patient_count = error_msg
    return str(patient_count)

def get_patient_count(dbdao) -> str:
    try:
        patient_count = dbdao.get_distinct_count("patient_dimension", "patient_num")
    except Exception as e:
        error_msg = f"Error retrieving patient count"
        print(f"{error_msg}: {e}")
        patient_count = error_msg
    return str(patient_count)

def get_metadata_date(dbdao, column_name: str) -> str:
    try:
        metadata_date = str(dbdao.get_value('dataset_metadata', column_name)).split(" ")[0]
    except Exception as e:
        error_msg = f"Error retrieving created {column_name}"
        print(f"{error_msg}: {e}")
        metadata_date = error_msg
    return metadata_date
        
def get_metadata_version(dbdao, column_name: str) -> str:
    try:
        metadata_version = dbdao.get_value('dataset_metadata', column_name)
    except Exception as e:
        error_msg = f"Error retrieving created {column_name}"
        print(f"{error_msg}: {e}")
        metadata_version = error_msg
    return metadata_version