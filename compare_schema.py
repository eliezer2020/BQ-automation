from google.cloud import bigquery
import sys
from google.oauth2 import service_account
from google.cloud import storage

# Path to the service account credential file
credential_path = 'credential-dev.json'
   
# Create a credentials object from the service account file
credentials = service_account.Credentials.from_service_account_file(credential_path)
client=bigquery.Client(credentials=credentials)

class BqColumn:
    def __init__(self, name, datatype, oldDatatype):
        self.name = name
        self.datatype = datatype
        self.oldDatatype = oldDatatype

different_columns_bq=[]
exactly_same_columns_bq =[]
different_column_types_bq = []


def get_schema(project_id, dataset_id, table_id):
    
    table_ref = client.dataset(dataset_id).table(table_id)
    table = client.get_table(table_ref)
    return table.schema

def updateOldTable(project_id, dataset_id,table_id, newColumns):

    for column in newColumns:
        sqlStatement=f"ALTER TABLE `{project_id}.{dataset_id}.{table_id}` ADD COLUMN {column.name} {column.datatype}"
        handleSQLexecution(sqlStatement)

def typeCasting(attributteBQ):
    if attributteBQ.datatype=="TIMESTAMP" and attributteBQ.oldDatatype=='INTEGER':
        return f"TIMESTAMP_MILLIS({attributteBQ.name}) as {attributteBQ.name},\n"
    elif attributteBQ.datatype=="TIMESTAMP" and attributteBQ.oldDatatype=='STRING':
        return f"CAST ({attributteBQ.name} as TIMESTAMP) as {attributteBQ.name},\n"
    elif attributteBQ.datatype=="INTEGER" and attributteBQ.oldDatatype=='STRING':
         return f"SAFE_CAST({attributteBQ.name} AS INT64) as {attributteBQ.name},\n"
    elif attributteBQ.datatype=="STRING" and attributteBQ.oldDatatype=='BOOLEAN':
        return f"CAST({attributteBQ.name} AS STRING) as {attributteBQ.name},\n"

    else: 
        return "NEED TO INTRODUCE TYPE CASTING: " + attributteBQ.oldDatatype + " <-> " + attributteBQ.datatype + " for attribute: " + attributteBQ.name + "\n" 


def castColumns(different_column_types_bq):
    castedColumns=""
    for column in different_column_types_bq:
        castedColumns=castedColumns+typeCasting(column)
    return castedColumns

def generateWhereDate(project_id, dataset_id, table1_id, table2_id):
    #get first laminar record inserted Timestamp
    tsFirstLaminarRecordSQL= f"\nselect operation_ts from  `{project_id}.{dataset_id}.{table2_id}` order by  operation_ts asc limit 1"
    tsLaminarRaws=handleSQLexecution(tsFirstLaminarRecordSQL)
    tsFirstLaminarRaw=next(iter(tsLaminarRaws))
    tsFirstLaminarInt=tsFirstLaminarRaw.operation_ts
    print(f"\n\nfirst inserted laminar record {tsFirstLaminarInt}")
    return f"WHERE operation_ts < TIMESTAMP_MILLIS ({tsFirstLaminarInt})"

    



def handleSQLexecution(sqlStatement):
    print(sqlStatement)
    query_job=client.query(sqlStatement)
    rowResults=query_job.result()

    if query_job.errors:
        print("ERRORS IN SQL EXECUTION")
        for error in query_job.errors:
            print(error['message'])
    else:
        print("SUCCESS :) ")

    return rowResults

  



def generateNewView(project_id, dataset_id, table1_id, table2_id ):
    
    viewDataset=f"{dataset_id}_vm"
    initSQLStatement= f"\n\nCREATE OR REPLACE VIEW `{project_id}.{viewDataset}.{table1_id}` AS SELECT "
    sameColumnString=""
    whereDateCondition=""
    sameColumnDifferentDataType=""
    for column in exactly_same_columns_bq:
        sameColumnString= sameColumnString+f"{column.name},\n"
    for column in different_column_types_bq:
        sameColumnDifferentDataType= sameColumnDifferentDataType+f"{column.name},\n"
    whereDateCondition=generateWhereDate(project_id, dataset_id, table1_id, table2_id)    
    intermediateSQLstatement= f"FROM `{project_id}.{dataset_id}.{table1_id}` {whereDateCondition} UNION ALL SELECT {sameColumnString} "
    castingSQLstatement= castColumns(different_column_types_bq)
    finalQueryStatement=f"FROM `{project_id}.{dataset_id}.{table2_id}`;"
    
    sqlStatement=initSQLStatement+sameColumnString+sameColumnDifferentDataType+intermediateSQLstatement+castingSQLstatement+finalQueryStatement
    
    handleSQLexecution(sqlStatement)

    
    
    


    
    




def compare_schemas(project_id, dataset_id, table1_id, table2_id):
    table1_schema = get_schema(project_id, dataset_id, table1_id)
    table2_schema = get_schema(project_id, dataset_id, table2_id)

    if table1_schema == table2_schema:
        print(f"The schemas of {table1_id} and {table2_id} are identical.")
    else:
        print(f"The schemas of {table1_id} and {table2_id} are different.")

        table1_columns = {field.name: field.field_type for field in table1_schema}
        table2_columns = {field.name: field.field_type for field in table2_schema}

        different_columns = set(table1_columns.keys()).symmetric_difference(set(table2_columns.keys()))
        
        for column in different_columns:
            if(column in table2_columns and table2_columns[column]!='RECORD'):
                different_columns_bq.append(BqColumn(column, table2_columns[column],"NA"))


        print("\nDIFFERENT COLUMN:")
        for column in different_columns_bq:

            print(vars(column))
        updateOldTable(project_id, dataset_id, table1_id,different_columns_bq)
        
   
       

        same_columns = set(table1_columns.keys()).intersection(set(table2_columns.keys()))
        
        for column in same_columns:
            if table1_columns[column] != table2_columns[column]:
               different_column_types_bq.append(BqColumn(column,table1_columns[column],table2_columns[column]))
            else:
                exactly_same_columns_bq.append(BqColumn(column,table1_columns[column],"NA"))

        print("\nSAME COLUMN BUT DIFFERENT DATATYPE:")
        for column in different_column_types_bq:
            print(vars(column))

        print("\nSAME COLUMNS:")
        for column in exactly_same_columns_bq:
            print(vars(column))

        generateNewView(project_id, dataset_id, table1_id,table2_id)
    


      

if len(sys.argv) != 5:
    print("Usage: python compare_schemas.py project_id dataset_id table1_id table2_id")
    sys.exit(1)

project_id = sys.argv[1]
dataset_id = sys.argv[2]
table1_id = sys.argv[3]
table2_id = sys.argv[4]

compare_schemas(project_id, dataset_id, table1_id, table2_id)
