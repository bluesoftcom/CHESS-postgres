#!/bin/bash
data_mode='postgres'
data_path="data/postgres/victim_dataset.json"
pipeline_nodes='keyword_extraction+entity_retrieval+context_retrieval+column_filtering+table_selection+column_selection+candidate_generation+revision+evaluation'
checkpoint_nodes=''
checkpoint_dir=""

entity_retieval_mode='ask_model' # Options: 'corrects', 'ask_model'
context_retrieval_mode='vector_db' # Options: 'corrects', 'vector_db'
top_k=5
table_selection_mode='ask_model' # Options: 'corrects', 'ask_model'
column_selection_mode='ask_model' # Options: 'corrects', 'ask_model'

engine2='gpt-4o-mini'
engine3='gpt-4o-latest'

pipeline_setup='{
    "keyword_extraction": {
        "engine": "'${engine2}'",
        "temperature": 0.2,
        "base_uri": ""
    },
    "entity_retrieval": {
        "mode": "'${entity_retieval_mode}'"
    },
    "context_retrieval": {
        "mode": "'${context_retrieval_mode}'",
        "top_k": '${top_k}'
    },
    "column_filtering": {
        "engine": "'${engine2}'",
        "temperature": 0.0,
        "base_uri": ""
    },
    "table_selection": {
        "mode": "'${table_selection_mode}'",
        "engine": "'${engine3}'",
        "temperature": 0.0,
        "base_uri": "",
        "sampling_count": 1
    },
    "column_selection": {
        "mode": "'${column_selection_mode}'",
        "engine": "'${engine3}'",
        "temperature": 0.0,
        "base_uri": "",
        "sampling_count": 1
    },
    "candidate_generation": {
        "engine": "'${engine3}'",
        "temperature": 0.0,
        "base_uri": "",
        "sampling_count": 1
    },
    "revision": {
        "engine": "'${engine3}'",
        "temperature": 0.0,
        "base_uri": "",
        "sampling_count": 1
    }
}'

# PostgreSQL connection details
db_type="postgres"
db_name="postgres"
db_user="postgres"
db_password="example"
db_host="localhost"
db_port="5433"

echo -e "${run_name}"
python3 -u ./src/main.py --data_mode ${data_mode} --data_path ${data_path} \
        --pipeline_nodes ${pipeline_nodes} --pipeline_setup "$pipeline_setup" \
        --db_type ${db_type} --db_name ${db_name} --db_user ${db_user} \
        --db_password ${db_password} --db_host ${db_host} --db_port ${db_port}
        # --use_checkpoint --checkpoint_nodes ${checkpoint_nodes} --checkpoint_dir ${checkpoint_dir}