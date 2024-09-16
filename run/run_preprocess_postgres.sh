# Define variables
db_type=postgres
db_root_directory="./data/postgres/databases" # UPDATE THIS WITH THE PATH TO THE PARENT DIRECTORY OF THE DATABASES
db_id="victim" # Options: all or a specific db_id
verbose=true
signature_size=100
n_gram=3
threshold=0.01

# Add PostgreSQL-specific variables
db_name="postgres"
db_user="postgres"
db_password="example"
db_host="localhost"
db_port=5433

# Run the Python script with the defined variables
python3 -u ./src/preprocess.py --db_type "${db_type}" \
                              --db_root_directory "${db_root_directory}" \
                              --signature_size "${signature_size}" \
                              --n_gram "${n_gram}" \
                              --threshold "${threshold}" \
                              --db_id "${db_id}" \
                              --verbose "${verbose}" \
                              --db_name "${db_name}" \
                              --db_user "${db_user}" \
                              --db_password "${db_password}" \
                              --db_host "${db_host}" \
                              --db_port "${db_port}"