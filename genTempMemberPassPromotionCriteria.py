import os
import uuid
import psycopg2
from psycopg2 import sql

batch_name = "temp_member_pass_promotion_criteria"

# db_transaction_params = {
#     'host': os.environ.get("DB_TRANSACTION_HOST"),
#     'database': 'transaction',
#     'user': os.environ.get("DB_TRANSACTION_USER"),
#     'password': os.environ.get("DB_TRANSACTION_PASSWORD")
# }

data_insert_pass_member = "INSERT INTO temp_member_pass_promotion_criteria (id, privilege_id, member_id, member_code, member_fullname_th, source_of_creation, member_created_at, created_at, updated_at, status, reason) VALUES\n"
value_insert = "('{id}','{privilege_id}', '{member_uuid}', 'OW{running_number}', 'Perf Test Batch', 'SYSTEM', '2022-01-01 18:09:54.132', '2023-06-23 18:09:54.132', '2023-06-23 18:09:54.132', 'Processed', 'Test'),"

from tqdm import tqdm
generate_member_amount = 50000
for i in tqdm(range(int(generate_member_amount))):
    uuid_gen = uuid.uuid4()
    privilege_id_gen = uuid.uuid4()
    member_uuid_gen = uuid.uuid4()
    running_number = str(i).rjust(6, '0')

    #insert temp members promotion
    data_insert_pass_member += value_insert.format(running_number=running_number, id = uuid_gen, privilege_id = privilege_id_gen, member_uuid = member_uuid_gen) + '\n'

data_insert_pass_member = data_insert_pass_member[:-2]+';'

with open(f"./work/{batch_name}/gen_member_script.sql", 'w') as f:
    f.write(data_insert_pass_member)

# try:
#     connection = psycopg2.connect(**db_transaction_params)
#     cursor = connection.cursor()
#     print("Connected to the database " + db_transaction_params['database'])

#     sql_script = data_insert_member

#     cursor.execute(sql_script)
#     connection.commit()
#     print("SQL script: gen_member_script executed successfully!")

# except (Exception, psycopg2.Error) as error:
#     print("Error while connecting to the database or executing the script:", error)

# finally:
#     if connection:
#         cursor.close()
#         connection.close()
#         print("Database " + db_transaction_params['database'] +  " connection closed.")