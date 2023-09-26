import os
import uuid
import psycopg2
from psycopg2 import sql

batch_name = "temp_new_member_promotions"

db_member_params = {
    'host': os.environ.get("DB_MEMBER_HOST"),
    'database': 'member',
    'user': os.environ.get("DB_MEMBER_USER"),
    'password': os.environ.get("DB_MEMBER_PASSWORD")
}

# db_member_params = {
#     'host': 'localhost',
#     'database': 'member',
#     'user': 'postgres',
#     'password': 'postgres'
# }

print(os.environ.get("DB_MEMBER_HOST"))

data_insert_new_member = "INSERT INTO temp_new_member_promotions (id, member_id, member_code, member_status, member_fullname_th, member_firstname_th, member_lastname_th, source_of_creation, member_created_at, priority, created_at, updated_at, status) VALUES\n"
value_insert_new = "('{id}', '{member_uuid}', 'OW{running_number}', 'Registered', 'Perf Test Batch', 'Perf Test', 'Batch', 'SYSTEM', '2022-01-01 18:09:54.132', 1, '2023-06-23 18:09:54.132', '2023-06-23 18:09:54.132', 'Processed'),"

from tqdm import tqdm
generate_member_amount = 100
for i in tqdm(range(int(generate_member_amount))):
    uuid_gen = uuid.uuid4()
    member_uuid_gen = uuid.uuid4()
    running_number = str(i).rjust(6, '0')

    #insert temp new members promotion
    data_insert_new_member += value_insert_new.format(running_number=running_number, id = uuid_gen , member_uuid = member_uuid_gen) + '\n'

data_insert_new_member = data_insert_new_member[:-2]+';'

with open(f"./work/{batch_name}/gen_member_script.sql", 'w') as f:
    f.write(data_insert_new_member)

# try:
#     connection = psycopg2.connect(**db_member_params)
#     cursor = connection.cursor()
#     print("Connected to the database " + db_member_params['database'])

#     sql_script = data_insert_new_member

#     cursor.execute(sql_script)
#     connection.commit()
#     print("SQL script: gen_member_script executed successfully!")

# except (Exception, psycopg2.Error) as error:
#     print("Error while connecting to the database or executing the script:", error)

# finally:
#     if connection:
#         cursor.close()
#         connection.close()
#         print("Database " + db_member_params['database'] +  " connection closed.")