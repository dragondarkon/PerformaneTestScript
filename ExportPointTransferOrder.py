import os
import uuid
import psycopg2
from psycopg2 import sql

batch_name = "export_point_transfer_orders"

db_member_params = {
    'host': os.environ.get("DB_MEMBER_HOST"),
    'database': 'member',
    'user': os.environ.get("DB_MEMBER_USER"),
    'password': os.environ.get("DB_MEMBER_PASSWORD")
}

db_transaction_params = {
    'host': os.environ.get("DB_TRANSACTION_HOST"),
    'database': 'transaction',
    'user': os.environ.get("DB_TRANSACTION_USER"),
    'password': os.environ.get("DB_TRANSACTION_PASSWORD")
}

data_insert_member = "INSERT INTO members (member_id, status, nationality, national_id, passport_number, firstname_th, lastname_th, firstname_en, lastname_en, birth_date, gender, mobile_number, email, personal_income, education, occupation, occupation_text, marital_status, vehicle_type_1, vehicle_license_plate_1, vehicle_type_2, vehicle_license_plate_2, vehicle_type_3, vehicle_license_plate_3, address_id, delivery_address_id, referred_by, created_at, updated_at, tier_id, tier_action, tier_action_date, tier_point, tier_point_updated_at, next_tier_minimum_point, is_upgradeable, is_downgradeable, id, referred_by_id, last_tier_action_at, tier_maintain_point, is_test_member, primary_card_id, primary_card_no, all_card_no, nanopass_id, member_point, source_of_creation, member_point_earned_today, created_by_id, created_by_name, last_modified_by_id, last_modified_by_name, pending_to_cancel_reason, pending_to_cancel_remark, pending_to_cancel_amount, pending_to_cancel_at, pending_to_cancel_by_id, pending_to_cancel_by_name, uncancelled_remark, status_before_pending_to_cancel, cancelled_at, suspended_reason, suspended_remark, suspended_amount, suspended_at, suspended_by_id, suspended_by_name, unsuspended_remark, status_before_suspended, terminated_reason, terminated_remark, terminated_amount, terminated_at, terminated_by_id, terminated_by_name, unterminated_remark, status_before_terminated, last_transaction_at, mid, tid, store_id, store_name, store_terminal_id, registered_date, first_login_date, language_preference, is_welcome_email_sent, is_welcome_sms_sent, created_remarks, migrated_remarks, cobrand_address, lifetime_points_earned, total_points_earned, total_points_redeemed, pending_points, facebook, line, member_point_updated_at, preference_center_url, last_modified_at, fullname_th, fullname_en, is_aoa_member, old_member_id, tier_check_point, tier_expiry_date, next_tier_name, next_tier_level, is_privilege_unavailable, is_customize, tier_point_expired_warning_at, tier_reward_warning_at, tier_action_warning_at, member_point_earned_today_date, is_blue_connect_member) VALUES\n"
value_insert = "('{id}', '{member_uuid}', 'OW{running_number}', 'Registered', 'Perf Test Batch', 'Perf Test', 'Batch', 'SYSTEM', '2022-01-01 18:09:54.132', 1, '2023-06-23 18:09:54.132', '2023-06-23 18:09:54.132', 'Processed'),"
uuid_gen = uuid.uuid4()
member_uuid = uuid.uuid4()
running_number = str(0).rjust(6, '0')
data_insert_member += value_insert.format(running_number=running_number, id = uuid_gen, member_uuid= member_uuid) + '\n'
data_insert_member = data_insert_member[:-2]+';'

delete_member_script = "DELETE FROM members WHERE id = '{member_uuid}');".format(member_uuid= member_uuid)

partner_uuid = uuid.uuid4()
data_insert_partners = "INSERT INTO loyalty_partners (id,code,name,start_at,end_at,description,is_batch,hq_allocated_type,exchange_rate_to_or,exchange_rate_to_partner,point_step_to_partner,point_amount_to_partner,point_step_to_or,point_amount_to_or,minimum_point_to_partner,currency,conversion_rate_to_partner,conversion_rate_to_or,max_point_per_month_per_member_to_partner,max_point_per_month_per_member_to_or,max_point_per_transaction_per_member_to_partner,max_point_per_campaign_to_partner,max_point_per_campaign_to_or,created_at,updated_at,max_point_per_transaction_per_member_to_or,status,created_by_id,created_by_name,last_modified_at,last_modified_by_id,last_modified_by_name,remark,partner_user_id,partner_endpoint) VALUES\n"
value_insert_partner = "('{partner_uuid}','PN-Perf-Test-01','Perf Test Partner','2022-07-29 00:00:00','2999-12-31 00:00:00',NULL,false,'HQ Allocated',NULL,NULL,100,20,60,120,600,'THB',0.35,0.09,NULL,NULL,NULL,NULL,NULL,'2022-09-11 12:00:00','2022-09-11 12:00:00',NULL,'ACTIVE',NULL,'SYSTEM','2023-04-11 17:10:24.94329',NULL,'SYSTEM',NULL,NULL,NULL);"
data_insert_partners += value_insert_partner.format(partner_uuid = partner_uuid) 

data_insert_orders = "INSERT INTO orders (id,order_code,member_id,member_fullname_th,card_id,card_no,is_pos_online,pos_order_id,batch_id,stand_id,store_id,store_name,store_station_name,store_business_type_id,store_business_type_name,store_owner_type_id,store_owner_type_name,store_company_id,store_company_name,mid,tid,store_shop_ship_to,store_shop_sold_to,status,ordered_at,settlemented_at,reconciled_at,confirmed_at,reversed_at,voided_at,cancelled_at,adjusted_at,created_by_name,created_by_id,created_at,last_modified_by_name,last_modified_by_id,updated_at,is_get_card_no_by_mobile_no,source_of_data,member_code,amount_applied,amount_applied_unit,total_amount_before_discount,base_point,extra_point,redemption_point,total_amount_after_discount,is_over_limitation,over_limitation_reason_backoffice,member_firstname_th,member_lastname_th,discount,store_terminal_id,total_product_discount,over_limitation_reason,product_business_type_id,product_business_type_name,hq_allocated_type,oil_product_id,cancelled_remark,redemption_amount,redemption_rate,redemption_rule_id,last_modified_at,reward_detail,privilege_order_status,mobile_no,email,privilege_id,privilege_type_key,privilege_activity_type_id,privilege_name,coupon_master_id,coupon_master_name,coupon_code,privilege_order_quantity,promotion_code_mass,promotion_name_mass,promotion_period_mass,applicable_days_mass,applicable_time_mass,promotion_code_personalized,promotion_name_personalized,promotion_period_personalized,applicable_days_personalized,applicable_time_personalized,order_type,point_transfer_code,loyalty_partner_code,accrued_amount,accrued_rate,partner_point,currency,partner_transaction_id,partner_card_no,last_edited_at,is_over_limitation_mass_promotion,is_over_limitation_personalized_promotion,over_limitation_mass_promotion_reason,over_limitation_personalized_promotion_reason,privilege_activity_type_code,privilege_activity_type_name,reward_status,remarks,base_accrual_rule_name,mass_promotion_point,personalized_promotion_point,privilege_no,reward_name,reward_receiver_name,reward_receiver_mobile_number,reward_cancelled_at,reward_returned_at,vendor_type,delivery_method,address_detail,subdistrict,subdistrict_id,district,district_id,province,province_id,country,country_id,postal_code,point_updated_at,loyalty_partner_name,mka_campaign_id,mka_offer_id,reward_store_id,is_effect_to_tier_point,public_remark,reward_store_name,reward_store_code,type_member_privilege_limit,type_privilege_limit,is_promotion_mass_accrued,is_promotion_personalized_accrued,coupon_id,reward_type,mka_treatmentcode,request_session_id,offer_start_date,privilege_just_for_you_member_id,mka_order_status,returned_at,friend_member_id,friend_member_mobile_no,friend_member_fullname_th,partner_transaction_id_out,partner_reference_no) VALUES\n"
value_insert_order = "('{order_uuid}','{order_code}','{member_uuid}','Perf Test','6d9922e7-2c61-4411-becf-9574f6cf82cd','5555880000023393',false,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'Reconciled','2023-03-29 14:58:08.092694',NULL,'2023-03-29 14:58:08.112455','2023-03-29 14:58:08.112455',NULL,NULL,NULL,NULL,'PARTNER',NULL,'2023-03-29 14:58:08.113181','PARTNER',NULL,'2023-03-29 14:58:08.113187',false,'PARTNER','SV235636',0,'Baht',0,1200,0,0,0,false,NULL,'Perf','Test',NULL,NULL,0,NULL,NULL,NULL,NULL,NULL,NULL,0,NULL,NULL,'2023-03-29 14:58:08.112473',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,0,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'Point Transfer','PTX-268','BZB-Dtac001',108,0.09,600,'THB',NULL,'8027200212',NULL,false,false,NULL,NULL,NULL,NULL,NULL,NULL,NULL,0,0,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'2023-03-29 14:58:08.112455','DTAC',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,false,false,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL),"

from tqdm import tqdm
for i in tqdm(range(int(10))):
    order_uuid = uuid.uuid4()
    running_number = str(i).rjust(15, '0')
    #insert orders
    data_insert_orders += value_insert_order.format(order_uuid= order_uuid, order_code = running_number, member_uuid = member_uuid) + '\n'

data_insert_orders = data_insert_orders[:-2]+';'

with open(f"./work/{batch_name}/gen_member_script.sql", 'w') as f:
    f.write(data_insert_member)

with open(f"./work/{batch_name}/script_delete_member.sql", 'w') as f:
    f.write(delete_member_script)

with open(f"./work/{batch_name}/gen_partner_script.sql", 'w') as f:
    f.write(data_insert_partners)

with open(f"./work/{batch_name}/gen_order_script.sql", 'w') as f:
    f.write(data_insert_orders)


# function connect db and execute sql script
def run_script(db_params, sql_script):
    try:
        connection = psycopg2.connect(**db_params)
        cursor = connection.cursor()
        print("Connected to the database " + db_params['database'])

        cursor.execute(sql_script)
        connection.commit()
        print("SQL script: executed successfully!")

    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to the database or executing the script:", error)

    finally:
        if connection:
            cursor.close()
            connection.close()
            print("Database " + db_params['database'] +  " connection closed.")

# run_script(db_member_params, data_insert_member)
# run_script(db_transaction_params,data_insert_orders)
