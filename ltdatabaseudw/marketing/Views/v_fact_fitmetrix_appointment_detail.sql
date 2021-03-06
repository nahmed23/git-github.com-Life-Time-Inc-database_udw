﻿CREATE VIEW [marketing].[v_fact_fitmetrix_appointment_detail] AS select d_fitmetrix_api_appointment_id_statistics.fact_fitmetrix_appointment_detail_key fact_fitmetrix_appointment_detail_key,
       d_fitmetrix_api_appointment_id_statistics.profile_appointment_id profile_appointment_id,
       d_fitmetrix_api_appointment_id_statistics.appointment_name appointment_name,
       d_fitmetrix_api_appointment_id_statistics.checked_in_flag checked_in_flag,
       d_fitmetrix_api_appointment_id_statistics.created_dim_date_key created_dim_date_key,
       d_fitmetrix_api_appointment_id_statistics.created_dim_time_key created_dim_time_key,
       d_fitmetrix_api_appointment_id_statistics.dim_fitmetrix_appointment_key dim_fitmetrix_appointment_key,
       d_fitmetrix_api_appointment_id_statistics.dim_mms_member_key dim_mms_member_key,
       d_fitmetrix_api_appointment_id_statistics.email_address email_address,
       d_fitmetrix_api_appointment_id_statistics.first_name first_name,
       d_fitmetrix_api_appointment_id_statistics.last_name last_name,
       d_fitmetrix_api_appointment_id_statistics.spot_number spot_number,
       d_fitmetrix_api_appointment_id_statistics.start_dim_date_key start_dim_date_key,
       d_fitmetrix_api_appointment_id_statistics.start_dim_time_key start_dim_time_key,
       d_fitmetrix_api_appointment_id_statistics.total_points total_points,
       d_fitmetrix_api_appointment_id_statistics.waitlist_dim_date_key waitlist_dim_date_key,
       d_fitmetrix_api_appointment_id_statistics.waitlist_dim_time_key waitlist_dim_time_key,
       d_fitmetrix_api_appointment_id_statistics.waitlist_flag waitlist_flag,
       d_fitmetrix_api_appointment_id_statistics.waitlist_position waitlist_position
  from dbo.d_fitmetrix_api_appointment_id_statistics;