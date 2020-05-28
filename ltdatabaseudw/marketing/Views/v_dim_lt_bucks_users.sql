﻿CREATE VIEW [marketing].[v_dim_lt_bucks_users] AS select d_lt_bucks_users.dim_lt_bucks_users_key dim_lt_bucks_users_key,
       d_lt_bucks_users.user_id user_id,
       d_lt_bucks_users.active_flag active_flag,
       d_lt_bucks_users.address_city address_city,
       d_lt_bucks_users.address_line_1 address_line_1,
       d_lt_bucks_users.address_line_2 address_line_2,
       d_lt_bucks_users.address_postal_code address_postal_code,
       d_lt_bucks_users.address_state address_state,
       d_lt_bucks_users.current_points current_points,
       d_lt_bucks_users.dim_mms_member_key dim_mms_member_key,
       d_lt_bucks_users.email email,
       d_lt_bucks_users.first_name first_name,
       d_lt_bucks_users.last_name last_name,
       d_lt_bucks_users.referring_dim_lt_bucks_user_key referring_dim_lt_bucks_user_key,
       d_lt_bucks_users.register_date_time register_date_time,
       d_lt_bucks_users.user_parent user_parent,
       d_lt_bucks_users.user_phone user_phone,
       d_lt_bucks_users.user_type user_type,
       d_lt_bucks_users.user_username user_username
  from dbo.d_lt_bucks_users;