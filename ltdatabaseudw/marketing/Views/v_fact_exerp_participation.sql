﻿CREATE VIEW [marketing].[v_fact_exerp_participation]
AS select fact_exerp_participation.billable_flag billable_flag,
       fact_exerp_participation.booking_cancelled_flag booking_cancelled_flag,
       fact_exerp_participation.booking_dim_date_key booking_dim_date_key,
       fact_exerp_participation.booking_dim_time_key booking_dim_time_key,
       fact_exerp_participation.cancel_dim_date_key cancel_dim_date_key,
       fact_exerp_participation.cancel_dim_time_key cancel_dim_time_key,
       fact_exerp_participation.cancel_interface_type cancel_interface_type,
       fact_exerp_participation.cancel_reason cancel_reason,
       fact_exerp_participation.clipcard_flag clipcard_flag,
       fact_exerp_participation.creation_dim_date_key creation_dim_date_key,
       fact_exerp_participation.creation_dim_time_key creation_dim_time_key,
       fact_exerp_participation.dim_club_key dim_club_key,
       fact_exerp_participation.dim_exerp_activity_key dim_exerp_activity_key,
       fact_exerp_participation.dim_exerp_booking_key dim_exerp_booking_key,
       fact_exerp_participation.dim_exerp_clipcard_key dim_exerp_clipcard_key,
       fact_exerp_participation.dim_exerp_product_key dim_exerp_product_key,
       fact_exerp_participation.dim_exerp_subscription_key dim_exerp_subscription_key,
       fact_exerp_participation.dim_exerp_subscription_period_key dim_exerp_subscription_period_key,
       fact_exerp_participation.dim_mms_member_key dim_mms_member_key,
       fact_exerp_participation.dim_mms_product_key dim_mms_product_key,
       fact_exerp_participation.ets ets,
       fact_exerp_participation.fact_exerp_participation_key fact_exerp_participation_key,
       fact_exerp_participation.fact_exerp_transaction_log_key fact_exerp_transaction_log_key,
       fact_exerp_participation.participant_number participant_number,
       fact_exerp_participation.participated_flag participated_flag,
       fact_exerp_participation.participation_cancelled_flag participation_cancelled_flag,
       fact_exerp_participation.participation_cancelled_no_show_flag participation_cancelled_no_show_flag,
       fact_exerp_participation.participation_id participation_id,
       fact_exerp_participation.participation_state participation_state,
       fact_exerp_participation.seat_id seat_id,
       fact_exerp_participation.seat_obtained_dim_date_key seat_obtained_dim_date_key,
       fact_exerp_participation.seat_obtained_dim_time_key seat_obtained_dim_time_key,
       fact_exerp_participation.seat_state seat_state,
       fact_exerp_participation.show_up_dim_date_key show_up_dim_date_key,
       fact_exerp_participation.show_up_dim_time_key show_up_dim_time_key,
       fact_exerp_participation.show_up_interface_type show_up_interface_type,
       fact_exerp_participation.show_up_using_card_flag show_up_using_card_flag,
       fact_exerp_participation.subscription_flag subscription_flag,
       fact_exerp_participation.user_interface_type user_interface_type,
       fact_exerp_participation.waitlist_flag waitlist_flag,
       fact_exerp_participation.was_on_waiting_list_flag was_on_waiting_list_flag
  from dbo.fact_exerp_participation;