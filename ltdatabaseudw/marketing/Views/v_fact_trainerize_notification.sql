CREATE VIEW [marketing].[v_fact_trainerize_notification]
AS select fact_trainerize_notification.created_dim_date_key created_dim_date_key,
       fact_trainerize_notification.created_dim_time_key created_dim_time_key,
       fact_trainerize_notification.fact_trainerize_notification_key fact_trainerize_notification_key,
       fact_trainerize_notification.from_dim_employee_key from_dim_employee_key,
       fact_trainerize_notification.message message,
       fact_trainerize_notification.message_type message_type,
       fact_trainerize_notification.notification_id notification_id,
       fact_trainerize_notification.received_dim_date_key received_dim_date_key,
       fact_trainerize_notification.received_dim_time_key received_dim_time_key,
       fact_trainerize_notification.source_id source_id,
       fact_trainerize_notification.source_thread_id source_thread_id,
       fact_trainerize_notification.source_type source_type,
       fact_trainerize_notification.status status,
       fact_trainerize_notification.subject subject,
       fact_trainerize_notification.to_dim_mms_member_key to_dim_mms_member_key,
       fact_trainerize_notification.updated_dim_date_key updated_dim_date_key,
       fact_trainerize_notification.updated_dim_time_key updated_dim_time_key
  from dbo.fact_trainerize_notification;