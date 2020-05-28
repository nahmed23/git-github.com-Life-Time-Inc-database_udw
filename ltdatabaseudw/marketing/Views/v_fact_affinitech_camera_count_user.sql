CREATE VIEW [marketing].[v_fact_affinitech_camera_count_user]
AS select  club_name, club_code, class_date, resource, upc_code, upc_desc, booking_reference_id, booking_instance_id, start_time, end_time, instructor_count, 
camera_count from fact_affinitech_camera_count_user;