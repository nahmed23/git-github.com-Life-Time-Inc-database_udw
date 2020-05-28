CREATE VIEW [marketing].[v_dim_exerp_activity_group]
AS select d_exerp_activity_group.dim_exerp_activity_group_key dim_exerp_activity_group_key,
       d_exerp_activity_group.activity_group_id activity_group_id,
       d_exerp_activity_group.activity_group_name activity_group_name,
       d_exerp_activity_group.activity_group_state activity_group_state,
       d_exerp_activity_group.book_api_flag book_api_flag,
       d_exerp_activity_group.book_client_flag book_client_flag,
       d_exerp_activity_group.book_kiosk_flag book_kiosk_flag,
       d_exerp_activity_group.book_mobile_api_flag book_mobile_api_flag,
       d_exerp_activity_group.book_web_flag book_web_flag,
       d_exerp_activity_group.external_id external_id,
       d_exerp_activity_group.parent_d_exerp_activity_group_bk_hash parent_d_exerp_activity_group_bk_hash
  from dbo.d_exerp_activity_group;