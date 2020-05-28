CREATE VIEW [marketing].[v_dim_mart_seg_member_lifestage]
AS select d_mart_seg_member_lifestage_history.dim_mart_seg_member_lifestage_key dim_mart_seg_member_lifestage_key,
       d_mart_seg_member_lifestage_history.lifestage_segment_id lifestage_segment_id,
       d_mart_seg_member_lifestage_history.effective_date_time effective_date_time,
       d_mart_seg_member_lifestage_history.expiration_date_time expiration_date_time,
       d_mart_seg_member_lifestage_history.active_flag active_flag,
       d_mart_seg_member_lifestage_history.gender gender,
       d_mart_seg_member_lifestage_history.has_kids has_kids,
       d_mart_seg_member_lifestage_history.lifestage_description lifestage_description,
       d_mart_seg_member_lifestage_history.max_age max_age,
       d_mart_seg_member_lifestage_history.min_age min_age
  from dbo.d_mart_seg_member_lifestage_history;