CREATE VIEW [marketing].[v_dim_mart_seg_member_juniors_on_account]
AS select d_mart_seg_member_juniors_on_account_history.dim_juniors_on_account_segment_key dim_juniors_on_account_segment_key,
       d_mart_seg_member_juniors_on_account_history.juniors_on_account_segment_id juniors_on_account_segment_id,
       d_mart_seg_member_juniors_on_account_history.effective_date_time effective_date_time,
       d_mart_seg_member_juniors_on_account_history.expiration_date_time expiration_date_time,
       d_mart_seg_member_juniors_on_account_history.active_flag active_flag,
       d_mart_seg_member_juniors_on_account_history.juniors_on_account juniors_on_account
  from dbo.d_mart_seg_member_juniors_on_account_history;