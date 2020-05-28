CREATE VIEW [marketing].[v_d_mms_guest_count]
AS select d_mms_guest_count.d_mms_guest_count_key d_mms_guest_count_key,
       d_mms_guest_count.guest_count_id guest_count_id,
       d_mms_guest_count.club_id club_id,
       d_mms_guest_count.dim_club_key dim_club_key,
       d_mms_guest_count.fact_guest_count_dim_date_key fact_guest_count_dim_date_key,
       d_mms_guest_count.guest_count_date guest_count_date,
       d_mms_guest_count.inserted_date_time inserted_date_time,
       d_mms_guest_count.inserted_dim_date_key inserted_dim_date_key,
       d_mms_guest_count.member_child_count member_child_count,
       d_mms_guest_count.member_count member_count,
       d_mms_guest_count.non_member_child_count non_member_child_count,
       d_mms_guest_count.non_member_count non_member_count
  from dbo.d_mms_guest_count;