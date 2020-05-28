CREATE VIEW [marketing].[v_dim_mms_member_tenure] AS select d_mms_member.dim_mms_member_key dim_mms_member_key,
       d_mms_member.member_id member_id,
	   d_mms_member.customer_name customer_name,
	   d_mms_member.first_name first_name,
	   d_mms_member.last_name last_name,
	   d_mms_member.gender_abbreviation gender,
	   d_mms_member.join_date join_date,
	   d_mms_member.join_date_key join_date_key,
	   d_mms_member.member_active_flag member_active_flag,
	   d_mms_member.membership_id membership_id,
	   case when member_active_flag='Y' and datediff(month,join_date,getdate())>= 12 then 'Y'
   else 'N' end is_tenure_more_than_12_months        
  from dbo.d_mms_member d_mms_member
  right join dbo.d_mms_membership d_mms_membership
  on d_mms_member.dim_mms_membership_key=d_mms_membership.dim_mms_membership_key
  where d_mms_membership.val_membership_status_id not in (1,3)
  and d_mms_membership.current_price > 0
  and d_mms_member.val_member_type_id in (1,2);