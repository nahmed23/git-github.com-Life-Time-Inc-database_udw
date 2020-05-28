CREATE VIEW [marketing].[v_dim_membership_star_rank]
AS select dim_membership_star_rank.customer_name customer_name,
       dim_membership_star_rank.dim_mms_member_key dim_mms_member_key,
       dim_membership_star_rank.first_name first_name,
       dim_membership_star_rank.gender gender,
       dim_membership_star_rank.join_date join_date,
       dim_membership_star_rank.last_name last_name,
       dim_membership_star_rank.member_id member_id,
       dim_membership_star_rank.membership_id membership_id,
       dim_membership_star_rank.val_star_rank_id val_star_rank_id
  from dbo.dim_membership_star_rank;