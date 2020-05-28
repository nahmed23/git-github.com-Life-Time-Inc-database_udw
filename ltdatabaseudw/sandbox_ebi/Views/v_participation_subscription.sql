CREATE VIEW [sandbox_ebi].[v_participation_subscription]
AS select p.participation_id,
       m.member_id,
	   m.customer_name_last_first member_name,

       mp.product_id mms_product_id,
	   ep.product_name exerp_product_name,
       mp.product_description mms_product_description,
	   mp.workday_cost_center,
       mp.workday_offering,

       dc.club_id,
	   dc.club_name,
	   dc.club_code,
	   dc.workday_region,
	   mms_region.description mms_region,
	   pt_rcl_region.description pt_rcl_region,
	   member_activities_region.description member_activities_region,

       b.booking_id,
	   b.booking_name,
	   p.booking_dim_date_key,
	   p.booking_dim_time_key,

	   s.subscription_id,
	   sp.subscription_period_id,
	   sp.net_amount,
	   sp.lt_bucks_amount,
	   sp.refund_amount,
	   sp.from_dim_date_key subscription_period_start_dim_date_key,
	   sp.to_dim_date_key susbcription_period_end_dim_date_key,
	   sp.billing_dim_date_key,
	   sp.subscription_period_state,
	   p.cancel_reason,
	   p.participation_state,
	   p.billable_flag
from dbo.fact_exerp_participation p
join dbo.dim_exerp_subscription_period sp on p.dim_exerp_subscription_period_key = sp.dim_exerp_subscription_period_key
join dbo.d_exerp_subscription s on p.dim_exerp_subscription_key = s.bk_hash
join dbo.dim_exerp_booking b on p.dim_exerp_booking_key = b.dim_exerp_booking_key
join dbo.d_mms_member m on p.dim_mms_member_key = m.bk_hash
join dbo.dim_club dc on p.dim_club_key = dc.dim_club_key
join dbo.dim_description mms_region on mms_region.dim_description_key = dc.region_dim_description_key 
join dbo.dim_description pt_rcl_region on pt_rcl_region.dim_description_key = dc.pt_rcl_area_dim_description_key
join dbo.dim_description member_activities_region on member_activities_region.dim_description_key = dc.member_activities_region_dim_description_key
join dbo.dim_exerp_product ep on p.dim_exerp_product_key = ep.dim_exerp_product_key
join dbo.dim_mms_product mp on p.dim_mms_product_key = mp.dim_mms_product_key
where p.subscription_flag = 'Y'
and (p.participation_cancelled_flag = 'N' or p.participation_cancelled_no_show_flag = 'Y')
and sp.subscription_period_state <> 'cancelled';