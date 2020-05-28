CREATE VIEW [sandbox_ebi].[v_subscription_period]
AS select sp.subscription_period_id,
       s.subscription_id,
	   sp.from_dim_date_key subscription_period_start_dim_date_key,
	   sp.to_dim_date_key susbcription_period_end_dim_date_key,
	   sp.net_amount,
	   sp.lt_bucks_amount,
	   sp.refund_amount,
	   sp.billing_dim_date_key,
	   sp.subscription_period_state,

	   count(distinct p.dim_exerp_booking_key) booking_count,
	   count(distinct case when p.booking_dim_date_key < today_dim_date.dim_date_key then p.dim_exerp_booking_key else null end) past_booking_count,
	   count(distinct case when p.booking_dim_date_key >= today_dim_date.dim_date_key then p.dim_exerp_booking_key else null end) future_booking_count,
	   datediff(dd,start_dim_date.calendar_date,end_dim_date.calendar_date) + 1 subscription_period_day_length,

	   start_dim_date.dim_date_key month_1_month_starting_dim_date_key,
       case when start_dim_date.month_starting_dim_date_key <> end_dim_date.month_starting_dim_date_key then datediff(dd,start_dim_date.calendar_date, start_dim_date.month_ending_date) + 1 
            else datediff(dd,start_dim_date.calendar_date,end_dim_date.calendar_date) + 1
        end month_1_day_count,
       case when start_dim_date.month_starting_dim_date_key <> end_dim_date.month_starting_dim_date_key then end_dim_date.month_starting_dim_date_key
            else '-998'
        end month_2_month_starting_dim_date_key,
       case when start_dim_date.month_starting_dim_date_key <> end_dim_date.month_starting_dim_date_key then datediff(dd,end_dim_date.month_starting_date, end_dim_date.calendar_date) + 1 
            else 0
        end month_2_day_count,
	   
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
	   member_activities_region.description member_activities_region
from dbo.dim_exerp_subscription_period sp
join dbo.dim_date start_dim_date on sp.from_dim_date_key = start_dim_date.dim_date_key
join dbo.dim_date end_dim_date on sp.to_dim_date_key = end_dim_date.dim_date_key
join dbo.dim_date today_dim_date on today_dim_date.dim_date_key = convert(varchar(8),getdate(),112) 
join dbo.d_exerp_subscription s on sp.dim_exerp_subscription_key = s.bk_hash
join dbo.dim_club dc on sp.dim_club_key = dc.dim_club_key
join dbo.dim_description mms_region on mms_region.dim_description_key = dc.region_dim_description_key 
join dbo.dim_description pt_rcl_region on pt_rcl_region.dim_description_key = dc.pt_rcl_area_dim_description_key
join dbo.dim_description member_activities_region on member_activities_region.dim_description_key = dc.member_activities_region_dim_description_key
join d_mms_member m on sp.dim_mms_member_key = m.dim_mms_member_key
join dim_exerp_product ep on sp.dim_exerp_product_key = ep.dim_exerp_product_key
join dim_mms_product mp on ep.dim_mms_product_key = mp.dim_mms_product_key
left join dbo.fact_exerp_participation p on sp.dim_exerp_subscription_period_key = p.dim_exerp_subscription_period_key
where sp.subscription_period_state <> 'cancelled'
group by sp.subscription_period_id,
       s.subscription_id,
	   sp.from_dim_date_key,
	   sp.to_dim_date_key,
	   sp.net_amount,
	   sp.lt_bucks_amount,
	   sp.refund_amount,
	   sp.billing_dim_date_key,
	   sp.subscription_period_state,
	   datediff(dd,start_dim_date.calendar_date,end_dim_date.calendar_date) + 1,
	   start_dim_date.dim_date_key,
       case when start_dim_date.month_starting_dim_date_key <> end_dim_date.month_starting_dim_date_key then datediff(dd,start_dim_date.calendar_date, start_dim_date.month_ending_date) + 1 
            else datediff(dd,start_dim_date.calendar_date,end_dim_date.calendar_date) + 1
        end,
       case when start_dim_date.month_starting_dim_date_key <> end_dim_date.month_starting_dim_date_key then end_dim_date.month_starting_dim_date_key
            else '-998'
        end,
       case when start_dim_date.month_starting_dim_date_key <> end_dim_date.month_starting_dim_date_key then datediff(dd,end_dim_date.month_starting_date, end_dim_date.calendar_date) + 1 
            else 0
        end,
	   
       m.member_id,
	   m.customer_name_last_first,

       mp.product_id,
	   ep.product_name,
       mp.product_description,
	   mp.workday_cost_center,
       mp.workday_offering,

       dc.club_id,
	   dc.club_name,
	   dc.club_code,
	   dc.workday_region,
	   mms_region.description,
	   pt_rcl_region.description,
	   member_activities_region.description;