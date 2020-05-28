CREATE PROC [dbo].[proc_dim_exerp_initial_participation_employee] AS
BEGIN
	SET XACT_ABORT ON
	SET NOCOUNT ON
--this distribution aligns the target temp table with the largest sub-table (fact_exerp_participation)
--If you check the request steps, you'll notice there's zero movement for the ~17m participation records
IF OBJECT_ID('tempdb.dbo.#first_participation', 'U') IS NOT NULL drop table #first_participation
create table #first_participation with (distribution = hash(fact_exerp_participation_key)) as
select par.dim_exerp_subscription_key, 
        b.start_dim_date_key,
        b.start_dim_time_key,
        su.dim_employee_key,
        fact_exerp_participation_key,
        rank() over (partition by par.dim_exerp_clipcard_key
                        order by b.start_dim_date_key,b.start_dim_time_key,
                                case when (par.participation_state = 'cancelled' and par.cancel_reason = 'no_show') or (par.participation_state <> 'cancelled') then 1 else 2 end,
                                par.participation_id) rnk,
        par.dim_exerp_clipcard_key
        ,par.participation_state, par.cancel_reason
    from fact_exerp_participation par --on sp.dim_exerp_subscription_key = par.dim_exerp_subscription_key
    join dim_exerp_staff_usage su on par.dim_exerp_booking_key = su.dim_exerp_booking_key and su.staff_usage_state = 'active'
    join d_exerp_booking b on par.dim_exerp_booking_key = b.bk_hash
where dim_exerp_clipcard_key <> '-998'
union all
select par.dim_exerp_subscription_key, 
        b.start_dim_date_key,
        b.start_dim_time_key,
        su.dim_employee_key,
        fact_exerp_participation_key,
        rank() over (partition by par.dim_exerp_subscription_key
                        order by b.start_dim_date_key,b.start_dim_time_key,
                                case when (par.participation_state = 'cancelled' and par.cancel_reason = 'no_show') or (par.participation_state <> 'cancelled') then 1 else 2 end,
                                par.participation_id) rnk,
        par.dim_exerp_clipcard_key
        ,par.participation_state, par.cancel_reason
    from fact_exerp_participation par --on sp.dim_exerp_subscription_key = par.dim_exerp_subscription_key
    join dim_exerp_staff_usage su on par.dim_exerp_booking_key = su.dim_exerp_booking_key and su.staff_usage_state = 'active'
    join d_exerp_booking b on par.dim_exerp_booking_key = b.bk_hash
where dim_exerp_subscription_key <> '-998'

--this distribution simply aligns it with the above temp table.  There's not a clear great choice here.
--the "exerp" limit on fact_mms_sales_transaction_item is enough for the optimizer to know to limit results before data movement (likely due to auto stats), so there's no 300m record shuffle/broadcast
IF OBJECT_ID('tempdb.dbo.#x', 'U') IS NOT NULL drop table #x
create table #x with (distribution = hash(fact_exerp_participation_key)) as
select mti.fact_mms_sales_transaction_item_key,
        mti.tran_item_id, --not needed, only for comparison checks to original
        mti.mms_tran_id, --not needed, only for comparison checks to original
        sale_de.dim_employee_key sale_dim_employee_key,
        sale_de.employee_id sale_employee_id,
        serv_de.dim_employee_key service_dim_employee_key, 
        serv_de.employee_id service_employee_id,
        par.fact_exerp_participation_key
from d_exerp_sale_log sl
join fact_mms_sales_transaction_item mti on sl.external_id = mti.external_item_id and mti.transaction_source = 'Exerp' --only exerp, should filter before data movement
left join dim_exerp_subscription_period sp on sl.fact_exerp_transaction_log_key =sp.fact_exerp_transaction_log_key
left join #first_participation par on sp.dim_exerp_subscription_key = par.dim_exerp_subscription_key and par.rnk = 1
left join dim_employee sale_de on mti.primary_sales_dim_employee_key = sale_de.dim_employee_key
left join dim_employee serv_de on par.dim_employee_key = serv_de.dim_employee_key
where mti.primary_sales_dim_employee_key <> '-998'

--the table itself serves simply as a lookup, so it makes sense to distribute it on the lookup key.
--Whatever is looking up TO this table should involve fact_mms_sales_transaction_item, so those distributions should be aligned
--currently just a drop and reload, needs incremental logic up top
--if object_id('dbo.dim_exerp_initial_participation_employee') is not null drop table dim_exerp_initial_participation_employee
--create table dbo.dim_exerp_initial_participation_employee with (distribution = hash(fact_mms_sales_transaction_item_key)) as
truncate table dim_exerp_initial_participation_employee

insert into dim_exerp_initial_participation_employee
select fact_mms_sales_transaction_item_key,
       mms_tran_id,
       tran_item_id,
       sale_dim_employee_key,
       sale_employee_id,
       service_dim_employee_key, 
       service_employee_id
       --,plus a bunch of other columns
from #x
end
