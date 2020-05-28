CREATE PROC [dbo].[proc_dim_reporting_hierarchy_history] @dv_batch_id [varchar](500) AS
begin

set xact_abort on
set nocount on

-- Get the distinct set of hierarchies and active dates
-- There are a few scenarios where there is more than one reporting_region_type for a given hierarchy and date so arbitrarily chose one
-- Only go back to 2015
if object_id('tempdb..#hierarchy_active_date') is not null drop table #hierarchy_active_date
create table dbo.#hierarchy_active_date with(distribution = round_robin, location=user_db) as
select distinct
       hierarchy.dim_reporting_hierarchy_key,
       hierarchy.reporting_division,
       hierarchy.reporting_sub_division,
       hierarchy.reporting_department,
       hierarchy.reporting_product_group,
       min(hierarchy.reporting_region_type) reporting_region_type,
       dim_date.dim_date_key,
       dim_date.prior_day_dim_date_key,
       dim_date.next_day_dim_date_key
  from (select distinct d_udwcloudsync_product_master_history.dim_reporting_hierarchy_key dim_reporting_hierarchy_key,
               d_udwcloudsync_product_master_history.reporting_division,
               d_udwcloudsync_product_master_history.reporting_sub_division,
               d_udwcloudsync_product_master_history.reporting_department,
               d_udwcloudsync_product_master_history.reporting_product_group,
               d_udwcloudsync_product_master_history.reporting_region_type,
               d_udwcloudsync_product_master_history.effective_date_time,
               d_udwcloudsync_product_master_history.expiration_date_time
          from d_udwcloudsync_product_master_history) hierarchy
  join dim_date
    on hierarchy.effective_date_time <= dim_date.calendar_date
   and hierarchy.expiration_date_time > dim_date.calendar_date
   and dim_date.dim_date_key >= 20150101
   and dim_date.dim_date_key <= convert(varchar,getdate()+5,112)
 group by hierarchy.dim_reporting_hierarchy_key,
       hierarchy.reporting_division,
       hierarchy.reporting_sub_division,
       hierarchy.reporting_department,
       hierarchy.reporting_product_group,
       dim_date.dim_date_key,
       dim_date.prior_day_dim_date_key,
       dim_date.next_day_dim_date_key

-- Calculate lag and lead dates to determine where a series for a given hierarchy starts and ends
if object_id('tempdb..#hierarchy_lag_lead') is not null drop table #hierarchy_lag_lead
create table dbo.#hierarchy_lag_lead with(distribution = round_robin, location=user_db) as
select #hierarchy_active_date.dim_reporting_hierarchy_key,
       #hierarchy_active_date.dim_date_key,
       #hierarchy_active_date.prior_day_dim_date_key,
       #hierarchy_active_date.next_day_dim_date_key,
       lag(#hierarchy_active_date.dim_date_key,1) over (partition by #hierarchy_active_date.dim_reporting_hierarchy_key order by #hierarchy_active_date.dim_date_key) lag_dim_date_key,
       lead(#hierarchy_active_date.dim_date_key,1) over (partition by #hierarchy_active_date.dim_reporting_hierarchy_key order by #hierarchy_active_date.dim_date_key) lead_dim_date_key
  from #hierarchy_active_date

-- Get the effective and expiration dates of each series within each hierarchy
if object_id('tempdb..#hierarchy_effective_expiration') is not null drop table #hierarchy_effective_expiration
create table dbo.#hierarchy_effective_expiration with(distribution = round_robin, location=user_db) as
with
-- Get the effective date of each series and rank within each hierarchy
first_in_series(dim_reporting_hierarchy_key, dim_date_key, r) as (
 select #hierarchy_lag_lead.dim_reporting_hierarchy_key,
        #hierarchy_lag_lead.dim_date_key,
        row_number() over(partition by #hierarchy_lag_lead.dim_reporting_hierarchy_key order by #hierarchy_lag_lead.dim_date_key) r
   from #hierarchy_lag_lead
  where (#hierarchy_lag_lead.lag_dim_date_key is null
         or #hierarchy_lag_lead.prior_day_dim_date_key != lag_dim_date_key)
),
-- Get the expiration date of each series and rank within each hierarchy
last_in_series(dim_reporting_hierarchy_key, next_day_dim_date_key, r) as (
 select #hierarchy_lag_lead.dim_reporting_hierarchy_key,
        #hierarchy_lag_lead.next_day_dim_date_key,
        row_number() over(partition by #hierarchy_lag_lead.dim_reporting_hierarchy_key order by #hierarchy_lag_lead.dim_date_key) r
   from #hierarchy_lag_lead
  where (#hierarchy_lag_lead.lead_dim_date_key is null
         or #hierarchy_lag_lead.next_day_dim_date_key != lead_dim_date_key)
)
select first_in_series.dim_reporting_hierarchy_key,
       first_in_series.dim_date_key effective_dim_date_key,
       case when last_in_series.next_day_dim_date_key != convert(varchar,getdate()+6,112) then last_in_series.next_day_dim_date_key 
            else '99991231'
        end expiration_dim_date_key
  from first_in_series
  join last_in_series
    on first_in_series.dim_reporting_hierarchy_key = last_in_series.dim_reporting_hierarchy_key
   and first_in_series.r = last_in_series.r

-- Join back to the hierarchy details to get the full set of columns
if object_id('tempdb..#dim_reporting_hierarchy_history') is not null drop table #dim_reporting_hierarchy_history
create table dbo.#dim_reporting_hierarchy_history with(distribution = round_robin, location=user_db) as
select #hierarchy_effective_expiration.dim_reporting_hierarchy_key,
       #hierarchy_effective_expiration.effective_dim_date_key,
       #hierarchy_effective_expiration.expiration_dim_date_key,
       #hierarchy_active_date.reporting_division,
       #hierarchy_active_date.reporting_sub_division,
       #hierarchy_active_date.reporting_department,
       #hierarchy_active_date.reporting_product_group,
       #hierarchy_active_date.reporting_region_type,
       @dv_batch_id dv_batch_id,
       getdate() dv_load_date_time,
       convert(datetime,'99991231',112) dv_load_end_date_time
  from #hierarchy_effective_expiration
  join #hierarchy_active_date
    on #hierarchy_effective_expiration.dim_reporting_hierarchy_key = #hierarchy_active_date.dim_reporting_hierarchy_key
   and #hierarchy_effective_expiration.effective_dim_date_key = #hierarchy_active_date.dim_date_key

-- Delete and re-insert as a single transaction
--   Delete all records from the table
--   Insert all records

begin tran

  delete dbo.dim_reporting_hierarchy_history
   
  insert into dim_reporting_hierarchy_history
        (dim_reporting_hierarchy_key,
         effective_dim_date_key,
         expiration_dim_date_key,
         reporting_division,
         reporting_sub_division,
         reporting_department,
         reporting_product_group,
         reporting_region_type,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         dv_inserted_date_time,
         dv_insert_user)
  select dim_reporting_hierarchy_key,
         effective_dim_date_key,
         expiration_dim_date_key,
         reporting_division,
         reporting_sub_division,
         reporting_department,
         reporting_product_group,
         reporting_region_type,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate() ,
         suser_sname()
    from #dim_reporting_hierarchy_history
 
commit tran

end
