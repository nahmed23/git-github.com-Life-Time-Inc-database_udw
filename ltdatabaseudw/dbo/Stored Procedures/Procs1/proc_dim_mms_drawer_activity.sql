CREATE PROC [dbo].[proc_dim_mms_drawer_activity] @dv_batch_id [varchar](500) AS
begin

set xact_abort on
set nocount on

declare @max_dv_batch_id bigint = (select max(isnull(dv_batch_id,-1)) from dim_mms_drawer_activity)
declare @current_dv_batch_id bigint = @dv_batch_id
declare @load_dv_batch_id bigint = case when @max_dv_batch_id < @current_dv_batch_id then @max_dv_batch_id else @current_dv_batch_id end

if object_id('tempdb..#dim_mms_drawer_activity') is not null drop table #dim_mms_drawer_activity
create table dbo.#dim_mms_drawer_activity with(distribution=hash(dim_mms_drawer_activity_key), location=user_db, heap) as
select d_mms_drawer_activity.dim_mms_drawer_activity_key,
       d_mms_drawer_activity.drawer_activity_id,
       d_mms_drawer.dim_club_key,
       d_mms_drawer_activity.open_flag,
       d_mms_drawer_activity.pending_flag,
       d_mms_drawer_activity.closed_flag,
       case when dim_mms_drawer_activity.dim_mms_drawer_activity_key is null then
			     case when d_mms_drawer_activity.closed_flag = 'Y' then convert(varchar, dateadd(hh, -6, getdate()), 112)
			          else '-998'
			      end			
		   else 
			     case when d_mms_drawer_activity.closed_flag = 'Y' 
				      then 
					       case when dim_mms_drawer_activity.closed_flag_set_in_edw_dim_date_key = '-998'
					       then convert(varchar, dateadd(hh, -6, getdate()), 112)
						   else dim_mms_drawer_activity.closed_flag_set_in_edw_dim_date_key
					   end
					  else dim_mms_drawer_activity.closed_flag_set_in_edw_dim_date_key
				  end
	    end closed_flag_set_in_edw_dim_date_key,		 
       d_mms_drawer_activity.open_dim_date_key,
       d_mms_drawer_activity.open_dim_time_key,
       d_mms_drawer_activity.open_dim_employee_key,
       d_mms_drawer_activity.pending_dim_date_key,
       d_mms_drawer_activity.pending_dim_time_key,
       d_mms_drawer_activity.pending_dim_employee_key,
       d_mms_drawer_activity.closed_dim_date_key,
       d_mms_drawer_activity.closed_dim_time_key,
       d_mms_drawer_activity.closed_business_dim_date_key,
       d_mms_drawer_activity.closed_dim_employee_key,
       d_mms_drawer_activity.closing_comments,
       case when d_mms_drawer.dv_load_date_time >= isnull(d_mms_drawer_activity.dv_load_date_time,'jan 1, 1753') then d_mms_drawer.dv_load_date_time
            else d_mms_drawer_activity.dv_load_date_time
        end dv_load_date_time,
       'Dec 31, 9999' dv_load_end_date_time,
       case when d_mms_drawer.dv_batch_id >= isnull(d_mms_drawer_activity.dv_batch_id,-1) then d_mms_drawer.dv_batch_id
            else d_mms_drawer_activity.dv_batch_id
        end dv_batch_id,
       getdate() dv_inserted_date_time,
       suser_sname() dv_insert_user
  from d_mms_drawer
  join d_mms_drawer_activity
    on d_mms_drawer.bk_hash = d_mms_drawer_activity.d_mms_drawer_bk_hash
  left join dim_mms_drawer_activity 
    on dim_mms_drawer_activity.dim_mms_drawer_activity_key = d_mms_drawer_activity.dim_mms_drawer_activity_key
 where (d_mms_drawer.dv_batch_id >= @load_dv_batch_id
        or d_mms_drawer_activity.dv_batch_id >= @load_dv_batch_id)

   
-- Delete and re-insert as a single transaction
--   Delete records from the table that exist
--   Insert records from records from current and missing batches

begin tran

  delete dbo.dim_mms_drawer_activity
   where dim_mms_drawer_activity_key in (select dim_mms_drawer_activity_key from dbo.#dim_mms_drawer_activity) 

  insert into dim_mms_drawer_activity
   (dim_mms_drawer_activity_key,
    drawer_activity_id,
    dim_club_key,
    open_flag,
    pending_flag,
    closed_flag,
    closed_flag_set_in_edw_dim_date_key,
    open_dim_date_key,
    open_dim_time_key,
    open_dim_employee_key,
    pending_dim_date_key,
    pending_dim_time_key,
    pending_dim_employee_key,
    closed_dim_date_key,
    closed_dim_time_key,
    closed_business_dim_date_key,
    closed_dim_employee_key,
    closing_comments,
    dv_load_date_time,
    dv_load_end_date_time,
    dv_batch_id,
    dv_inserted_date_time,
    dv_insert_user
     )
 select dim_mms_drawer_activity_key,
        drawer_activity_id,
        dim_club_key,
        open_flag,
        pending_flag,
        closed_flag,
        closed_flag_set_in_edw_dim_date_key,
        open_dim_date_key,
        open_dim_time_key,
        open_dim_employee_key,
        pending_dim_date_key,
        pending_dim_time_key,
        pending_dim_employee_key,
        closed_dim_date_key,
        closed_dim_time_key,
        closed_business_dim_date_key,
        closed_dim_employee_key,
        closing_comments,
        dv_load_date_time,
        dv_load_end_date_time,
        dv_batch_id,
        dv_inserted_date_time,
        dv_insert_user
    from #dim_mms_drawer_activity

commit tran

end
