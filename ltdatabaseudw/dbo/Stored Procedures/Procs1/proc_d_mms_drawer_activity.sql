CREATE PROC [dbo].[proc_d_mms_drawer_activity] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_mms_drawer_activity)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_mms_drawer_activity_insert') is not null drop table #p_mms_drawer_activity_insert
create table dbo.#p_mms_drawer_activity_insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_drawer_activity.p_mms_drawer_activity_id,
       p_mms_drawer_activity.bk_hash
  from dbo.p_mms_drawer_activity
 where p_mms_drawer_activity.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_mms_drawer_activity.dv_batch_id > @max_dv_batch_id
        or p_mms_drawer_activity.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_drawer_activity.bk_hash,
       p_mms_drawer_activity.bk_hash dim_mms_drawer_activity_key,
       l_mms_drawer_activity.drawer_activity_id drawer_activity_id,
       case when p_mms_drawer_activity.bk_hash in ('-997', '-998', '-999') then p_mms_drawer_activity.bk_hash
             when s_mms_drawer_activity.close_date_time is null then '-998'
             when substring(convert(varchar,s_mms_drawer_activity.close_date_time,114), 1,2) < 12 then convert(varchar, dateadd(DD, -1, close_date_time), 112) --dateadd
             else convert(varchar, close_date_time, 112)
       end closed_business_dim_date_key,
       case when p_mms_drawer_activity.bk_hash in ('-997', '-998', '-999') then p_mms_drawer_activity.bk_hash
             when s_mms_drawer_activity.close_date_time is null then '-998'
              else convert(varchar, s_mms_drawer_activity.close_date_time, 112)
       	   end closed_dim_date_key,
       case when p_mms_drawer_activity.bk_hash in ('-997','-998','-999') then p_mms_drawer_activity.bk_hash
             when l_mms_drawer_activity.close_employee_id is null then '-998'
             else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_mms_drawer_activity.close_employee_id as varchar(500)),'z#@$k%&P'))),2)
        end closed_dim_employee_key,
       case when p_mms_drawer_activity.bk_hash in ('-997', '-998', '-999') then p_mms_drawer_activity.bk_hash
             when s_mms_drawer_activity.close_date_time is null then '-998'
             else '1' + replace(substring(convert(varchar,s_mms_drawer_activity.close_date_time,114), 1, 5),':','')
       	  end closed_dim_time_key,
       case when l_mms_drawer_activity.val_drawer_status_id = 3 then 'Y'
       	  else 'N'
       	  end closed_flag,
       case when s_mms_drawer_activity.closing_comments is null then ''
             else replace(replace(replace(replace(closing_comments, char(9), ''), char(10), ''), char(13), ''), char(34), char(39))
       	  end closing_comments,
       case when p_mms_drawer_activity.bk_hash in ('-997', '-998', '-999') then p_mms_drawer_activity.bk_hash
             else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_mms_drawer_activity.drawer_id as varchar(500)),'z#@$k%&P'))),2)
       	  end d_mms_drawer_bk_hash,
       case when p_mms_drawer_activity.bk_hash in ('-997', '-998', '-999') then p_mms_drawer_activity.bk_hash
             when s_mms_drawer_activity.open_date_time is null then '-998'
             else convert(varchar, s_mms_drawer_activity.open_date_time, 112)
       	  end open_dim_date_key,
       case when p_mms_drawer_activity.bk_hash in ('-997','-998','-999') then p_mms_drawer_activity.bk_hash
             when l_mms_drawer_activity.open_employee_id is null then '-998'
             else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_mms_drawer_activity.open_employee_id as varchar(500)),'z#@$k%&P'))),2)
        end open_dim_employee_key,
       case when p_mms_drawer_activity.bk_hash in ('-997', '-998', '-999') then p_mms_drawer_activity.bk_hash
             when s_mms_drawer_activity.open_date_time is null then '-998'
             else '1' + replace(substring(convert(varchar,s_mms_drawer_activity.open_date_time,114), 1, 5),':','')
       	  end open_dim_time_key,
       case when l_mms_drawer_activity.val_drawer_status_id = 1 then 'Y'
       	  else 'N'
       	  end open_flag,
       case when p_mms_drawer_activity.bk_hash in ('-997', '-998', '-999') then p_mms_drawer_activity.bk_hash
             when s_mms_drawer_activity.pend_date_time is null then '-998'
             else convert(varchar, s_mms_drawer_activity.pend_date_time, 112)
       	  end pending_dim_date_key,
       case when p_mms_drawer_activity.bk_hash in ('-997','-998','-999') then p_mms_drawer_activity.bk_hash
             when l_mms_drawer_activity.pend_employee_id is null then '-998'
             else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_mms_drawer_activity.pend_employee_id as varchar(500)),'z#@$k%&P'))),2)
        end pending_dim_employee_key,
       case when p_mms_drawer_activity.bk_hash in ('-997', '-998', '-999') then p_mms_drawer_activity.bk_hash
             when s_mms_drawer_activity.pend_date_time is null then '-998'
             else '1' + replace(substring(convert(varchar,s_mms_drawer_activity.pend_date_time,114), 1, 5),':','')
       	  end pending_dim_time_key,
       case when l_mms_drawer_activity.val_drawer_status_id = 2 then 'Y'
       	  else 'N'
       	  end pending_flag,
       p_mms_drawer_activity.p_mms_drawer_activity_id,
       p_mms_drawer_activity.dv_batch_id,
       p_mms_drawer_activity.dv_load_date_time,
       p_mms_drawer_activity.dv_load_end_date_time
  from dbo.p_mms_drawer_activity
  join #p_mms_drawer_activity_insert
    on p_mms_drawer_activity.bk_hash = #p_mms_drawer_activity_insert.bk_hash
   and p_mms_drawer_activity.p_mms_drawer_activity_id = #p_mms_drawer_activity_insert.p_mms_drawer_activity_id
  join dbo.l_mms_drawer_activity
    on p_mms_drawer_activity.bk_hash = l_mms_drawer_activity.bk_hash
   and p_mms_drawer_activity.l_mms_drawer_activity_id = l_mms_drawer_activity.l_mms_drawer_activity_id
  join dbo.s_mms_drawer_activity
    on p_mms_drawer_activity.bk_hash = s_mms_drawer_activity.bk_hash
   and p_mms_drawer_activity.s_mms_drawer_activity_id = s_mms_drawer_activity.s_mms_drawer_activity_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_mms_drawer_activity
   where d_mms_drawer_activity.bk_hash in (select bk_hash from #p_mms_drawer_activity_insert)

  insert dbo.d_mms_drawer_activity(
             bk_hash,
             dim_mms_drawer_activity_key,
             drawer_activity_id,
             closed_business_dim_date_key,
             closed_dim_date_key,
             closed_dim_employee_key,
             closed_dim_time_key,
             closed_flag,
             closing_comments,
             d_mms_drawer_bk_hash,
             open_dim_date_key,
             open_dim_employee_key,
             open_dim_time_key,
             open_flag,
             pending_dim_date_key,
             pending_dim_employee_key,
             pending_dim_time_key,
             pending_flag,
             p_mms_drawer_activity_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         dim_mms_drawer_activity_key,
         drawer_activity_id,
         closed_business_dim_date_key,
         closed_dim_date_key,
         closed_dim_employee_key,
         closed_dim_time_key,
         closed_flag,
         closing_comments,
         d_mms_drawer_bk_hash,
         open_dim_date_key,
         open_dim_employee_key,
         open_dim_time_key,
         open_flag,
         pending_dim_date_key,
         pending_dim_employee_key,
         pending_dim_time_key,
         pending_flag,
         p_mms_drawer_activity_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_mms_drawer_activity)
--Done!
end
