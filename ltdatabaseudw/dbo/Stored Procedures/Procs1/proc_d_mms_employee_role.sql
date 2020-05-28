CREATE PROC [dbo].[proc_d_mms_employee_role] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_mms_employee_role)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_mms_employee_role_insert') is not null drop table #p_mms_employee_role_insert
create table dbo.#p_mms_employee_role_insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_employee_role.p_mms_employee_role_id,
       p_mms_employee_role.bk_hash
  from dbo.p_mms_employee_role
 where p_mms_employee_role.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_mms_employee_role.dv_batch_id > @max_dv_batch_id
        or p_mms_employee_role.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_employee_role.bk_hash,
       p_mms_employee_role.employee_role_id employee_role_id,
       case when l_mms_employee_role.val_employee_role_id IN (10378) THEN 'Y'
                    else 'N'
                 end assistant_department_head_sales_for_net_units_flag,
       case when l_mms_employee_role.val_employee_role_id IN (10083, 10084, 10902, 11151) THEN 'Y'
                    else 'N'
                 end department_head_sales_for_net_units_flag,
       case when p_mms_employee_role.bk_hash in ('-997','-998','-999') then p_mms_employee_role.bk_hash
            when l_mms_employee_role.employee_id is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_employee_role.employee_id as int) as varchar(500)),'z#@$k%&P'))),2)
        end dim_employee_key,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_employee_role.val_employee_role_id as int) as varchar(500)),'z#@$k%&P'))),2) dim_employee_role_key,
       l_mms_employee_role.employee_id employee_id,
       case when s_mms_employee_role.primary_employee_role_flag = 1
       		then 'Y'
       	else 'N'
               end primary_employee_role_flag,
       case when l_mms_employee_role.val_employee_role_id IN (10085, 10084, 10378, 10355, 10083, 10902, 11151) THEN 'Y'
                    else 'N'
                 end sales_group_flag,
       case when l_mms_employee_role.val_employee_role_id IN (10084, 10083, 10902, 11151) THEN 'Y'
                    else 'N'
                 end sales_manager_flag,
       isnull(h_mms_employee_role.dv_deleted,0) dv_deleted,
       p_mms_employee_role.p_mms_employee_role_id,
       p_mms_employee_role.dv_batch_id,
       p_mms_employee_role.dv_load_date_time,
       p_mms_employee_role.dv_load_end_date_time
  from dbo.h_mms_employee_role
  join dbo.p_mms_employee_role
    on h_mms_employee_role.bk_hash = p_mms_employee_role.bk_hash
  join #p_mms_employee_role_insert
    on p_mms_employee_role.bk_hash = #p_mms_employee_role_insert.bk_hash
   and p_mms_employee_role.p_mms_employee_role_id = #p_mms_employee_role_insert.p_mms_employee_role_id
  join dbo.l_mms_employee_role
    on p_mms_employee_role.bk_hash = l_mms_employee_role.bk_hash
   and p_mms_employee_role.l_mms_employee_role_id = l_mms_employee_role.l_mms_employee_role_id
  join dbo.s_mms_employee_role
    on p_mms_employee_role.bk_hash = s_mms_employee_role.bk_hash
   and p_mms_employee_role.s_mms_employee_role_id = s_mms_employee_role.s_mms_employee_role_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_mms_employee_role
   where d_mms_employee_role.bk_hash in (select bk_hash from #p_mms_employee_role_insert)

  insert dbo.d_mms_employee_role(
             bk_hash,
             employee_role_id,
             assistant_department_head_sales_for_net_units_flag,
             department_head_sales_for_net_units_flag,
             dim_employee_key,
             dim_employee_role_key,
             employee_id,
             primary_employee_role_flag,
             sales_group_flag,
             sales_manager_flag,
             deleted_flag,
             p_mms_employee_role_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         employee_role_id,
         assistant_department_head_sales_for_net_units_flag,
         department_head_sales_for_net_units_flag,
         dim_employee_key,
         dim_employee_role_key,
         employee_id,
         primary_employee_role_flag,
         sales_group_flag,
         sales_manager_flag,
         dv_deleted,
         p_mms_employee_role_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_mms_employee_role)
--Done!
end
