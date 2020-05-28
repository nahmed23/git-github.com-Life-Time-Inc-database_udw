CREATE PROC [dbo].[proc_d_nmo_hub_task_department] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_nmo_hub_task_department)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_nmo_hub_task_department_insert') is not null drop table #p_nmo_hub_task_department_insert
create table dbo.#p_nmo_hub_task_department_insert with(distribution=hash(bk_hash), location=user_db) as
select p_nmo_hub_task_department.p_nmo_hub_task_department_id,
       p_nmo_hub_task_department.bk_hash
  from dbo.p_nmo_hub_task_department
 where p_nmo_hub_task_department.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_nmo_hub_task_department.dv_batch_id > @max_dv_batch_id
        or p_nmo_hub_task_department.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_nmo_hub_task_department.bk_hash,
       p_nmo_hub_task_department.id hub_task_department_id,
       s_nmo_hub_task_department.activation_date activation_date,
       case when p_nmo_hub_task_department.bk_hash in('-997', '-998', '-999') then p_nmo_hub_task_department.bk_hash
           when s_nmo_hub_task_department.activation_date is null then '-998'
        else convert(varchar, s_nmo_hub_task_department.activation_date, 112)    end activation_dim_date_key,
       case when p_nmo_hub_task_department.bk_hash in ('-997','-998','-999') then p_nmo_hub_task_department.bk_hash
       when s_nmo_hub_task_department.activation_date is null then '-998'
       else '1' + replace(substring(convert(varchar,s_nmo_hub_task_department.activation_date,114), 1, 5),':','') end activation_dim_time_key,
       s_nmo_hub_task_department.created_date created_date,
       case when p_nmo_hub_task_department.bk_hash in('-997', '-998', '-999') then p_nmo_hub_task_department.bk_hash
           when s_nmo_hub_task_department.created_date is null then '-998'
        else convert(varchar, s_nmo_hub_task_department.created_date, 112)  end created_dim_date_key,
       case when p_nmo_hub_task_department.bk_hash in ('-997','-998','-999') then p_nmo_hub_task_department.bk_hash
       when s_nmo_hub_task_department.created_date is null then '-998'
       else '1' + replace(substring(convert(varchar,s_nmo_hub_task_department.created_date,114), 1, 5),':','') end created_dim_time_key,
       s_nmo_hub_task_department.expiration_date expiration_date,
       case when p_nmo_hub_task_department.bk_hash in('-997', '-998', '-999') then p_nmo_hub_task_department.bk_hash
           when s_nmo_hub_task_department.expiration_date is null then '-998'
        else convert(varchar, s_nmo_hub_task_department.expiration_date, 112)  end expiration_dim_date_key,
       case when p_nmo_hub_task_department.bk_hash in ('-997','-998','-999') then p_nmo_hub_task_department.bk_hash
       when s_nmo_hub_task_department.expiration_date is null then '-998'
       else '1' + replace(substring(convert(varchar,s_nmo_hub_task_department.expiration_date,114), 1, 5),':','') end expiration_dim_time_key,
       s_nmo_hub_task_department.title title,
       s_nmo_hub_task_department.updated_date updated_date,
       case when p_nmo_hub_task_department.bk_hash in('-997', '-998', '-999') then p_nmo_hub_task_department.bk_hash
           when s_nmo_hub_task_department.updated_date is null then '-998'
        else convert(varchar, s_nmo_hub_task_department.updated_date, 112)  end updated_dim_date_key,
       case when p_nmo_hub_task_department.bk_hash in ('-997','-998','-999') then p_nmo_hub_task_department.bk_hash
       when s_nmo_hub_task_department.updated_date is null then '-998'
       else '1' + replace(substring(convert(varchar,s_nmo_hub_task_department.updated_date,114), 1, 5),':','') end updated_dim_time_key,
       isnull(h_nmo_hub_task_department.dv_deleted,0) dv_deleted,
       p_nmo_hub_task_department.p_nmo_hub_task_department_id,
       p_nmo_hub_task_department.dv_batch_id,
       p_nmo_hub_task_department.dv_load_date_time,
       p_nmo_hub_task_department.dv_load_end_date_time
  from dbo.h_nmo_hub_task_department
  join dbo.p_nmo_hub_task_department
    on h_nmo_hub_task_department.bk_hash = p_nmo_hub_task_department.bk_hash
  join #p_nmo_hub_task_department_insert
    on p_nmo_hub_task_department.bk_hash = #p_nmo_hub_task_department_insert.bk_hash
   and p_nmo_hub_task_department.p_nmo_hub_task_department_id = #p_nmo_hub_task_department_insert.p_nmo_hub_task_department_id
  join dbo.s_nmo_hub_task_department
    on p_nmo_hub_task_department.bk_hash = s_nmo_hub_task_department.bk_hash
   and p_nmo_hub_task_department.s_nmo_hub_task_department_id = s_nmo_hub_task_department.s_nmo_hub_task_department_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_nmo_hub_task_department
   where d_nmo_hub_task_department.bk_hash in (select bk_hash from #p_nmo_hub_task_department_insert)

  insert dbo.d_nmo_hub_task_department(
             bk_hash,
             hub_task_department_id,
             activation_date,
             activation_dim_date_key,
             activation_dim_time_key,
             created_date,
             created_dim_date_key,
             created_dim_time_key,
             expiration_date,
             expiration_dim_date_key,
             expiration_dim_time_key,
             title,
             updated_date,
             updated_dim_date_key,
             updated_dim_time_key,
             deleted_flag,
             p_nmo_hub_task_department_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         hub_task_department_id,
         activation_date,
         activation_dim_date_key,
         activation_dim_time_key,
         created_date,
         created_dim_date_key,
         created_dim_time_key,
         expiration_date,
         expiration_dim_date_key,
         expiration_dim_time_key,
         title,
         updated_date,
         updated_dim_date_key,
         updated_dim_time_key,
         dv_deleted,
         p_nmo_hub_task_department_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_nmo_hub_task_department)
--Done!
end
