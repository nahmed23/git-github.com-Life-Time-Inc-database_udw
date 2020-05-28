﻿CREATE PROC [dbo].[proc_d_nmo_hub_task] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_nmo_hub_task)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_nmo_hub_task_insert') is not null drop table #p_nmo_hub_task_insert
create table dbo.#p_nmo_hub_task_insert with(distribution=hash(bk_hash), location=user_db) as
select p_nmo_hub_task.p_nmo_hub_task_id,
       p_nmo_hub_task.bk_hash
  from dbo.p_nmo_hub_task
 where p_nmo_hub_task.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_nmo_hub_task.dv_batch_id > @max_dv_batch_id
        or p_nmo_hub_task.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_nmo_hub_task.bk_hash,
       p_nmo_hub_task.bk_hash dim_nmo_hub_task_key,
       p_nmo_hub_task.id hub_task_id,
       s_nmo_hub_task.Assignee_Name assignee_name,
       l_nmo_hub_task.assignee_Party_Id assignee_party_id,
       s_nmo_hub_task.created_date created_date,
       case when p_nmo_hub_task.bk_hash in('-997', '-998', '-999') then p_nmo_hub_task.bk_hash
           when s_nmo_hub_task.created_date is null then '-998'
        else convert(varchar, s_nmo_hub_task.created_date, 112)  end created_dim_date_key,
       case when p_nmo_hub_task.bk_hash in ('-997','-998','-999') then p_nmo_hub_task.bk_hash
       when s_nmo_hub_task.created_date is null then '-998'
       else '1' + replace(substring(convert(varchar,s_nmo_hub_task.created_date,114), 1, 5),':','') end created_dim_time_key,
       s_nmo_hub_task.creator_Name creator_name,
       l_nmo_hub_task.creator_Party_Id creator_party_id,
       case when p_nmo_hub_task.bk_hash in ('-997','-998','-999') then p_nmo_hub_task.bk_hash     
         when l_nmo_hub_task.club_id is null then '-998'   
         else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_nmo_hub_task.club_id as int) as varchar(500)),'z#@$k%&P'))),2)   end dim_club_key,
       case when p_nmo_hub_task.bk_hash in('-997', '-998', '-999') then p_nmo_hub_task.bk_hash
           when l_nmo_hub_task.hub_task_department_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_nmo_hub_task.hub_task_department_id as int ) as varchar(500)),'z#@$k%&P'))),2)   end dim_nmo_hub_task_department_key,
       case when p_nmo_hub_task.bk_hash in('-997', '-998', '-999') then p_nmo_hub_task.bk_hash
           when l_nmo_hub_task.hub_task_status_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_nmo_hub_task.hub_task_status_id as int ) as varchar(500)),'z#@$k%&P'))),2)   end dim_nmo_hub_task_status_key,
       case when p_nmo_hub_task.bk_hash in('-997', '-998', '-999') then p_nmo_hub_task.bk_hash
           when l_nmo_hub_task.hub_task_type_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_nmo_hub_task.hub_task_type_id as int ) as varchar(500)),'z#@$k%&P'))),2)   end dim_nmo_hub_task_type_key,
       s_nmo_hub_task.due_date due_date,
       case when p_nmo_hub_task.bk_hash in('-997', '-998', '-999') then p_nmo_hub_task.bk_hash
           when s_nmo_hub_task.due_date is null then '-998'
        else convert(varchar, s_nmo_hub_task.due_date, 112)    end due_dim_date_key,
       case when p_nmo_hub_task.bk_hash in ('-997','-998','-999') then p_nmo_hub_task.bk_hash
       when s_nmo_hub_task.due_date is null then '-998'
       else '1' + replace(substring(convert(varchar,s_nmo_hub_task.due_date,114), 1, 5),':','') end due_dim_time_key,
       l_nmo_hub_task.party_id party_id,
       s_nmo_hub_task.priority priority,
       s_nmo_hub_task.resolution_date resolution_date,
       case when p_nmo_hub_task.bk_hash in('-997', '-998', '-999') then p_nmo_hub_task.bk_hash
           when s_nmo_hub_task.resolution_date is null then '-998'
        else convert(varchar, s_nmo_hub_task.resolution_date, 112)  end resolution_dim_date_key,
       case when p_nmo_hub_task.bk_hash in ('-997','-998','-999') then p_nmo_hub_task.bk_hash
       when s_nmo_hub_task.resolution_Date is null then '-998'
       else '1' + replace(substring(convert(varchar,s_nmo_hub_task.resolution_Date,114), 1, 5),':','') end resolution_dim_time_key,
       s_nmo_hub_task.title title,
       s_nmo_hub_task.updated_date updated_date,
       case when p_nmo_hub_task.bk_hash in('-997', '-998', '-999') then p_nmo_hub_task.bk_hash
           when s_nmo_hub_task.updated_date is null then '-998'
        else convert(varchar, s_nmo_hub_task.updated_date, 112)  end updated_dim_date_key,
       case when p_nmo_hub_task.bk_hash in ('-997','-998','-999') then p_nmo_hub_task.bk_hash
       when s_nmo_hub_task.updated_date is null then '-998'
       else '1' + replace(substring(convert(varchar,s_nmo_hub_task.updated_date,114), 1, 5),':','') end updated_dim_time_key,
       isnull(h_nmo_hub_task.dv_deleted,0) dv_deleted,
       p_nmo_hub_task.p_nmo_hub_task_id,
       p_nmo_hub_task.dv_batch_id,
       p_nmo_hub_task.dv_load_date_time,
       p_nmo_hub_task.dv_load_end_date_time
  from dbo.h_nmo_hub_task
  join dbo.p_nmo_hub_task
    on h_nmo_hub_task.bk_hash = p_nmo_hub_task.bk_hash
  join #p_nmo_hub_task_insert
    on p_nmo_hub_task.bk_hash = #p_nmo_hub_task_insert.bk_hash
   and p_nmo_hub_task.p_nmo_hub_task_id = #p_nmo_hub_task_insert.p_nmo_hub_task_id
  join dbo.l_nmo_hub_task
    on p_nmo_hub_task.bk_hash = l_nmo_hub_task.bk_hash
   and p_nmo_hub_task.l_nmo_hub_task_id = l_nmo_hub_task.l_nmo_hub_task_id
  join dbo.s_nmo_hub_task
    on p_nmo_hub_task.bk_hash = s_nmo_hub_task.bk_hash
   and p_nmo_hub_task.s_nmo_hub_task_id = s_nmo_hub_task.s_nmo_hub_task_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_nmo_hub_task
   where d_nmo_hub_task.bk_hash in (select bk_hash from #p_nmo_hub_task_insert)

  insert dbo.d_nmo_hub_task(
             bk_hash,
             dim_nmo_hub_task_key,
             hub_task_id,
             assignee_name,
             assignee_party_id,
             created_date,
             created_dim_date_key,
             created_dim_time_key,
             creator_name,
             creator_party_id,
             dim_club_key,
             dim_nmo_hub_task_department_key,
             dim_nmo_hub_task_status_key,
             dim_nmo_hub_task_type_key,
             due_date,
             due_dim_date_key,
             due_dim_time_key,
             party_id,
             priority,
             resolution_date,
             resolution_dim_date_key,
             resolution_dim_time_key,
             title,
             updated_date,
             updated_dim_date_key,
             updated_dim_time_key,
             deleted_flag,
             p_nmo_hub_task_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         dim_nmo_hub_task_key,
         hub_task_id,
         assignee_name,
         assignee_party_id,
         created_date,
         created_dim_date_key,
         created_dim_time_key,
         creator_name,
         creator_party_id,
         dim_club_key,
         dim_nmo_hub_task_department_key,
         dim_nmo_hub_task_status_key,
         dim_nmo_hub_task_type_key,
         due_date,
         due_dim_date_key,
         due_dim_time_key,
         party_id,
         priority,
         resolution_date,
         resolution_dim_date_key,
         resolution_dim_time_key,
         title,
         updated_date,
         updated_dim_date_key,
         updated_dim_time_key,
         dv_deleted,
         p_nmo_hub_task_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_nmo_hub_task)
--Done!
end
