CREATE PROC [dbo].[proc_d_nmo_hub_task_audit] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_nmo_hub_task_audit)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_nmo_hub_task_audit_insert') is not null drop table #p_nmo_hub_task_audit_insert
create table dbo.#p_nmo_hub_task_audit_insert with(distribution=hash(bk_hash), location=user_db) as
select p_nmo_hub_task_audit.p_nmo_hub_task_audit_id,
       p_nmo_hub_task_audit.bk_hash
  from dbo.p_nmo_hub_task_audit
 where p_nmo_hub_task_audit.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_nmo_hub_task_audit.dv_batch_id > @max_dv_batch_id
        or p_nmo_hub_task_audit.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_nmo_hub_task_audit.bk_hash,
       p_nmo_hub_task_audit.id hub_task_audit_id,
       s_nmo_hub_task_audit.created_date created_date,
       case when p_nmo_hub_task_audit.bk_hash in('-997', '-998', '-999') then p_nmo_hub_task_audit.bk_hash
           when s_nmo_hub_task_audit.created_date is null then '-998'
        else convert(varchar, s_nmo_hub_task_audit.created_date, 112)  end created_dim_date_key,
       case when p_nmo_hub_task_audit.bk_hash in ('-997','-998','-999') then p_nmo_hub_task_audit.bk_hash
       when s_nmo_hub_task_audit.created_date is null then '-998'
       else '1' + replace(substring(convert(varchar,s_nmo_hub_task_audit.created_date,114), 1, 5),':','') end created_dim_time_key,
       case when p_nmo_hub_task_audit.bk_hash in('-997', '-998', '-999') then p_nmo_hub_task_audit.bk_hash
           when l_nmo_hub_task_audit.hub_task_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_nmo_hub_task_audit.hub_task_id as int ) as varchar(500)),'z#@$k%&P'))),2)   end dim_nmo_hub_task_key,
       s_nmo_hub_task_audit.field field,
       s_nmo_hub_task_audit.modified_name modified_name,
       l_nmo_hub_task_audit.modified_party_id modified_party_id,
       s_nmo_hub_task_audit.new_value new_value,
       s_nmo_hub_task_audit.old_value old_value,
       s_nmo_hub_task_audit.operation operation,
       s_nmo_hub_task_audit.updated_date updated_date,
       case when p_nmo_hub_task_audit.bk_hash in('-997', '-998', '-999') then p_nmo_hub_task_audit.bk_hash
           when s_nmo_hub_task_audit.updated_date is null then '-998'
        else convert(varchar, s_nmo_hub_task_audit.updated_date, 112)  end updated_dim_date_key,
       case when p_nmo_hub_task_audit.bk_hash in ('-997','-998','-999') then p_nmo_hub_task_audit.bk_hash
       when s_nmo_hub_task_audit.updated_date is null then '-998'
       else '1' + replace(substring(convert(varchar,s_nmo_hub_task_audit.updated_date,114), 1, 5),':','') end updated_dim_time_key,
       isnull(h_nmo_hub_task_audit.dv_deleted,0) dv_deleted,
       p_nmo_hub_task_audit.p_nmo_hub_task_audit_id,
       p_nmo_hub_task_audit.dv_batch_id,
       p_nmo_hub_task_audit.dv_load_date_time,
       p_nmo_hub_task_audit.dv_load_end_date_time
  from dbo.h_nmo_hub_task_audit
  join dbo.p_nmo_hub_task_audit
    on h_nmo_hub_task_audit.bk_hash = p_nmo_hub_task_audit.bk_hash
  join #p_nmo_hub_task_audit_insert
    on p_nmo_hub_task_audit.bk_hash = #p_nmo_hub_task_audit_insert.bk_hash
   and p_nmo_hub_task_audit.p_nmo_hub_task_audit_id = #p_nmo_hub_task_audit_insert.p_nmo_hub_task_audit_id
  join dbo.l_nmo_hub_task_audit
    on p_nmo_hub_task_audit.bk_hash = l_nmo_hub_task_audit.bk_hash
   and p_nmo_hub_task_audit.l_nmo_hub_task_audit_id = l_nmo_hub_task_audit.l_nmo_hub_task_audit_id
  join dbo.s_nmo_hub_task_audit
    on p_nmo_hub_task_audit.bk_hash = s_nmo_hub_task_audit.bk_hash
   and p_nmo_hub_task_audit.s_nmo_hub_task_audit_id = s_nmo_hub_task_audit.s_nmo_hub_task_audit_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_nmo_hub_task_audit
   where d_nmo_hub_task_audit.bk_hash in (select bk_hash from #p_nmo_hub_task_audit_insert)

  insert dbo.d_nmo_hub_task_audit(
             bk_hash,
             hub_task_audit_id,
             created_date,
             created_dim_date_key,
             created_dim_time_key,
             dim_nmo_hub_task_key,
             field,
             modified_name,
             modified_party_id,
             new_value,
             old_value,
             operation,
             updated_date,
             updated_dim_date_key,
             updated_dim_time_key,
             deleted_flag,
             p_nmo_hub_task_audit_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         hub_task_audit_id,
         created_date,
         created_dim_date_key,
         created_dim_time_key,
         dim_nmo_hub_task_key,
         field,
         modified_name,
         modified_party_id,
         new_value,
         old_value,
         operation,
         updated_date,
         updated_dim_date_key,
         updated_dim_time_key,
         dv_deleted,
         p_nmo_hub_task_audit_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_nmo_hub_task_audit)
--Done!
end
