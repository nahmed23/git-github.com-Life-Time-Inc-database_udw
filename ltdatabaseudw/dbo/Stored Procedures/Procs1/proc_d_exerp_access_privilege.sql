CREATE PROC [dbo].[proc_d_exerp_access_privilege] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_exerp_access_privilege)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_exerp_access_privilege_insert') is not null drop table #p_exerp_access_privilege_insert
create table dbo.#p_exerp_access_privilege_insert with(distribution=hash(bk_hash), location=user_db) as
select p_exerp_access_privilege.p_exerp_access_privilege_id,
       p_exerp_access_privilege.bk_hash
  from dbo.p_exerp_access_privilege
 where p_exerp_access_privilege.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_exerp_access_privilege.dv_batch_id > @max_dv_batch_id
        or p_exerp_access_privilege.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_exerp_access_privilege.bk_hash,
       p_exerp_access_privilege.access_privilege_id access_privilege_id,
       l_exerp_access_privilege.access_group_id access_group_id,
       l_exerp_access_privilege.scope_id access_privilege_scope_id,
       s_exerp_access_privilege.scope_type access_privilege_scope_type,
       case when p_exerp_access_privilege.bk_hash in ('-997','-998','-999') then p_exerp_access_privilege.bk_hash     
         when l_exerp_access_privilege.access_group_id is null then '-998'   
         else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_exerp_access_privilege.access_group_id as int) as varchar(500)),'z#@$k%&P'))),2)   end d_exerp_access_group_bk_hash,
       case when p_exerp_access_privilege.bk_hash in ('-997','-998','-999') then p_exerp_access_privilege.bk_hash     
         when l_exerp_access_privilege.scope_id is null then '-998'   
         else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_exerp_access_privilege.scope_id as int) as varchar(500)),'z#@$k%&P'))),2)   end d_exerp_access_privilege_scope_bk_hash,
       case when p_exerp_access_privilege.bk_hash in ('-997','-998','-999') then p_exerp_access_privilege.bk_hash     
         when l_exerp_access_privilege.privilege_set_id is null then '-998'   
         else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_exerp_access_privilege.privilege_set_id as int) as varchar(500)),'z#@$k%&P'))),2)   end dim_exerp_privilege_set_key,
       l_exerp_access_privilege.privilege_set_id privilege_set_id,
       isnull(h_exerp_access_privilege.dv_deleted,0) dv_deleted,
       p_exerp_access_privilege.p_exerp_access_privilege_id,
       p_exerp_access_privilege.dv_batch_id,
       p_exerp_access_privilege.dv_load_date_time,
       p_exerp_access_privilege.dv_load_end_date_time
  from dbo.h_exerp_access_privilege
  join dbo.p_exerp_access_privilege
    on h_exerp_access_privilege.bk_hash = p_exerp_access_privilege.bk_hash
  join #p_exerp_access_privilege_insert
    on p_exerp_access_privilege.bk_hash = #p_exerp_access_privilege_insert.bk_hash
   and p_exerp_access_privilege.p_exerp_access_privilege_id = #p_exerp_access_privilege_insert.p_exerp_access_privilege_id
  join dbo.l_exerp_access_privilege
    on p_exerp_access_privilege.bk_hash = l_exerp_access_privilege.bk_hash
   and p_exerp_access_privilege.l_exerp_access_privilege_id = l_exerp_access_privilege.l_exerp_access_privilege_id
  join dbo.s_exerp_access_privilege
    on p_exerp_access_privilege.bk_hash = s_exerp_access_privilege.bk_hash
   and p_exerp_access_privilege.s_exerp_access_privilege_id = s_exerp_access_privilege.s_exerp_access_privilege_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_exerp_access_privilege
   where d_exerp_access_privilege.bk_hash in (select bk_hash from #p_exerp_access_privilege_insert)

  insert dbo.d_exerp_access_privilege(
             bk_hash,
             access_privilege_id,
             access_group_id,
             access_privilege_scope_id,
             access_privilege_scope_type,
             d_exerp_access_group_bk_hash,
             d_exerp_access_privilege_scope_bk_hash,
             dim_exerp_privilege_set_key,
             privilege_set_id,
             deleted_flag,
             p_exerp_access_privilege_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         access_privilege_id,
         access_group_id,
         access_privilege_scope_id,
         access_privilege_scope_type,
         d_exerp_access_group_bk_hash,
         d_exerp_access_privilege_scope_bk_hash,
         dim_exerp_privilege_set_key,
         privilege_set_id,
         dv_deleted,
         p_exerp_access_privilege_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_exerp_access_privilege)
--Done!
end
