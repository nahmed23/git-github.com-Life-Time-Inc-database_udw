CREATE PROC [dbo].[proc_d_commprefs_communication_types] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_commprefs_communication_types)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_commprefs_communication_types_insert') is not null drop table #p_commprefs_communication_types_insert
create table dbo.#p_commprefs_communication_types_insert with(distribution=hash(bk_hash), location=user_db) as
select p_commprefs_communication_types.p_commprefs_communication_types_id,
       p_commprefs_communication_types.bk_hash
  from dbo.p_commprefs_communication_types
 where p_commprefs_communication_types.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_commprefs_communication_types.dv_batch_id > @max_dv_batch_id
        or p_commprefs_communication_types.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_commprefs_communication_types.bk_hash,
       p_commprefs_communication_types.communication_types_id communication_types_id,
       case when p_commprefs_communication_types.bk_hash in('-997', '-998', '-999') then p_commprefs_communication_types.bk_hash
    when s_commprefs_communication_types.active_on is null then '-998'
	else convert(varchar, s_commprefs_communication_types.active_on, 112) 
end active_on_dim_date_key,
       case when p_commprefs_communication_types.bk_hash in('-997', '-998', '-999') then p_commprefs_communication_types.bk_hash
    when s_commprefs_communication_types.active_until is null then '-998'
	else convert(varchar, s_commprefs_communication_types.active_until, 112) 
end active_until_dim_date_key,
       case when p_commprefs_communication_types.bk_hash in('-997', '-998', '-999') then p_commprefs_communication_types.bk_hash
    when s_commprefs_communication_types.created_time is null then '-998'
	else convert(varchar, s_commprefs_communication_types.created_time, 112) 
end created_dim_date_key,
       isnull(s_commprefs_communication_types.name,'') name,
       case when s_commprefs_communication_types.opt_in_required='1' then 'Y'
	else 'N'
end opt_in_flag,
       isnull(s_commprefs_communication_types.slug,'') slug,
       p_commprefs_communication_types.p_commprefs_communication_types_id,
       p_commprefs_communication_types.dv_batch_id,
       p_commprefs_communication_types.dv_load_date_time,
       p_commprefs_communication_types.dv_load_end_date_time
  from dbo.h_commprefs_communication_types
  join dbo.p_commprefs_communication_types
    on h_commprefs_communication_types.bk_hash = p_commprefs_communication_types.bk_hash  join #p_commprefs_communication_types_insert
    on p_commprefs_communication_types.bk_hash = #p_commprefs_communication_types_insert.bk_hash
   and p_commprefs_communication_types.p_commprefs_communication_types_id = #p_commprefs_communication_types_insert.p_commprefs_communication_types_id
  join dbo.l_commprefs_communication_types
    on p_commprefs_communication_types.bk_hash = l_commprefs_communication_types.bk_hash
   and p_commprefs_communication_types.l_commprefs_communication_types_id = l_commprefs_communication_types.l_commprefs_communication_types_id
  join dbo.s_commprefs_communication_types
    on p_commprefs_communication_types.bk_hash = s_commprefs_communication_types.bk_hash
   and p_commprefs_communication_types.s_commprefs_communication_types_id = s_commprefs_communication_types.s_commprefs_communication_types_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_commprefs_communication_types
   where d_commprefs_communication_types.bk_hash in (select bk_hash from #p_commprefs_communication_types_insert)

  insert dbo.d_commprefs_communication_types(
             bk_hash,
             communication_types_id,
             active_on_dim_date_key,
             active_until_dim_date_key,
             created_dim_date_key,
             name,
             opt_in_flag,
             slug,
             p_commprefs_communication_types_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         communication_types_id,
         active_on_dim_date_key,
         active_until_dim_date_key,
         created_dim_date_key,
         name,
         opt_in_flag,
         slug,
         p_commprefs_communication_types_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_commprefs_communication_types)
--Done!
end
