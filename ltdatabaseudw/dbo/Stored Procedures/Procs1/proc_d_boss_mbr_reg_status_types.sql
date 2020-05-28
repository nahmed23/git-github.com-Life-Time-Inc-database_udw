CREATE PROC [dbo].[proc_d_boss_mbr_reg_status_types] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_boss_mbr_reg_status_types)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_boss_mbr_reg_status_types_insert') is not null drop table #p_boss_mbr_reg_status_types_insert
create table dbo.#p_boss_mbr_reg_status_types_insert with(distribution=hash(bk_hash), location=user_db) as
select p_boss_mbr_reg_status_types.p_boss_mbr_reg_status_types_id,
       p_boss_mbr_reg_status_types.bk_hash
  from dbo.p_boss_mbr_reg_status_types
 where p_boss_mbr_reg_status_types.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_boss_mbr_reg_status_types.dv_batch_id > @max_dv_batch_id
        or p_boss_mbr_reg_status_types.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_boss_mbr_reg_status_types.bk_hash,
       p_boss_mbr_reg_status_types.mbr_reg_status_types_id mbr_reg_status_types_id,
       case when p_boss_mbr_reg_status_types.bk_hash in('-997', '-998', '-999') then p_boss_mbr_reg_status_types.bk_hash
            when s_boss_mbr_reg_status_types.created_at is null then '-998'
         else convert(varchar, s_boss_mbr_reg_status_types.created_at, 112)    end created_dim_date_key,
       case when p_boss_mbr_reg_status_types.bk_hash in ('-997','-998','-999') then p_boss_mbr_reg_status_types.bk_hash
        when s_boss_mbr_reg_status_types.created_at is null then '-998'
        else '1' + replace(substring(convert(varchar,s_boss_mbr_reg_status_types.created_at,114), 1, 5),':','') end created_dim_time_key,
       case when s_boss_mbr_reg_status_types.active = 'Y' then 'Y'     else 'N'  end mbr_reg_status_types_active_flag,
       s_boss_mbr_reg_status_types.description mbr_reg_status_types_description,
       s_boss_mbr_reg_status_types.name mbr_reg_status_types_name,
       s_boss_mbr_reg_status_types.position mbr_reg_status_types_position,
       s_boss_mbr_reg_status_types.term_type mbr_reg_status_types_term_type,
       case when p_boss_mbr_reg_status_types.bk_hash in('-997', '-998', '-999') then p_boss_mbr_reg_status_types.bk_hash
            when s_boss_mbr_reg_status_types.updated_at is null then '-998'
         else convert(varchar, s_boss_mbr_reg_status_types.updated_at, 112)    end updated_dim_date_key,
       case when p_boss_mbr_reg_status_types.bk_hash in ('-997','-998','-999') then p_boss_mbr_reg_status_types.bk_hash
        when s_boss_mbr_reg_status_types.updated_at is null then '-998'
        else '1' + replace(substring(convert(varchar,s_boss_mbr_reg_status_types.updated_at,114), 1, 5),':','') end updated_dim_time_key,
       isnull(h_boss_mbr_reg_status_types.dv_deleted,0) dv_deleted,
       p_boss_mbr_reg_status_types.p_boss_mbr_reg_status_types_id,
       p_boss_mbr_reg_status_types.dv_batch_id,
       p_boss_mbr_reg_status_types.dv_load_date_time,
       p_boss_mbr_reg_status_types.dv_load_end_date_time
  from dbo.h_boss_mbr_reg_status_types
  join dbo.p_boss_mbr_reg_status_types
    on h_boss_mbr_reg_status_types.bk_hash = p_boss_mbr_reg_status_types.bk_hash
  join #p_boss_mbr_reg_status_types_insert
    on p_boss_mbr_reg_status_types.bk_hash = #p_boss_mbr_reg_status_types_insert.bk_hash
   and p_boss_mbr_reg_status_types.p_boss_mbr_reg_status_types_id = #p_boss_mbr_reg_status_types_insert.p_boss_mbr_reg_status_types_id
  join dbo.s_boss_mbr_reg_status_types
    on p_boss_mbr_reg_status_types.bk_hash = s_boss_mbr_reg_status_types.bk_hash
   and p_boss_mbr_reg_status_types.s_boss_mbr_reg_status_types_id = s_boss_mbr_reg_status_types.s_boss_mbr_reg_status_types_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_boss_mbr_reg_status_types
   where d_boss_mbr_reg_status_types.bk_hash in (select bk_hash from #p_boss_mbr_reg_status_types_insert)

  insert dbo.d_boss_mbr_reg_status_types(
             bk_hash,
             mbr_reg_status_types_id,
             created_dim_date_key,
             created_dim_time_key,
             mbr_reg_status_types_active_flag,
             mbr_reg_status_types_description,
             mbr_reg_status_types_name,
             mbr_reg_status_types_position,
             mbr_reg_status_types_term_type,
             updated_dim_date_key,
             updated_dim_time_key,
             deleted_flag,
             p_boss_mbr_reg_status_types_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         mbr_reg_status_types_id,
         created_dim_date_key,
         created_dim_time_key,
         mbr_reg_status_types_active_flag,
         mbr_reg_status_types_description,
         mbr_reg_status_types_name,
         mbr_reg_status_types_position,
         mbr_reg_status_types_term_type,
         updated_dim_date_key,
         updated_dim_time_key,
         dv_deleted,
         p_boss_mbr_reg_status_types_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_boss_mbr_reg_status_types)
--Done!
end
