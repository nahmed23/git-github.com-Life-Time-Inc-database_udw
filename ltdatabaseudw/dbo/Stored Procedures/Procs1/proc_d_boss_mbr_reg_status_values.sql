﻿CREATE PROC [dbo].[proc_d_boss_mbr_reg_status_values] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_boss_mbr_reg_status_values)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_boss_mbr_reg_status_values_insert') is not null drop table #p_boss_mbr_reg_status_values_insert
create table dbo.#p_boss_mbr_reg_status_values_insert with(distribution=hash(bk_hash), location=user_db) as
select p_boss_mbr_reg_status_values.p_boss_mbr_reg_status_values_id,
       p_boss_mbr_reg_status_values.bk_hash
  from dbo.p_boss_mbr_reg_status_values
 where p_boss_mbr_reg_status_values.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_boss_mbr_reg_status_values.dv_batch_id > @max_dv_batch_id
        or p_boss_mbr_reg_status_values.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_boss_mbr_reg_status_values.bk_hash,
       p_boss_mbr_reg_status_values.mbr_reg_status_values_id mbr_reg_status_values_id,
       case when p_boss_mbr_reg_status_values.bk_hash in('-997', '-998', '-999') then p_boss_mbr_reg_status_values.bk_hash
            when s_boss_mbr_reg_status_values.created_at is null then '-998'
         else convert(varchar, s_boss_mbr_reg_status_values.created_at, 112)    end created_dim_date_key,
       case when p_boss_mbr_reg_status_values.bk_hash in ('-997','-998','-999') then p_boss_mbr_reg_status_values.bk_hash
        when s_boss_mbr_reg_status_values.created_at is null then '-998'
        else '1' + replace(substring(convert(varchar,s_boss_mbr_reg_status_values.created_at,114), 1, 5),':','') end created_dim_time_key,
       case when p_boss_mbr_reg_status_values.bk_hash in('-997', '-998', '-999') then p_boss_mbr_reg_status_values.bk_hash
            when l_boss_mbr_reg_status_values.reg_status_type_id is null then '-998'
         else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_boss_mbr_reg_status_values.reg_status_type_id as int) as varchar(500)),'z#@$k%&P'))),2)   end d_mbr_reg_status_types_bk_hash,
       case when p_boss_mbr_reg_status_values.bk_hash in('-997', '-998', '-999') then p_boss_mbr_reg_status_values.bk_hash
            when s_boss_mbr_reg_status_values.end_date is null then '-998'
        	when  convert(varchar, s_boss_mbr_reg_status_values.end_date, 112) > '20991231'  then '99991231'
         else convert(varchar, s_boss_mbr_reg_status_values.end_date, 112)    end end_dim_date_key,
       case when p_boss_mbr_reg_status_values.bk_hash in ('-997','-998','-999') then p_boss_mbr_reg_status_values.bk_hash
        when s_boss_mbr_reg_status_values.end_date is null then '-998'
        else '1' + replace(substring(convert(varchar,s_boss_mbr_reg_status_values.end_date,114), 1, 5),':','') end end_dim_time_key,
       s_boss_mbr_reg_status_values.cust_code mbr_reg_status_values_cust_code,
       s_boss_mbr_reg_status_values.mbr_code mbr_reg_status_values_mbr_code,
       s_boss_mbr_reg_status_values.value mbr_reg_status_values_value,
       l_boss_mbr_reg_status_values.reg_status_type_id reg_status_type_id,
       case when p_boss_mbr_reg_status_values.bk_hash in('-997', '-998', '-999') then p_boss_mbr_reg_status_values.bk_hash
            when s_boss_mbr_reg_status_values.start_date is null then '-998'
        	when  convert(varchar, s_boss_mbr_reg_status_values.start_date, 112) > '20991231'  then '99991231'
         else convert(varchar, s_boss_mbr_reg_status_values.start_date, 112)    end start_dim_date_key,
       case when p_boss_mbr_reg_status_values.bk_hash in ('-997','-998','-999') then p_boss_mbr_reg_status_values.bk_hash
        when s_boss_mbr_reg_status_values.start_date is null then '-998'
        else '1' + replace(substring(convert(varchar,s_boss_mbr_reg_status_values.start_date,114), 1, 5),':','') end start_dim_time_key,
       case when p_boss_mbr_reg_status_values.bk_hash in('-997', '-998', '-999') then p_boss_mbr_reg_status_values.bk_hash
            when s_boss_mbr_reg_status_values.updated_at is null then '-998'
         else convert(varchar, s_boss_mbr_reg_status_values.updated_at, 112)    end updated_dim_date_key,
       case when p_boss_mbr_reg_status_values.bk_hash in ('-997','-998','-999') then p_boss_mbr_reg_status_values.bk_hash
        when s_boss_mbr_reg_status_values.updated_at is null then '-998'
        else '1' + replace(substring(convert(varchar,s_boss_mbr_reg_status_values.updated_at,114), 1, 5),':','') end updated_dim_time_key,
       isnull(h_boss_mbr_reg_status_values.dv_deleted,0) dv_deleted,
       p_boss_mbr_reg_status_values.p_boss_mbr_reg_status_values_id,
       p_boss_mbr_reg_status_values.dv_batch_id,
       p_boss_mbr_reg_status_values.dv_load_date_time,
       p_boss_mbr_reg_status_values.dv_load_end_date_time
  from dbo.h_boss_mbr_reg_status_values
  join dbo.p_boss_mbr_reg_status_values
    on h_boss_mbr_reg_status_values.bk_hash = p_boss_mbr_reg_status_values.bk_hash
  join #p_boss_mbr_reg_status_values_insert
    on p_boss_mbr_reg_status_values.bk_hash = #p_boss_mbr_reg_status_values_insert.bk_hash
   and p_boss_mbr_reg_status_values.p_boss_mbr_reg_status_values_id = #p_boss_mbr_reg_status_values_insert.p_boss_mbr_reg_status_values_id
  join dbo.l_boss_mbr_reg_status_values
    on p_boss_mbr_reg_status_values.bk_hash = l_boss_mbr_reg_status_values.bk_hash
   and p_boss_mbr_reg_status_values.l_boss_mbr_reg_status_values_id = l_boss_mbr_reg_status_values.l_boss_mbr_reg_status_values_id
  join dbo.s_boss_mbr_reg_status_values
    on p_boss_mbr_reg_status_values.bk_hash = s_boss_mbr_reg_status_values.bk_hash
   and p_boss_mbr_reg_status_values.s_boss_mbr_reg_status_values_id = s_boss_mbr_reg_status_values.s_boss_mbr_reg_status_values_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_boss_mbr_reg_status_values
   where d_boss_mbr_reg_status_values.bk_hash in (select bk_hash from #p_boss_mbr_reg_status_values_insert)

  insert dbo.d_boss_mbr_reg_status_values(
             bk_hash,
             mbr_reg_status_values_id,
             created_dim_date_key,
             created_dim_time_key,
             d_mbr_reg_status_types_bk_hash,
             end_dim_date_key,
             end_dim_time_key,
             mbr_reg_status_values_cust_code,
             mbr_reg_status_values_mbr_code,
             mbr_reg_status_values_value,
             reg_status_type_id,
             start_dim_date_key,
             start_dim_time_key,
             updated_dim_date_key,
             updated_dim_time_key,
             deleted_flag,
             p_boss_mbr_reg_status_values_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         mbr_reg_status_values_id,
         created_dim_date_key,
         created_dim_time_key,
         d_mbr_reg_status_types_bk_hash,
         end_dim_date_key,
         end_dim_time_key,
         mbr_reg_status_values_cust_code,
         mbr_reg_status_values_mbr_code,
         mbr_reg_status_values_value,
         reg_status_type_id,
         start_dim_date_key,
         start_dim_time_key,
         updated_dim_date_key,
         updated_dim_time_key,
         dv_deleted,
         p_boss_mbr_reg_status_values_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_boss_mbr_reg_status_values)
--Done!
end
