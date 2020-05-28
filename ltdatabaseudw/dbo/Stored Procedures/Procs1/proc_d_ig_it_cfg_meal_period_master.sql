﻿CREATE PROC [dbo].[proc_d_ig_it_cfg_meal_period_master] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_ig_it_cfg_meal_period_master)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_ig_it_cfg_meal_period_master_insert') is not null drop table #p_ig_it_cfg_meal_period_master_insert
create table dbo.#p_ig_it_cfg_meal_period_master_insert with(distribution=hash(bk_hash), location=user_db) as
select p_ig_it_cfg_meal_period_master.p_ig_it_cfg_meal_period_master_id,
       p_ig_it_cfg_meal_period_master.bk_hash
  from dbo.p_ig_it_cfg_meal_period_master
 where p_ig_it_cfg_meal_period_master.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_ig_it_cfg_meal_period_master.dv_batch_id > @max_dv_batch_id
        or p_ig_it_cfg_meal_period_master.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_ig_it_cfg_meal_period_master.bk_hash,
       p_ig_it_cfg_meal_period_master.ent_id ent_id,
       p_ig_it_cfg_meal_period_master.meal_period_id meal_period_id,
       l_ig_it_cfg_meal_period_master.default_check_type_id default_check_type_id,
       case when p_ig_it_cfg_meal_period_master.bk_hash in ('-997','-998','-999') then p_ig_it_cfg_meal_period_master.bk_hash     
         when l_ig_it_cfg_meal_period_master.default_check_type_id is null then '-998'   
         else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_ig_it_cfg_meal_period_master.default_check_type_id as int) as varchar(500)),'z#@$k%&P'))),2)   end default_dim_cafe_check_type_key,
       l_ig_it_cfg_meal_period_master.default_price_level_id default_price_level_id,
       case when p_ig_it_cfg_meal_period_master.bk_hash in ('-997','-998','-999') then p_ig_it_cfg_meal_period_master.bk_hash     
         when p_ig_it_cfg_meal_period_master.meal_period_id is null then '-998'   
         else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(p_ig_it_cfg_meal_period_master.meal_period_id as      varchar(500)),'z#@$k%&P'))),2)   end dim_cafe_meal_period_key,
       s_ig_it_cfg_meal_period_master.enterprise_created_id enterprise_created_id,
       case when s_ig_it_cfg_meal_period_master.entertainment_flag = 1 then 'Y' else 'N'   end entertainment_flag,
       isnull(s_ig_it_cfg_meal_period_master.meal_period_abbr_1,'') meal_period_abbr_1,
       isnull(s_ig_it_cfg_meal_period_master.meal_period_abbr_2,'') meal_period_abbr_2,
       isnull(s_ig_it_cfg_meal_period_master.meal_period_name,'') meal_period_name,
       l_ig_it_cfg_meal_period_master.meal_period_sec_id meal_period_sec_id,
       isnull(s_ig_it_cfg_meal_period_master.receipt_code, '') receipt_code,
       s_ig_it_cfg_meal_period_master.row_version row_version,
       case when p_ig_it_cfg_meal_period_master.bk_hash in ('-997','-998','-999') then p_ig_it_cfg_meal_period_master.bk_hash     
         when l_ig_it_cfg_meal_period_master.meal_period_sec_id is null then '-998' 
         else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_ig_it_cfg_meal_period_master.meal_period_sec_id as  varchar(500)),'z#@$k%&P'))),2)  end secondary_dim_cafe_meal_period_key,
       l_ig_it_cfg_meal_period_master.store_id store_id,
       isnull(h_ig_it_cfg_meal_period_master.dv_deleted,0) dv_deleted,
       p_ig_it_cfg_meal_period_master.p_ig_it_cfg_meal_period_master_id,
       p_ig_it_cfg_meal_period_master.dv_batch_id,
       p_ig_it_cfg_meal_period_master.dv_load_date_time,
       p_ig_it_cfg_meal_period_master.dv_load_end_date_time
  from dbo.h_ig_it_cfg_meal_period_master
  join dbo.p_ig_it_cfg_meal_period_master
    on h_ig_it_cfg_meal_period_master.bk_hash = p_ig_it_cfg_meal_period_master.bk_hash
  join #p_ig_it_cfg_meal_period_master_insert
    on p_ig_it_cfg_meal_period_master.bk_hash = #p_ig_it_cfg_meal_period_master_insert.bk_hash
   and p_ig_it_cfg_meal_period_master.p_ig_it_cfg_meal_period_master_id = #p_ig_it_cfg_meal_period_master_insert.p_ig_it_cfg_meal_period_master_id
  join dbo.l_ig_it_cfg_meal_period_master
    on p_ig_it_cfg_meal_period_master.bk_hash = l_ig_it_cfg_meal_period_master.bk_hash
   and p_ig_it_cfg_meal_period_master.l_ig_it_cfg_meal_period_master_id = l_ig_it_cfg_meal_period_master.l_ig_it_cfg_meal_period_master_id
  join dbo.s_ig_it_cfg_meal_period_master
    on p_ig_it_cfg_meal_period_master.bk_hash = s_ig_it_cfg_meal_period_master.bk_hash
   and p_ig_it_cfg_meal_period_master.s_ig_it_cfg_meal_period_master_id = s_ig_it_cfg_meal_period_master.s_ig_it_cfg_meal_period_master_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_ig_it_cfg_meal_period_master
   where d_ig_it_cfg_meal_period_master.bk_hash in (select bk_hash from #p_ig_it_cfg_meal_period_master_insert)

  insert dbo.d_ig_it_cfg_meal_period_master(
             bk_hash,
             ent_id,
             meal_period_id,
             default_check_type_id,
             default_dim_cafe_check_type_key,
             default_price_level_id,
             dim_cafe_meal_period_key,
             enterprise_created_id,
             entertainment_flag,
             meal_period_abbr_1,
             meal_period_abbr_2,
             meal_period_name,
             meal_period_sec_id,
             receipt_code,
             row_version,
             secondary_dim_cafe_meal_period_key,
             store_id,
             deleted_flag,
             p_ig_it_cfg_meal_period_master_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         ent_id,
         meal_period_id,
         default_check_type_id,
         default_dim_cafe_check_type_key,
         default_price_level_id,
         dim_cafe_meal_period_key,
         enterprise_created_id,
         entertainment_flag,
         meal_period_abbr_1,
         meal_period_abbr_2,
         meal_period_name,
         meal_period_sec_id,
         receipt_code,
         row_version,
         secondary_dim_cafe_meal_period_key,
         store_id,
         dv_deleted,
         p_ig_it_cfg_meal_period_master_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_ig_it_cfg_meal_period_master)
--Done!
end
