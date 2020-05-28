﻿CREATE PROC [dbo].[proc_d_ig_ig_dimension_tender_dimension] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_ig_ig_dimension_tender_dimension)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_ig_ig_dimension_tender_dimension_insert') is not null drop table #p_ig_ig_dimension_tender_dimension_insert
create table dbo.#p_ig_ig_dimension_tender_dimension_insert with(distribution=hash(bk_hash), location=user_db) as
select p_ig_ig_dimension_tender_dimension.p_ig_ig_dimension_tender_dimension_id,
       p_ig_ig_dimension_tender_dimension.bk_hash
  from dbo.p_ig_ig_dimension_tender_dimension
 where p_ig_ig_dimension_tender_dimension.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_ig_ig_dimension_tender_dimension.dv_batch_id > @max_dv_batch_id
        or p_ig_ig_dimension_tender_dimension.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_ig_ig_dimension_tender_dimension.bk_hash,
       p_ig_ig_dimension_tender_dimension.tender_dim_id tender_dim_id,
       s_ig_ig_dimension_tender_dimension_1.additional_check_id_code_id additional_check_id_code_id,
       case when s_ig_ig_dimension_tender_dimension.cash_tender_flag = 1 then 'Y' else 'N'   end cash_tender_flag,
       case when s_ig_ig_dimension_tender_dimension.comp_tender_flag = 1 then 'Y' else 'N'   end comp_tender_flag,
       l_ig_ig_dimension_tender_dimension.corp_id corp_id,
       l_ig_ig_dimension_tender_dimension.customer_id customer_id,
        case when p_ig_ig_dimension_tender_dimension.bk_hash in ('-997', '-998', '-999') then p_ig_ig_dimension_tender_dimension.bk_hash
        when l_ig_ig_dimension_tender_dimension.tender_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_ig_ig_dimension_tender_dimension.tender_id as varchar(500)),'z#@$k%&P'))),2)   
        end dim_cafe_payment_type_key,
       s_ig_ig_dimension_tender_dimension.eff_date_from eff_date_from,
       s_ig_ig_dimension_tender_dimension.eff_date_to eff_date_to,
       convert(varchar,s_ig_ig_dimension_tender_dimension.eff_date_from, 112) effective_dim_date_key,
       l_ig_ig_dimension_tender_dimension.ent_id ent_id,
       isnull(convert(varchar,s_ig_ig_dimension_tender_dimension.eff_date_to, 112),'99991231') expiration_dim_date_key,
       isnull(s_ig_ig_dimension_tender_dimension.tender_class_name, '') payment_class,
       l_ig_ig_dimension_tender_dimension.tender_class_id payment_id,
       isnull(s_ig_ig_dimension_tender_dimension.tender_name, '') payment_type,
       l_ig_ig_dimension_tender_dimension.profit_center_dim_level_2_id profit_center_dim_level_2_id,
       l_ig_ig_dimension_tender_dimension.tender_id tender_id,
       isnull(h_ig_ig_dimension_tender_dimension.dv_deleted,0) dv_deleted,
       p_ig_ig_dimension_tender_dimension.p_ig_ig_dimension_tender_dimension_id,
       p_ig_ig_dimension_tender_dimension.dv_batch_id,
       p_ig_ig_dimension_tender_dimension.dv_load_date_time,
       p_ig_ig_dimension_tender_dimension.dv_load_end_date_time
  from dbo.h_ig_ig_dimension_tender_dimension
  join dbo.p_ig_ig_dimension_tender_dimension
    on h_ig_ig_dimension_tender_dimension.bk_hash = p_ig_ig_dimension_tender_dimension.bk_hash
  join #p_ig_ig_dimension_tender_dimension_insert
    on p_ig_ig_dimension_tender_dimension.bk_hash = #p_ig_ig_dimension_tender_dimension_insert.bk_hash
   and p_ig_ig_dimension_tender_dimension.p_ig_ig_dimension_tender_dimension_id = #p_ig_ig_dimension_tender_dimension_insert.p_ig_ig_dimension_tender_dimension_id
  join dbo.l_ig_ig_dimension_tender_dimension
    on p_ig_ig_dimension_tender_dimension.bk_hash = l_ig_ig_dimension_tender_dimension.bk_hash
   and p_ig_ig_dimension_tender_dimension.l_ig_ig_dimension_Tender_Dimension_id = l_ig_ig_dimension_tender_dimension.l_ig_ig_dimension_Tender_Dimension_id
  join dbo.s_ig_ig_dimension_tender_dimension
    on p_ig_ig_dimension_tender_dimension.bk_hash = s_ig_ig_dimension_tender_dimension.bk_hash
   and p_ig_ig_dimension_tender_dimension.s_ig_ig_dimension_Tender_Dimension_id = s_ig_ig_dimension_tender_dimension.s_ig_ig_dimension_Tender_Dimension_id
  join dbo.s_ig_ig_dimension_tender_dimension_1
    on p_ig_ig_dimension_tender_dimension.bk_hash = s_ig_ig_dimension_tender_dimension_1.bk_hash
   and p_ig_ig_dimension_tender_dimension.s_ig_ig_dimension_tender_dimension_1_id = s_ig_ig_dimension_tender_dimension_1.s_ig_ig_dimension_tender_dimension_1_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_ig_ig_dimension_tender_dimension
   where d_ig_ig_dimension_tender_dimension.bk_hash in (select bk_hash from #p_ig_ig_dimension_tender_dimension_insert)

  insert dbo.d_ig_ig_dimension_tender_dimension(
             bk_hash,
             tender_dim_id,
             additional_check_id_code_id,
             cash_tender_flag,
             comp_tender_flag,
             corp_id,
             customer_id,
             dim_cafe_payment_type_key,
             eff_date_from,
             eff_date_to,
             effective_dim_date_key,
             ent_id,
             expiration_dim_date_key,
             payment_class,
             payment_id,
             payment_type,
             profit_center_dim_level_2_id,
             tender_id,
             deleted_flag,
             p_ig_ig_dimension_tender_dimension_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         tender_dim_id,
         additional_check_id_code_id,
         cash_tender_flag,
         comp_tender_flag,
         corp_id,
         customer_id,
         dim_cafe_payment_type_key,
         eff_date_from,
         eff_date_to,
         effective_dim_date_key,
         ent_id,
         expiration_dim_date_key,
         payment_class,
         payment_id,
         payment_type,
         profit_center_dim_level_2_id,
         tender_id,
         dv_deleted,
         p_ig_ig_dimension_tender_dimension_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_ig_ig_dimension_tender_dimension)
--Done!
end
