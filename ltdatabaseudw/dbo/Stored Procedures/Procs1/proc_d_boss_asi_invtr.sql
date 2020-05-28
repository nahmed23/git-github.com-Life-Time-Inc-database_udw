CREATE PROC [dbo].[proc_d_boss_asi_invtr] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_boss_asi_invtr)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_boss_asi_invtr_insert') is not null drop table #p_boss_asi_invtr_insert
create table dbo.#p_boss_asi_invtr_insert with(distribution=hash(bk_hash), location=user_db) as
select p_boss_asi_invtr.p_boss_asi_invtr_id,
       p_boss_asi_invtr.bk_hash
  from dbo.p_boss_asi_invtr
 where p_boss_asi_invtr.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_boss_asi_invtr.dv_batch_id > @max_dv_batch_id
        or p_boss_asi_invtr.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_boss_asi_invtr.bk_hash,
       p_boss_asi_invtr.invtr_upc_code invtr_upc_code,
       isnull(s_boss_asi_invtr.invtr_color,'') color,
       case when p_boss_asi_invtr.bk_hash in('-997', '-998', '-999') then p_boss_asi_invtr.bk_hash
    when s_boss_asi_invtr.invtr_created is null then '-998'
	else convert(varchar, s_boss_asi_invtr.invtr_created, 112) 
end created_dim_date_key,
       case when p_boss_asi_invtr.bk_hash in ('-997', '-998', '-999') then p_boss_asi_invtr.bk_hash
     when l_boss_asi_invtr.invtr_class is null then '-998'
     when l_boss_asi_invtr.invtr_dept is null then '-998'
 else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_boss_asi_invtr.invtr_dept as int) as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(cast(l_boss_asi_invtr.invtr_class as int) as varchar(500)),'z#@$k%&P'))),2) end d_boss_asi_class_r_bk_hash,
       case when p_boss_asi_invtr.bk_hash in ('-997', '-998', '-999') then p_boss_asi_invtr.bk_hash
    when l_boss_asi_invtr.invtr_dept is null then '-998'
    when l_boss_asi_invtr.invtr_class is null then '-998'
    when s_boss_asi_invtr.invtr_color is null then '-998'
 else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_boss_asi_invtr.invtr_dept as int) as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(cast(l_boss_asi_invtr.invtr_class as int) as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(s_boss_asi_invtr.invtr_color as char(8)),'z#@$k%&P'))),2) end d_boss_asi_color_r_bk_hash,
       case when p_boss_asi_invtr.bk_hash in ('-997', '-998', '-999') then p_boss_asi_invtr.bk_hash
     when l_boss_asi_invtr.invtr_dept is null then '-998'
 else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_boss_asi_invtr.invtr_dept as int) as varchar(500)),'z#@$k%&P'))),2) end d_boss_asi_dept_m_bk_hash,
       case when p_boss_asi_invtr.bk_hash in ('-997', '-998', '-999') then p_boss_asi_invtr.bk_hash
           when l_boss_asi_invtr.invtr_dept is null then '-998'
           when l_boss_asi_invtr.invtr_class is null then '-998'
           when l_boss_asi_invtr.invtr_size is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_boss_asi_invtr.invtr_dept as int) as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(cast(l_boss_asi_invtr.invtr_class as int) as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_boss_asi_invtr.invtr_size as char(8)),'z#@$k%&P'))),2) end d_boss_asi_size_r_bk_hash,
       case when p_boss_asi_invtr.bk_hash in ('-997', '-998', '-999') then p_boss_asi_invtr.bk_hash
           when l_boss_asi_invtr.invtr_upc_code is null then '-998'    
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(ltrim(rtrim(l_boss_asi_invtr.invtr_upc_code)) as char(15)),'z#@$k%&P'))),2) end dim_boss_product_key,
       case when p_boss_asi_invtr.bk_hash in ('-997', '-998', '-999') then p_boss_asi_invtr.bk_hash
     when l_boss_asi_invtr.invtr_legacy_prod_id is null then '-998'
 else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_boss_asi_invtr.invtr_legacy_prod_id as int) as varchar(500)),'z#@$k%&P'))),2) end dim_mms_product_key,
       case when s_boss_asi_invtr.invtr_display='Y' then 'Y'
	else 'N'
end display_flag,
       l_boss_asi_invtr.invtr_category_id invtr_category_id,
       l_boss_asi_invtr.invtr_id invtr_id,
       case when p_boss_asi_invtr.bk_hash in('-997', '-998', '-999') then p_boss_asi_invtr.bk_hash
    when s_boss_asi_invtr.invtr_last_sold is null then '-998'
	else convert(varchar, s_boss_asi_invtr.invtr_last_sold, 112) 
end last_sold_dim_date_key,
       isnull(s_boss_asi_invtr.invtr_desc,'') product_description,
       l_boss_asi_invtr.invtr_size size,
       isnull(s_boss_asi_invtr.invtr_sku,'') sku,
       isnull(s_boss_asi_invtr.invtr_style,'') style,
       isnull(h_boss_asi_invtr.dv_deleted,0) dv_deleted,
       p_boss_asi_invtr.p_boss_asi_invtr_id,
       p_boss_asi_invtr.dv_batch_id,
       p_boss_asi_invtr.dv_load_date_time,
       p_boss_asi_invtr.dv_load_end_date_time
  from dbo.h_boss_asi_invtr
  join dbo.p_boss_asi_invtr
    on h_boss_asi_invtr.bk_hash = p_boss_asi_invtr.bk_hash
  join #p_boss_asi_invtr_insert
    on p_boss_asi_invtr.bk_hash = #p_boss_asi_invtr_insert.bk_hash
   and p_boss_asi_invtr.p_boss_asi_invtr_id = #p_boss_asi_invtr_insert.p_boss_asi_invtr_id
  join dbo.l_boss_asi_invtr
    on p_boss_asi_invtr.bk_hash = l_boss_asi_invtr.bk_hash
   and p_boss_asi_invtr.l_boss_asi_invtr_id = l_boss_asi_invtr.l_boss_asi_invtr_id
  join dbo.s_boss_asi_invtr
    on p_boss_asi_invtr.bk_hash = s_boss_asi_invtr.bk_hash
   and p_boss_asi_invtr.s_boss_asi_invtr_id = s_boss_asi_invtr.s_boss_asi_invtr_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_boss_asi_invtr
   where d_boss_asi_invtr.bk_hash in (select bk_hash from #p_boss_asi_invtr_insert)

  insert dbo.d_boss_asi_invtr(
             bk_hash,
             invtr_upc_code,
             color,
             created_dim_date_key,
             d_boss_asi_class_r_bk_hash,
             d_boss_asi_color_r_bk_hash,
             d_boss_asi_dept_m_bk_hash,
             d_boss_asi_size_r_bk_hash,
             dim_boss_product_key,
             dim_mms_product_key,
             display_flag,
             invtr_category_id,
             invtr_id,
             last_sold_dim_date_key,
             product_description,
             size,
             sku,
             style,
             deleted_flag,
             p_boss_asi_invtr_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         invtr_upc_code,
         color,
         created_dim_date_key,
         d_boss_asi_class_r_bk_hash,
         d_boss_asi_color_r_bk_hash,
         d_boss_asi_dept_m_bk_hash,
         d_boss_asi_size_r_bk_hash,
         dim_boss_product_key,
         dim_mms_product_key,
         display_flag,
         invtr_category_id,
         invtr_id,
         last_sold_dim_date_key,
         product_description,
         size,
         sku,
         style,
         dv_deleted,
         p_boss_asi_invtr_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_boss_asi_invtr)
--Done!
end
