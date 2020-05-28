CREATE PROC [dbo].[proc_d_ig_it_cfg_discoup_master] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_ig_it_cfg_discoup_master)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_ig_it_cfg_discoup_master_insert') is not null drop table #p_ig_it_cfg_discoup_master_insert
create table dbo.#p_ig_it_cfg_discoup_master_insert with(distribution=hash(bk_hash), location=user_db) as
select p_ig_it_cfg_discoup_master.p_ig_it_cfg_discoup_master_id,
       p_ig_it_cfg_discoup_master.bk_hash
  from dbo.p_ig_it_cfg_discoup_master
 where p_ig_it_cfg_discoup_master.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_ig_it_cfg_discoup_master.dv_batch_id > @max_dv_batch_id
        or p_ig_it_cfg_discoup_master.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_ig_it_cfg_discoup_master.bk_hash,
       p_ig_it_cfg_discoup_master.ent_id ent_id,
       p_ig_it_cfg_discoup_master.discoup_id discoup_id,
       s_ig_it_cfg_discoup_master.discoup_amt amount,
       case when l_ig_it_cfg_discoup_master.discoup_pct_amt_code_id = 1 then 'N'
 when l_ig_it_cfg_discoup_master.discoup_pct_amt_code_id = 2 then 'Y'
else '' end amount_discount_flag ,
       s_ig_it_cfg_discoup_master.discoup_max_amt amount_maximum,
       case when p_ig_it_cfg_discoup_master.bk_hash in ('-997', '-998', '-999') then p_ig_it_cfg_discoup_master.bk_hash
        when l_ig_it_cfg_discoup_master.discoup_id is null then '-998'
       else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_ig_it_cfg_discoup_master.discoup_id as varchar(500)),'z#@$k%&P'))),2) end dim_cafe_discount_coupon_key,
       s_ig_it_cfg_discoup_master.discoup_abbr1 discount_coupon_abbreviation_1,
       s_ig_it_cfg_discoup_master.discoup_abbr2 discount_coupon_abbreviation_2,
       s_ig_it_cfg_discoup_master.discoup_name discount_coupon_name,
       case when l_ig_it_cfg_discoup_master.discoup_type_code_id= 1 then 'Coupon'
when l_ig_it_cfg_discoup_master.discoup_type_code_id= 2 then 'Discount'
else '' end discount_coupon_type,
       s_ig_it_cfg_discoup_master.discoup_percent discount_percent,
       s_ig_it_cfg_discoup_master.discoup_max_percent discount_percent_maximum,
       case when l_ig_it_cfg_discoup_master.discoup_pct_amt_code_id = 1 then 'Y'
 when l_ig_it_cfg_discoup_master.discoup_pct_amt_code_id = 2 then 'N'
else '' end percent_discount_flag ,
       h_ig_it_cfg_discoup_master.dv_deleted,
       p_ig_it_cfg_discoup_master.p_ig_it_cfg_discoup_master_id,
       p_ig_it_cfg_discoup_master.dv_batch_id,
       p_ig_it_cfg_discoup_master.dv_load_date_time,
       p_ig_it_cfg_discoup_master.dv_load_end_date_time
  from dbo.h_ig_it_cfg_discoup_master
  join dbo.p_ig_it_cfg_discoup_master
    on h_ig_it_cfg_discoup_master.bk_hash = p_ig_it_cfg_discoup_master.bk_hash  join #p_ig_it_cfg_discoup_master_insert
    on p_ig_it_cfg_discoup_master.bk_hash = #p_ig_it_cfg_discoup_master_insert.bk_hash
   and p_ig_it_cfg_discoup_master.p_ig_it_cfg_discoup_master_id = #p_ig_it_cfg_discoup_master_insert.p_ig_it_cfg_discoup_master_id
  join dbo.l_ig_it_cfg_discoup_master
    on p_ig_it_cfg_discoup_master.bk_hash = l_ig_it_cfg_discoup_master.bk_hash
   and p_ig_it_cfg_discoup_master.l_ig_it_cfg_discoup_master_id = l_ig_it_cfg_discoup_master.l_ig_it_cfg_discoup_master_id
  join dbo.s_ig_it_cfg_discoup_master
    on p_ig_it_cfg_discoup_master.bk_hash = s_ig_it_cfg_discoup_master.bk_hash
   and p_ig_it_cfg_discoup_master.s_ig_it_cfg_discoup_master_id = s_ig_it_cfg_discoup_master.s_ig_it_cfg_discoup_master_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_ig_it_cfg_discoup_master
   where d_ig_it_cfg_discoup_master.bk_hash in (select bk_hash from #p_ig_it_cfg_discoup_master_insert)

  insert dbo.d_ig_it_cfg_discoup_master(
             bk_hash,
             ent_id,
             discoup_id,
             amount,
             amount_discount_flag ,
             amount_maximum,
             dim_cafe_discount_coupon_key,
             discount_coupon_abbreviation_1,
             discount_coupon_abbreviation_2,
             discount_coupon_name,
             discount_coupon_type,
             discount_percent,
             discount_percent_maximum,
             percent_discount_flag ,
             deleted_flag,
             p_ig_it_cfg_discoup_master_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         ent_id,
         discoup_id,
         amount,
         amount_discount_flag ,
         amount_maximum,
         dim_cafe_discount_coupon_key,
         discount_coupon_abbreviation_1,
         discount_coupon_abbreviation_2,
         discount_coupon_name,
         discount_coupon_type,
         discount_percent,
         discount_percent_maximum,
         percent_discount_flag ,
         dv_deleted,
         p_ig_it_cfg_discoup_master_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_ig_it_cfg_discoup_master)
--Done!
end
