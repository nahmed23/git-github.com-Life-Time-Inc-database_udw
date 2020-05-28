CREATE PROC [dbo].[proc_d_spabiz_discount] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_spabiz_discount)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_spabiz_discount_insert') is not null drop table #p_spabiz_discount_insert
create table dbo.#p_spabiz_discount_insert with(distribution=hash(bk_hash), location=user_db) as
select p_spabiz_discount.p_spabiz_discount_id,
       p_spabiz_discount.bk_hash
  from dbo.p_spabiz_discount
 where p_spabiz_discount.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_spabiz_discount.dv_batch_id > @max_dv_batch_id
        or p_spabiz_discount.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_spabiz_discount.bk_hash,
       p_spabiz_discount.bk_hash dim_spabiz_discount_key,
       p_spabiz_discount.discount_id discount_id,
       p_spabiz_discount.store_number store_number,
       case when s_spabiz_discount.pay_retail_comish = 1 or s_spabiz_discount.pay_comish = 1 then 'Y' else 'N' end all_retail_commission_flag,
       case when s_spabiz_discount.pay_service_comish = 1 or s_spabiz_discount.pay_comish = 1 then 'Y' else 'N' end all_service_commission_flag,
       s_spabiz_discount.amount amount,
       's_spabiz_discount.apply_to_' + convert(varchar,convert(int,s_spabiz_discount.apply_to)) apply_to_dim_description_key,
       convert(int,s_spabiz_discount.apply_to) apply_to_id,
       's_spabiz_discount.apply_when_' + convert(varchar,convert(int,s_spabiz_discount.apply_when)) apply_when_dim_description_key,
       convert(int,s_spabiz_discount.apply_when) apply_when_id,
       case when p_spabiz_discount.bk_hash in ('-997','-998','-999') then 'N'
            when s_spabiz_discount.is_promo = 1 then 'Y'
            else 'N'
        end associated_with_promotion_flag,
       case when p_spabiz_discount.bk_hash in ('-997','-998','-999') then null
            when s_spabiz_discount.delete_date = convert(date, '18991230', 112) then null
            else s_spabiz_discount.delete_date
        end deleted_date_time,
       case when p_spabiz_discount.bk_hash in ('-997','-998','-999') then 'N'
            when s_spabiz_discount.discount_delete = -1 then 'Y'
            else 'N'
        end deleted_flag,
       case when p_spabiz_discount.bk_hash in ('-997','-998','-999') then p_spabiz_discount.bk_hash
            when l_spabiz_discount.store_number is null then '-998'
            when l_spabiz_discount.store_number = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_discount.store_number as varchar(500)),'z#@$k%&P'))),2)
        end dim_spabiz_store_key,
       'l_spabiz_discount.dept_cat' + convert(varchar,convert(int,l_spabiz_discount.dept_cat)) discount_category_dim_description_key,
       convert(int,l_spabiz_discount.dept_cat) discount_category_id,
       s_spabiz_discount.edit_time edit_date_time,
       case when p_spabiz_discount.bk_hash in ('-997','-998','-999') then null
            when s_spabiz_discount.from_date = convert(date, '18991230', 112) then null
            else s_spabiz_discount.from_date
        end from_date_time,
       case when s_spabiz_discount.name is null then ''
            else s_spabiz_discount.name
        end name,
       case when p_spabiz_discount.bk_hash in ('-997','-998','-999') then 'N'
            when s_spabiz_discount.pay_comish = 1 then 'Y'
            else 'N'
        end pay_commission_flag,
       case when p_spabiz_discount.bk_hash in ('-997','-998','-999') then 'N'
            when s_spabiz_discount.pay_retail_comish = 1 then 'Y'
            else 'N'
        end pay_retail_commission_flag,
       case when p_spabiz_discount.bk_hash in ('-997','-998','-999') then 'N'
            when s_spabiz_discount.pay_service_comish = 1 then 'Y'
            else 'N'
        end pay_service_commission_flag,
       's_spabiz_discount.percent_dollar_' + convert(varchar,convert(int,s_spabiz_discount.percent_dollar)) percent_dollar_dim_description_key,
       convert(int,s_spabiz_discount.percent_dollar) percent_dollar_id,
       case when s_spabiz_discount.quick_id is null then ''
            else s_spabiz_discount.quick_id
        end quick_id,
       case when p_spabiz_discount.bk_hash in ('-997','-998','-999') then null
            when s_spabiz_discount.to_date = convert(date, '18991230', 112) then null
            else s_spabiz_discount.to_date
        end to_date_time,
       case when p_spabiz_discount.bk_hash in ('-997','-998','-999') then 'N'
            when s_spabiz_discount.use_date_range = 1 then 'Y'
            else 'N'
        end use_date_range_flag,
       p_spabiz_discount.p_spabiz_discount_id,
       p_spabiz_discount.dv_batch_id,
       p_spabiz_discount.dv_load_date_time,
       p_spabiz_discount.dv_load_end_date_time
  from dbo.p_spabiz_discount
  join #p_spabiz_discount_insert
    on p_spabiz_discount.bk_hash = #p_spabiz_discount_insert.bk_hash
   and p_spabiz_discount.p_spabiz_discount_id = #p_spabiz_discount_insert.p_spabiz_discount_id
  join dbo.l_spabiz_discount
    on p_spabiz_discount.bk_hash = l_spabiz_discount.bk_hash
   and p_spabiz_discount.l_spabiz_discount_id = l_spabiz_discount.l_spabiz_discount_id
  join dbo.s_spabiz_discount
    on p_spabiz_discount.bk_hash = s_spabiz_discount.bk_hash
   and p_spabiz_discount.s_spabiz_discount_id = s_spabiz_discount.s_spabiz_discount_id
 where l_spabiz_discount.store_number not in (1,100,999) OR p_spabiz_discount.bk_hash in ('-999','-998','-997')

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_spabiz_discount
   where d_spabiz_discount.bk_hash in (select bk_hash from #p_spabiz_discount_insert)

  insert dbo.d_spabiz_discount(
             bk_hash,
             dim_spabiz_discount_key,
             discount_id,
             store_number,
             all_retail_commission_flag,
             all_service_commission_flag,
             amount,
             apply_to_dim_description_key,
             apply_to_id,
             apply_when_dim_description_key,
             apply_when_id,
             associated_with_promotion_flag,
             deleted_date_time,
             deleted_flag,
             dim_spabiz_store_key,
             discount_category_dim_description_key,
             discount_category_id,
             edit_date_time,
             from_date_time,
             name,
             pay_commission_flag,
             pay_retail_commission_flag,
             pay_service_commission_flag,
             percent_dollar_dim_description_key,
             percent_dollar_id,
             quick_id,
             to_date_time,
             use_date_range_flag,
             p_spabiz_discount_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         dim_spabiz_discount_key,
         discount_id,
         store_number,
         all_retail_commission_flag,
         all_service_commission_flag,
         amount,
         apply_to_dim_description_key,
         apply_to_id,
         apply_when_dim_description_key,
         apply_when_id,
         associated_with_promotion_flag,
         deleted_date_time,
         deleted_flag,
         dim_spabiz_store_key,
         discount_category_dim_description_key,
         discount_category_id,
         edit_date_time,
         from_date_time,
         name,
         pay_commission_flag,
         pay_retail_commission_flag,
         pay_service_commission_flag,
         percent_dollar_dim_description_key,
         percent_dollar_id,
         quick_id,
         to_date_time,
         use_date_range_flag,
         p_spabiz_discount_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_spabiz_discount)
--Done!
end
