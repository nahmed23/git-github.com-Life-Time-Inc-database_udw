CREATE PROC [dbo].[proc_d_spabiz_product] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_spabiz_product)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_spabiz_product_insert') is not null drop table #p_spabiz_product_insert
create table dbo.#p_spabiz_product_insert with(distribution=hash(bk_hash), location=user_db) as
select p_spabiz_product.p_spabiz_product_id,
       p_spabiz_product.bk_hash
  from dbo.p_spabiz_product
 where p_spabiz_product.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_spabiz_product.dv_batch_id > @max_dv_batch_id
        or p_spabiz_product.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_spabiz_product.bk_hash,
       p_spabiz_product.bk_hash dim_spabiz_product_key,
       p_spabiz_product.product_id product_id,
       p_spabiz_product.store_number store_number,
       case when s_spabiz_product.avg_cost is null then 0 
            else s_spabiz_product.avg_cost
        end avg_cost,
       case when s_spabiz_product.cost is null then 0 
            else s_spabiz_product.cost
        end cost,
       case when s_spabiz_product.cost2 is null then 0 
            else s_spabiz_product.cost2
        end cost2,
       case when s_spabiz_product.cost2_qty is null then 0 
            else s_spabiz_product.cost2_qty
        end cost2_quantity,
       case when p_spabiz_product.bk_hash in ('-997','-998','-999') then null
            else s_spabiz_product.date_created
        end created_date_time,
       case when s_spabiz_product.current_qty is null then 0 
            else s_spabiz_product.current_qty
        end current_quantity,
       case when p_spabiz_product.bk_hash in ('-997','-998','-999') then null
            when s_spabiz_product.delete_date = convert(date, '18991230', 112) then null
            else s_spabiz_product.delete_date
        end deleted_date_time,
       case when p_spabiz_product.bk_hash in ('-997','-998','-999') then 'N'
            when s_spabiz_product.product_delete = -1 then 'Y'
            else 'N'
        end deleted_flag,
       case when p_spabiz_product.bk_hash in ('-997','-998','-999') then p_spabiz_product.bk_hash
            when l_spabiz_product.dept_cat is null then '-998'
            when l_spabiz_product.dept_cat = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_product.dept_cat as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_spabiz_product.store_number as varchar(500)),'z#@$k%&P'))),2)
        end dim_spabiz_category_key,
       case when p_spabiz_product.bk_hash in ('-997','-998','-999') then p_spabiz_product.bk_hash
            when l_spabiz_product.man_id is null then '-998'
            when l_spabiz_product.man_id = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_product.man_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_spabiz_product.store_number as varchar(500)),'z#@$k%&P'))),2)
        end dim_spabiz_manufacturer_key,
       case when p_spabiz_product.bk_hash in ('-997','-998','-999') then p_spabiz_product.bk_hash
            when l_spabiz_product.default_staff_id is null then '-998'
            when l_spabiz_product.default_staff_id = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_product.default_staff_id as varchar(500)),'z#@$k%&P'))),2)
        end dim_spabiz_staff_key,
       case when p_spabiz_product.bk_hash in ('-997','-998','-999') then p_spabiz_product.bk_hash
            when l_spabiz_product.store_number is null then '-998'
            when l_spabiz_product.store_number = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_product.store_number as varchar(500)),'z#@$k%&P'))),2)
        end dim_spabiz_store_key,
       case when p_spabiz_product.bk_hash in ('-997','-998','-999') then p_spabiz_product.bk_hash
            when l_spabiz_product.search_cat is null then '-998'
            when l_spabiz_product.search_cat = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_product.search_cat as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_spabiz_product.store_number as varchar(500)),'z#@$k%&P'))),2)
        end dim_spabiz_sub_category_key,
       case when p_spabiz_product.bk_hash in ('-997','-998','-999') then p_spabiz_product.bk_hash
            when l_spabiz_product.vendor_id is null then '-998'
            when l_spabiz_product.vendor_id = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_product.vendor_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_spabiz_product.store_number as varchar(500)),'z#@$k%&P'))),2)
        end dim_spabiz_vendor_key,
       case when s_spabiz_product.eoq is null then 0 
            else s_spabiz_product.eoq
        end economic_order_quantity,
       s_spabiz_product.edit_time edit_date_time,
       case when charindex(' ',l_spabiz_product.gl_account) > 0 then substring(l_spabiz_product.gl_account,0,charindex(' ',l_spabiz_product.gl_account)) else l_spabiz_product.gl_account end gl_account,
       case when s_spabiz_product.label_name is null then ''
            else s_spabiz_product.label_name
        end label_name,
       case when s_spabiz_product.last_count = convert(date, '18991230', 112) then null
            else s_spabiz_product.last_count
        end last_count_date_time,
       case when s_spabiz_product.last_purchase = convert(date, '18991230', 112) then null
            else s_spabiz_product.last_purchase
        end last_purchased_date_time,
       case when s_spabiz_product.last_sold = convert(date, '18991230', 112) then null
            else s_spabiz_product.last_sold
        end last_sold_date_time,
       case when s_spabiz_product.location is null then ''
            else s_spabiz_product.location
        end location,
       case when s_spabiz_product.man_code is null then ''
            else s_spabiz_product.man_code
        end manufacturer_code,
       case when s_spabiz_product.max is null then 0
            else s_spabiz_product.max
        end maximum_inventory_count,
       case when s_spabiz_product.min is null then 0
            else s_spabiz_product.min
        end minimum_inventory_count,
       case when s_spabiz_product.on_order is null then 0
            else s_spabiz_product.on_order
        end on_order,
       case when p_spabiz_product.bk_hash in ('-997','-998','-999') then 'N'
            when s_spabiz_product.labels = 1 then 'Y'
            else 'N'
        end print_label_flag,
       case when s_spabiz_product.print_on_ticket is null then ''
             else s_spabiz_product.print_on_ticket 
        end print_on_ticket,
       case when s_spabiz_product.name is null then ''
            else s_spabiz_product.name
        end product_name,
       's_spabiz_product.type_' + convert(varchar,convert(int,s_spabiz_product.type)) product_type_dim_description_key,
       convert(int,s_spabiz_product.type) product_type_id,
       s_spabiz_product.quick_id quick_id,
       case when s_spabiz_product.retail_price is null then 0
            else s_spabiz_product.retail_price
        end retail_price,
       case when p_spabiz_product.bk_hash in ('-997','-998','-999') then 'N'
            when s_spabiz_product.taxable = 1 then 'Y'
            else 'N'
        end taxable_flag,
       case when s_spabiz_product.vendor_code is null then ''
            else s_spabiz_product.vendor_code
        end vendor_code,
       p_spabiz_product.p_spabiz_product_id,
       p_spabiz_product.dv_batch_id,
       p_spabiz_product.dv_load_date_time,
       p_spabiz_product.dv_load_end_date_time
  from dbo.p_spabiz_product
  join #p_spabiz_product_insert
    on p_spabiz_product.bk_hash = #p_spabiz_product_insert.bk_hash
   and p_spabiz_product.p_spabiz_product_id = #p_spabiz_product_insert.p_spabiz_product_id
  join dbo.l_spabiz_product
    on p_spabiz_product.bk_hash = l_spabiz_product.bk_hash
   and p_spabiz_product.l_spabiz_product_id = l_spabiz_product.l_spabiz_product_id
  join dbo.s_spabiz_product
    on p_spabiz_product.bk_hash = s_spabiz_product.bk_hash
   and p_spabiz_product.s_spabiz_product_id = s_spabiz_product.s_spabiz_product_id
 where l_spabiz_product.store_number not in (1,100,999) OR p_spabiz_product.bk_hash in ('-999','-998','-997')

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_spabiz_product
   where d_spabiz_product.bk_hash in (select bk_hash from #p_spabiz_product_insert)

  insert dbo.d_spabiz_product(
             bk_hash,
             dim_spabiz_product_key,
             product_id,
             store_number,
             avg_cost,
             cost,
             cost2,
             cost2_quantity,
             created_date_time,
             current_quantity,
             deleted_date_time,
             deleted_flag,
             dim_spabiz_category_key,
             dim_spabiz_manufacturer_key,
             dim_spabiz_staff_key,
             dim_spabiz_store_key,
             dim_spabiz_sub_category_key,
             dim_spabiz_vendor_key,
             economic_order_quantity,
             edit_date_time,
             gl_account,
             label_name,
             last_count_date_time,
             last_purchased_date_time,
             last_sold_date_time,
             location,
             manufacturer_code,
             maximum_inventory_count,
             minimum_inventory_count,
             on_order,
             print_label_flag,
             print_on_ticket,
             product_name,
             product_type_dim_description_key,
             product_type_id,
             quick_id,
             retail_price,
             taxable_flag,
             vendor_code,
             p_spabiz_product_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         dim_spabiz_product_key,
         product_id,
         store_number,
         avg_cost,
         cost,
         cost2,
         cost2_quantity,
         created_date_time,
         current_quantity,
         deleted_date_time,
         deleted_flag,
         dim_spabiz_category_key,
         dim_spabiz_manufacturer_key,
         dim_spabiz_staff_key,
         dim_spabiz_store_key,
         dim_spabiz_sub_category_key,
         dim_spabiz_vendor_key,
         economic_order_quantity,
         edit_date_time,
         gl_account,
         label_name,
         last_count_date_time,
         last_purchased_date_time,
         last_sold_date_time,
         location,
         manufacturer_code,
         maximum_inventory_count,
         minimum_inventory_count,
         on_order,
         print_label_flag,
         print_on_ticket,
         product_name,
         product_type_dim_description_key,
         product_type_id,
         quick_id,
         retail_price,
         taxable_flag,
         vendor_code,
         p_spabiz_product_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_spabiz_product)
--Done!
end
