CREATE PROC [dbo].[proc_dim_spabiz_product] @dv_batch_id [varchar](500) AS
Begin
set xact_abort on
set nocount on

-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
if object_id('tempdb..#dv_batch_id') is not null drop table #dv_batch_id
create table dbo.#dv_batch_id with(distribution=round_robin, location=user_db, heap) as
select isnull(max(dv_batch_id),-2) max_dv_batch_id,
    @dv_batch_id as current_dv_batch_id
    from dbo.dim_spabiz_product

--~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~STEP - 1~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~	
--For a dimension record, the complete record needs to be rebuilt for a change in any field in any of the participating tables, Hence:
-----STEP 1: Collecting Business Keys from the base table - that are corresponding to the changed Recs from all the participating tables & itself
--~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if object_id('tempdb..#Business_keys') is not null drop table #Business_keys
create table dbo.#Business_keys with(distribution=hash(dim_spabiz_product_key), location=user_db, heap) as
select dim_spabiz_product_key
from (select p_spabiz_product.bk_hash dim_spabiz_product_key 
        from p_spabiz_product
        join #dv_batch_id 
		  on (p_spabiz_product.dv_batch_id > #dv_batch_id.max_dv_batch_id
		      or p_spabiz_product.dv_batch_id = #dv_batch_id.current_dv_batch_id)
        join l_spabiz_product 
          on p_spabiz_product.l_spabiz_product_id = l_spabiz_product.l_spabiz_product_id
	     and isnull(l_spabiz_product.store_number,999999999) not in (1,100,999)
	   where p_spabiz_product.dv_load_end_date_time = 'Dec 31, 9999') Business_keys_Unioned
--~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~END OF STEP 1: BUSINESS KEY COLLECTION~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~




--~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~STEP - 2:~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
---STEP 2: Preparing the required fields to build the dimension table from the individual participating tables--------
---i.e. Business keys collected in "STEP 1" drives collection of records from each participating table!
--~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ 	  
	  
if object_id('tempdb..#spabiz_product') is not null drop table #spabiz_product
create table dbo.#spabiz_product with(distribution=hash(dim_spabiz_product_key), location=user_db, heap) as
 select 
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
               --util_bk_hash[l_spabiz_product.dept_cat,h_spabiz_category.category_id,l_spabiz_product.store_number,h_spabiz_category.store_number]
               else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_product.dept_cat as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_spabiz_product.store_number as varchar(500)),'z#@$k%&P'))),2)     
		   end dim_spabiz_category_key,
		   
          case when p_spabiz_product.bk_hash in ('-997','-998','-999') then p_spabiz_product.bk_hash
               when l_spabiz_product.man_id is null then '-998'
               when l_spabiz_product.man_id = 0 then '-998'
               --util_bk_hash[l_spabiz_product.man_id,h_spabiz_manufacturer.manufacturer_id,l_spabiz_product.store_number,h_spabiz_manufacturer.store_number]
               else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_product.man_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_spabiz_product.store_number as varchar(500)),'z#@$k%&P'))),2)     
		   end dim_spabiz_manufacturer_key,
		   
          case when p_spabiz_product.bk_hash in ('-997','-998','-999') then p_spabiz_product.bk_hash
               when l_spabiz_product.default_staff_id is null then '-998'
               when l_spabiz_product.default_staff_id = 0 then '-998'
               --util_bk_hash[l_spabiz_product.default_staff_id,h_spabiz_staff.staff_id]
               else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast( l_spabiz_product.default_staff_id as varchar(500)),'z#@$k%&P'))),2)
           end dim_spabiz_staff_key,
          case when p_spabiz_product.bk_hash in ('-997','-998','-999') then p_spabiz_product.bk_hash
               when l_spabiz_product.store_number is null then '-998'
               when l_spabiz_product.store_number = 0 then '-998'
               --util_bk_hash[l_spabiz_product.store_number,h_spabiz_store.store_number]
               else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast( l_spabiz_product.store_number as varchar(500)),'z#@$k%&P'))),2)
           end dim_spabiz_store_key,
          case when p_spabiz_product.bk_hash in ('-997','-998','-999') then p_spabiz_product.bk_hash
               when l_spabiz_product.search_cat is null then '-998'
               when l_spabiz_product.search_cat = 0 then '-998'
               --util_bk_hash[l_spabiz_product.search_cat,h_spabiz_category.category_id,l_spabiz_product.store_number,h_spabiz_category.store_number]
               else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_product.search_cat as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_spabiz_product.store_number as varchar(500)),'z#@$k%&P'))),2)     
          end dim_spabiz_sub_category_key,
          case when p_spabiz_product.bk_hash in ('-997','-998','-999') then p_spabiz_product.bk_hash
               when l_spabiz_product.vendor_id is null then '-998'
               when l_spabiz_product.vendor_id = 0 then '-998'
               -- util_bk_hash[l_spabiz_product.vendor_id,h_spabiz_vendor.vendor_id,l_spabiz_product.store_number,h_spabiz_vendor.store_number]
                else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_product.vendor_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_spabiz_product.store_number as varchar(500)),'z#@$k%&P'))),2)     
		   end dim_spabiz_vendor_key,
          s_spabiz_product.edit_time edit_date_time,
          case when s_spabiz_product.eoq is null then 0 
               else s_spabiz_product.eoq
           end economic_order_quantity,
          case when charindex(' ',l_spabiz_product.gl_account) > 0 then substring(l_spabiz_product.gl_account,0,charindex(' ',l_spabiz_product.gl_account)) 
		       else l_spabiz_product.gl_account 
		  end gl_account,
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
          -- product_level_flag
          --insert #d_etl_map select 'd_dim_spabiz_product', 'product_level_flag', 'char(1)',
          --'case when p_spabiz_product.bk_hash in ('-997','-998','-999') then 'N'
          --     when s_spabiz_product.product_level = 1 then 'Y'
          --     else 'N'
          -- end'
          --, 'DW_2017.03.22'
           case when s_spabiz_product.name is null then ''
               else s_spabiz_product.name
           end product_name,
--          case when s_spabiz_product.status = 0 then 'Active'
--               else 'Discontinued'
--           end product_status,
--          case when s_spabiz_product.type = 0 then 'Retail'
--                when s_spabiz_product.type = 1 then 'Professional'
--                when s_spabiz_product.type = 2 then 'Promotional'
--                when s_spabiz_product.type = 3 then 'Special Use'
--               else ''
--           end product_type,
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
--        case when p_spabiz_product.bk_hash in ('-997','-998','-999') then p_spabiz_product.bk_hash
--             when s_spabiz_product.status is null then '-998'
--	       else 's_spabiz_product.status_' + convert(varchar,convert(int,s_spabiz_product.status)) 
--        end product_status_dim_description_key,
--        case when p_spabiz_product.bk_hash in ('-997','-998','-999') then p_spabiz_product.bk_hash
--             when s_spabiz_product.status is null then '-998'
--             --util_bk_hash[s_spabiz_product.status,h_spabiz_store.store_number]
--             else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast( s_spabiz_product.status as varchar(500)),'z#@$k%&P'))),2)
--	       end spabiz_product_status_key,		  
          case when p_spabiz_product.bk_hash in ('-997','-998','-999') then p_spabiz_product.bk_hash
               when s_spabiz_product.type is null then '-998'
		       else 's_spabiz_product.type_' + convert(varchar,convert(int,s_spabiz_product.type)) 
		   end product_type_dim_description_key,
		   
          case when p_spabiz_product.bk_hash in ('-997','-998','-999') then null
               else convert(int,s_spabiz_product.type)
		   end product_type_id,
		s_spabiz_product.quick_id quick_id,
          p_spabiz_product.p_spabiz_product_id,
          p_spabiz_product.dv_load_end_date_time,
          p_spabiz_product.dv_batch_id,
          p_spabiz_product.dv_load_date_time          
     from #Business_keys
     join p_spabiz_product 
       on p_spabiz_product.bk_hash = #Business_keys.dim_spabiz_product_key
     join l_spabiz_product 
       on p_spabiz_product.l_spabiz_product_id = l_spabiz_product.l_spabiz_product_id
     join s_spabiz_product 
       on p_spabiz_product.s_spabiz_product_id = s_spabiz_product.s_spabiz_product_id
      and p_spabiz_product.dv_load_end_date_time = 'Dec 31, 9999'


 if object_id('tempdb..#sandbox_product_mapping') is not null drop table #sandbox_product_mapping
create table dbo.#sandbox_product_mapping with(distribution=hash(dim_spabiz_product_key), location=user_db, heap) as
 select 
     p_spabiz_product.bk_hash dim_spabiz_product_key,
     s_sandbox_product_mapping.category,
     s_sandbox_product_mapping.segment,
     s_sandbox_product_mapping.back_bar,
     s_sandbox_product_mapping.commission_mapping,
     p_spabiz_product.dv_load_end_date_time,
     p_spabiz_product.dv_batch_id,
     p_spabiz_product.dv_load_date_time    		
     from #Business_keys
     join p_spabiz_product 
       on p_spabiz_product.bk_hash = #Business_keys.dim_spabiz_product_key
     join l_spabiz_product 
       on p_spabiz_product.l_spabiz_product_id = l_spabiz_product.l_spabiz_product_id
     join s_spabiz_product 
       on p_spabiz_product.s_spabiz_product_id = s_spabiz_product.s_spabiz_product_id
      and p_spabiz_product.dv_load_end_date_time = 'Dec 31, 9999'
		   
     join l_sandbox_product_mapping
       on p_spabiz_product.product_id = l_sandbox_product_mapping.product_id
      and p_spabiz_product.store_number = l_sandbox_product_mapping.store_number   
     join p_sandbox_product_mapping
       on l_sandbox_product_mapping.l_sandbox_product_mapping_id = p_sandbox_product_mapping.l_sandbox_product_mapping_id
     join s_sandbox_product_mapping
       on p_sandbox_product_mapping.s_sandbox_product_mapping_id = s_sandbox_product_mapping.s_sandbox_product_mapping_id	  
      and p_sandbox_product_mapping.dv_load_end_date_time = 'Dec 31, 9999'
 	  
 

 --~~~~~~~~~~~~~~~END OF STEP 2: Requried Fields from different participating fields have been created as #TEMP tables~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


	 

--~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~STEP - 3:~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
----------------STEP 3: INSERT INTO DIM TABLE: By Joining the temp STEP 2's #temp tables, forming the main Dim table record-----------
--~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
--delete and re-insert
--do as a single transaction
--delete records from the fact table that exist
--insert records from records from current and missing batches
    begin tran
    delete dbo.dim_spabiz_product
    where dim_spabiz_product_key in (select dim_spabiz_product_key from dbo.#spabiz_product) 
	

	
	
 insert into dbo.dim_spabiz_product(dim_spabiz_product_key,
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
                                        edit_date_time,
                                        economic_order_quantity,
                                        label_name,
								gl_account,
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
--                                      product_status,
--                                      product_type,
                                        retail_price,
                                        taxable_flag,
                                        vendor_code,
--                                      product_status_dim_description_key,
--                                      spabiz_product_status_key,
                                        product_type_dim_description_key,
                                        product_type_id,
								quick_id,
                                        category,
                                        segment,
                                        back_bar,
                                        commission_mapping,
                                        p_spabiz_product_id,
                                        dv_batch_id,
                                        dv_load_date_time,
                                        dv_load_end_date_time,
                                        dv_inserted_date_time,
                                        dv_insert_user)
                                        
                        
                                 select #spabiz_product.dim_spabiz_product_key,
                                        #spabiz_product.product_id,
                                        #spabiz_product.store_number,
                                        #spabiz_product.avg_cost,
                                        #spabiz_product.cost,
                                        #spabiz_product.cost2,
                                        #spabiz_product.cost2_quantity,
                                        #spabiz_product.created_date_time,
                                        #spabiz_product.current_quantity,
                                        #spabiz_product.deleted_date_time,
                                        #spabiz_product.deleted_flag,
                                        #spabiz_product.dim_spabiz_category_key,
                                        #spabiz_product.dim_spabiz_manufacturer_key,
                                        #spabiz_product.dim_spabiz_staff_key,
                                        #spabiz_product.dim_spabiz_store_key,
                                        #spabiz_product.dim_spabiz_sub_category_key,
                                        #spabiz_product.dim_spabiz_vendor_key,
                                        #spabiz_product.edit_date_time,
                                        #spabiz_product.economic_order_quantity,
                                        #spabiz_product.label_name,
								#spabiz_product.gl_account,
                                        #spabiz_product.last_count_date_time,
                                        #spabiz_product.last_purchased_date_time,
                                        #spabiz_product.last_sold_date_time,
                                        #spabiz_product.location,
                                        #spabiz_product.manufacturer_code,
                                        #spabiz_product.maximum_inventory_count,
                                        #spabiz_product.minimum_inventory_count,
                                        #spabiz_product.on_order,
                                        #spabiz_product.print_label_flag,
                                        #spabiz_product.print_on_ticket,
                                        #spabiz_product.product_name,
--                                      #spabiz_product.product_status,
--                                      #spabiz_product.product_type,
                                        #spabiz_product.retail_price,
                                        #spabiz_product.taxable_flag,
                                        #spabiz_product.vendor_code,
--                                      #spabiz_product.product_status_dim_description_key,
--                                      #spabiz_product.spabiz_product_status_key,
                                        #spabiz_product.product_type_dim_description_key,
                                        #spabiz_product.product_type_id,
								#spabiz_product.quick_id,
                                        #sandbox_product_mapping.category,
                                        #sandbox_product_mapping.segment,
                                        #sandbox_product_mapping.back_bar,
                                        #sandbox_product_mapping.commission_mapping,
                                        #spabiz_product.p_spabiz_product_id,
                                        #spabiz_product.dv_batch_id,
                                        #spabiz_product.dv_load_date_time,
                                        #spabiz_product.dv_load_end_date_time,
									    getdate(),
                                        suser_sname()	
									from #spabiz_product 
                               left join #sandbox_product_mapping
                                      on #spabiz_product.dim_spabiz_product_key=#sandbox_product_mapping.dim_spabiz_product_key
   
   	 commit tran
---------------------------------------END OF STEP 3: END OF DIM INSERTS--------------------------------------
end
