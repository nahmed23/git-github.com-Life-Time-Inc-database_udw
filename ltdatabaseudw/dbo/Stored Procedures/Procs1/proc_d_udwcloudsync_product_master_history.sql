CREATE PROC [dbo].[proc_d_udwcloudsync_product_master_history] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!

-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_udwcloudsync_product_master_history);

if object_id('tempdb..#p_udwcloudsync_product_master_id_list') is not null drop table #p_udwcloudsync_product_master_id_list
create table dbo.#p_udwcloudsync_product_master_id_list with(distribution=hash(bk_hash), location=user_db, heap) as
with undo_delete (p_udwcloudsync_product_master_id,bk_hash,dv_load_date_time) as 
(
-- Find any updates with the current batch ID to undo in case of retry - just like in the PIT Proc
--   Find the records in the current batch
--   Find the records related to the current batch
--   Note that this needs to be done using the PIT ids within the fact/dimension base table since a workflow retry would have deleted those IDs from the PIT table and reinserted with new IDs
    select p_udwcloudsync_product_master_id,
           bk_hash,
           dv_load_date_time
      from dbo.d_udwcloudsync_product_master_history
     where dv_batch_id = @current_dv_batch_id
),
undo_update (p_udwcloudsync_product_master_id,bk_hash) as
(
    select d_udwcloudsync_product_master_history.p_udwcloudsync_product_master_id,
           d_udwcloudsync_product_master_history.bk_hash
      from dbo.d_udwcloudsync_product_master_history
      join undo_delete
        on d_udwcloudsync_product_master_history.bk_hash = undo_delete.bk_hash
       and d_udwcloudsync_product_master_history.dv_load_end_date_time = undo_delete.dv_load_date_time
),
p_udwcloudsync_product_master_insert (p_udwcloudsync_product_master_id,bk_hash,dv_load_date_time) as 
(
-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those have not been physically deleted yet - if they exist
-- Then find the PIT ids in the PIT table that correspond to the dimension/fact records to end-date
    select p_udwcloudsync_product_master_id,
           bk_hash,
           dv_load_date_time
      from dbo.p_udwcloudsync_product_master
     where dv_batch_id > @max_dv_batch_id
        or dv_batch_id = @current_dv_batch_id
),
p_udwcloudsync_product_master_update (p_udwcloudsync_product_master_id,bk_hash) as
(
    select p_udwcloudsync_product_master.p_udwcloudsync_product_master_id,
           p_udwcloudsync_product_master.bk_hash
      from dbo.p_udwcloudsync_product_master
      join p_udwcloudsync_product_master_insert
        on p_udwcloudsync_product_master.bk_hash = p_udwcloudsync_product_master_insert.bk_hash
       and p_udwcloudsync_product_master.dv_load_end_date_time = p_udwcloudsync_product_master_insert.dv_load_date_time
)
select undo_delete.p_udwcloudsync_product_master_id,
       bk_hash
  from undo_delete
union
select undo_update.p_udwcloudsync_product_master_id,
       bk_hash
  from undo_update
union
select p_udwcloudsync_product_master_insert.p_udwcloudsync_product_master_id,
       bk_hash
  from p_udwcloudsync_product_master_insert
union
select p_udwcloudsync_product_master_update.p_udwcloudsync_product_master_id,
       bk_hash
  from p_udwcloudsync_product_master_update

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db, heap) as
select #p_udwcloudsync_product_master_id_list.bk_hash,
       p_udwcloudsync_product_master.product_id product_id,
       p_udwcloudsync_product_master.product_sku product_sku,
       p_udwcloudsync_product_master.source_system_link_title source_system,
       isnull(p_udwcloudsync_product_master.dv_greatest_satellite_date_time, convert(datetime, '2000.01.01', 102)) effective_date_time,
       case when p_udwcloudsync_product_master.dv_load_end_date_time = convert(datetime, '9999.12.31', 102) then p_udwcloudsync_product_master.dv_load_end_date_time
             else p_udwcloudsync_product_master.dv_next_greatest_satellite_date_time
         end expiration_date_time,
       s_udwcloudsync_product_master.revenue_allocation_rule allocation_rule,
       s_udwcloudsync_product_master.connectivity_lead_generator connectivity_lead_generator_flag,
       s_udwcloudsync_product_master.connectivity_primary_lead_generator_flag connectivity_primary_lead_generator_flag,
       s_udwcloudsync_product_master.corporate_transfer_flag corporate_transfer_flag,
       s_udwcloudsync_product_master.corporate_transfer_multiplier corporate_transfer_multiplier,
       s_udwcloudsync_product_master.departmental_dssr_flag departmental_dssr_flag,
       case when p_udwcloudsync_product_master.bk_hash in ('-997','-998','-999') then p_udwcloudsync_product_master.bk_hash       when p_udwcloudsync_product_master.source_system_link_title = 'cafe' then convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_udwcloudsync_product_master.product_id as int) as varchar(500)),'z#@$k%&P'))),2)        else '-998'  end dim_cafe_product_key,
       case when p_udwcloudsync_product_master.bk_hash in ('-997','-998','-999') then p_udwcloudsync_product_master.bk_hash
        when p_udwcloudsync_product_master.source_system_link_title = 'HealthCheckUSA - HealthCheckUSA' then convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_udwcloudsync_product_master.product_sku as nvarchar(4000)),'z#@$k%&P'))),2) else '-998' end dim_healthcheckusa_product_key,
       case when p_udwcloudsync_product_master.bk_hash in ('-997','-998','-999') then p_udwcloudsync_product_master.bk_hash
       when p_udwcloudsync_product_master.source_system_link_title = 'E-Commerce - Hybris' then convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_udwcloudsync_product_master.product_sku as nvarchar(258)),'z#@$k%&P'))),2) else '-998' end dim_hybris_product_key,
       case when p_udwcloudsync_product_master.bk_hash in ('-997','-998','-999') then p_udwcloudsync_product_master.bk_hash  
            when p_udwcloudsync_product_master.source_system_link_title = 'Magento' then convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_udwcloudsync_product_master.product_id as int) as varchar(500)),'z#@$k%&P'))),2) else '-998' end dim_magento_product_key,
       case when p_udwcloudsync_product_master.bk_hash in ('-997','-998','-999') then p_udwcloudsync_product_master.bk_hash       when p_udwcloudsync_product_master.source_system_link_title = 'MMS' then convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_udwcloudsync_product_master.product_id as int) as varchar(500)),'z#@$k%&P'))),2)        else '-998'  end dim_mms_product_key,
       case when p_udwcloudsync_product_master.bk_hash in ('-997','-998','-999') then p_udwcloudsync_product_master.bk_hash
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(s_udwcloudsync_product_master.division as varchar(255)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(s_udwcloudsync_product_master.sub_division as varchar(255)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(s_udwcloudsync_product_master.reporting_dept_for_non_commissioned_sales as varchar(255)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(s_udwcloudsync_product_master.revenue_product_group_description as varchar(255)),'z#@$k%&P'))),2)
        end dim_reporting_hierarchy_key,
       s_udwcloudsync_product_master.dssr_down_grade_other_enrollment_fee_flag dssr_down_grade_other_enrollment_fee_flag,
       s_udwcloudsync_product_master.dssr_if_admin_fee_flag dssr_if_admin_fee_flag,
        experience_life_magazine_flag,
       s_udwcloudsync_product_master.mtd_average_delivered_session_price mtd_average_delivered_session_price_flag,
       s_udwcloudsync_product_master.mtd_average_sale_price mtd_average_sale_price_flag,
       s_udwcloudsync_product_master.new_business_old_business new_business_old_business_flag,
       s_udwcloudsync_product_master.payroll_extract_description payroll_description,
       s_udwcloudsync_product_master.payroll_my_ltbucks_product_group_description payroll_lt_bucks_group_description,
       s_udwcloudsync_product_master.payroll_my_ltbucks_product_group_sort_order payroll_lt_bucks_group_sort_order,
       case when s_udwcloudsync_product_master.payroll_my_ltbucks_product_group_description <> '' then 'Y'  else 'N' end payroll_lt_bucks_product_group_flag,
       s_udwcloudsync_product_master.payroll_my_ltbucks_sales_amount_flag payroll_lt_bucks_sales_amount_flag,
       s_udwcloudsync_product_master.payroll_my_ltbucks_service_amount_flag payroll_lt_bucks_service_amount_flag,
       s_udwcloudsync_product_master.payroll_my_ltbucks_service_quantity_flag payroll_lt_bucks_service_quantity_flag,
       s_udwcloudsync_product_master.payroll_extract_region_type payroll_region_type,
       s_udwcloudsync_product_master.payroll_product_group_description payroll_standard_group_description,
       s_udwcloudsync_product_master.payroll_product_group_sort_order payroll_standard_group_sort_order,
       case when s_udwcloudsync_product_master.payroll_product_group_description <> '' then 'Y' else 'N' end payroll_standard_product_group_flag,
       s_udwcloudsync_product_master.payroll_sales_amount_flag payroll_standard_sales_amount_flag,
       s_udwcloudsync_product_master.payroll_service_amount_flag payroll_standard_service_amount_flag,
       s_udwcloudsync_product_master.payroll_service_quantity_flag payroll_standard_service_quantity_flag,
       s_udwcloudsync_product_master.payroll_track_sales_flag payroll_track_sales_flag,
       s_udwcloudsync_product_master.payroll_track_service_flag payroll_track_service_flag,
       s_udwcloudsync_product_master.product_description product_description,
       s_udwcloudsync_product_master.product_status product_status,
       s_udwcloudsync_product_master.reporting_dept_for_non_commissioned_sales reporting_department,
       s_udwcloudsync_product_master.division reporting_division,
       s_udwcloudsync_product_master.revenue_product_group_description reporting_product_group,
       l_udwcloudsync_product_master.revenue_product_group_gl_account reporting_product_group_gl_account,
       s_udwcloudsync_product_master.revenue_product_group_sort_order reporting_product_group_sort_order,
       isnull(s_udwcloudsync_product_master.revenue_reporting_region_type,'') reporting_region_type,
       s_udwcloudsync_product_master.sub_division reporting_sub_division,
       l_udwcloudsync_product_master.revenue_product_group_discount_gl_account revenue_product_group_discount_gl_account,
       l_udwcloudsync_product_master.revenue_product_group_refund_gl_account revenue_product_group_refund_gl_account,
       s_udwcloudsync_product_master.sales_category_description sales_category_description,
       h_udwcloudsync_product_master.dv_deleted,
       p_udwcloudsync_product_master.p_udwcloudsync_product_master_id,
       p_udwcloudsync_product_master.dv_batch_id,
       p_udwcloudsync_product_master.dv_load_date_time,
       p_udwcloudsync_product_master.dv_load_end_date_time
  from dbo.h_udwcloudsync_product_master
  join dbo.p_udwcloudsync_product_master
    on h_udwcloudsync_product_master.bk_hash = p_udwcloudsync_product_master.bk_hash  join #p_udwcloudsync_product_master_id_list
    on p_udwcloudsync_product_master.p_udwcloudsync_product_master_id = #p_udwcloudsync_product_master_id_list.p_udwcloudsync_product_master_id
   and p_udwcloudsync_product_master.bk_hash = #p_udwcloudsync_product_master_id_list.bk_hash
  join dbo.l_udwcloudsync_product_master
    on p_udwcloudsync_product_master.bk_hash = l_udwcloudsync_product_master.bk_hash
   and p_udwcloudsync_product_master.l_udwcloudsync_product_master_id = l_udwcloudsync_product_master.l_udwcloudsync_product_master_id
  join dbo.s_udwcloudsync_product_master
    on p_udwcloudsync_product_master.bk_hash = s_udwcloudsync_product_master.bk_hash
   and p_udwcloudsync_product_master.s_udwcloudsync_product_master_id = s_udwcloudsync_product_master.s_udwcloudsync_product_master_id
 where h_udwcloudsync_product_master.dv_deleted = 0
   and isnull(p_udwcloudsync_product_master.dv_greatest_satellite_date_time, convert(datetime, '2000.01.01', 102))!= case when p_udwcloudsync_product_master.dv_load_end_date_time = convert(datetime, '9999.12.31', 102) then p_udwcloudsync_product_master.dv_load_end_date_time
      else p_udwcloudsync_product_master.dv_next_greatest_satellite_date_time
  end


-- do as a single transaction
--   delete records from dimension where PIT_id = #PIT.PIT_id
--     Note that this also gets rid of any records where the existing effective_date_time equals the soon to be newly calculated expiration_date_time
--   insert records from all of the joins to the pit table and to #PIT.PIT_id
    begin tran
      delete dbo.d_udwcloudsync_product_master_history
       where d_udwcloudsync_product_master_history.p_udwcloudsync_product_master_id in (select p_udwcloudsync_product_master_id from #p_udwcloudsync_product_master_id_list)

      insert dbo.d_udwcloudsync_product_master_history(
                 bk_hash,
                 product_id,
                 product_sku,
                 source_system,
                 effective_date_time,
                 expiration_date_time,
                 allocation_rule,
                 connectivity_lead_generator_flag,
                 connectivity_primary_lead_generator_flag,
                 corporate_transfer_flag,
                 corporate_transfer_multiplier,
                 departmental_dssr_flag,
                 dim_cafe_product_key,
                 dim_healthcheckusa_product_key,
                 dim_hybris_product_key,
                 dim_magento_product_key,
                 dim_mms_product_key,
                 dim_reporting_hierarchy_key,
                 dssr_down_grade_other_enrollment_fee_flag,
                 dssr_if_admin_fee_flag,
                 experience_life_magazine_flag,
                 mtd_average_delivered_session_price_flag,
                 mtd_average_sale_price_flag,
                 new_business_old_business_flag,
                 payroll_description,
                 payroll_lt_bucks_group_description,
                 payroll_lt_bucks_group_sort_order,
                 payroll_lt_bucks_product_group_flag,
                 payroll_lt_bucks_sales_amount_flag,
                 payroll_lt_bucks_service_amount_flag,
                 payroll_lt_bucks_service_quantity_flag,
                 payroll_region_type,
                 payroll_standard_group_description,
                 payroll_standard_group_sort_order,
                 payroll_standard_product_group_flag,
                 payroll_standard_sales_amount_flag,
                 payroll_standard_service_amount_flag,
                 payroll_standard_service_quantity_flag,
                 payroll_track_sales_flag,
                 payroll_track_service_flag,
                 product_description,
                 product_status,
                 reporting_department,
                 reporting_division,
                 reporting_product_group,
                 reporting_product_group_gl_account,
                 reporting_product_group_sort_order,
                 reporting_region_type,
                 reporting_sub_division,
                 revenue_product_group_discount_gl_account,
                 revenue_product_group_refund_gl_account,
                 sales_category_description,
                 deleted_flag,
                 p_udwcloudsync_product_master_id,
                 dv_load_date_time,
                 dv_load_end_date_time,
                 dv_batch_id,
                 dv_inserted_date_time,
                 dv_insert_user)
      select bk_hash,
             product_id,
             product_sku,
             source_system,
             effective_date_time,
             expiration_date_time,
             allocation_rule,
             connectivity_lead_generator_flag,
             connectivity_primary_lead_generator_flag,
             corporate_transfer_flag,
             corporate_transfer_multiplier,
             departmental_dssr_flag,
             dim_cafe_product_key,
             dim_healthcheckusa_product_key,
             dim_hybris_product_key,
             dim_magento_product_key,
             dim_mms_product_key,
             dim_reporting_hierarchy_key,
             dssr_down_grade_other_enrollment_fee_flag,
             dssr_if_admin_fee_flag,
             experience_life_magazine_flag,
             mtd_average_delivered_session_price_flag,
             mtd_average_sale_price_flag,
             new_business_old_business_flag,
             payroll_description,
             payroll_lt_bucks_group_description,
             payroll_lt_bucks_group_sort_order,
             payroll_lt_bucks_product_group_flag,
             payroll_lt_bucks_sales_amount_flag,
             payroll_lt_bucks_service_amount_flag,
             payroll_lt_bucks_service_quantity_flag,
             payroll_region_type,
             payroll_standard_group_description,
             payroll_standard_group_sort_order,
             payroll_standard_product_group_flag,
             payroll_standard_sales_amount_flag,
             payroll_standard_service_amount_flag,
             payroll_standard_service_quantity_flag,
             payroll_track_sales_flag,
             payroll_track_service_flag,
             product_description,
             product_status,
             reporting_department,
             reporting_division,
             reporting_product_group,
             reporting_product_group_gl_account,
             reporting_product_group_sort_order,
             reporting_region_type,
             reporting_sub_division,
             revenue_product_group_discount_gl_account,
             revenue_product_group_refund_gl_account,
             sales_category_description,
             dv_deleted,
             p_udwcloudsync_product_master_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             getdate(),
             suser_sname()
        from #insert
    commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_udwcloudsync_product_master_history)
--Done!
end
