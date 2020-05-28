CREATE PROC [dbo].[proc_d_mms_product_history] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!

-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_mms_product_history);

if object_id('tempdb..#p_mms_product_id_list') is not null drop table #p_mms_product_id_list
create table dbo.#p_mms_product_id_list with(distribution=hash(bk_hash), location=user_db, heap) as
with undo_delete (p_mms_product_id,bk_hash,dv_load_date_time) as 
(
-- Find any updates with the current batch ID to undo in case of retry - just like in the PIT Proc
--   Find the records in the current batch
--   Find the records related to the current batch
--   Note that this needs to be done using the PIT ids within the fact/dimension base table since a workflow retry would have deleted those IDs from the PIT table and reinserted with new IDs
    select p_mms_product_id,
           bk_hash,
           dv_load_date_time
      from dbo.d_mms_product_history
     where dv_batch_id = @current_dv_batch_id
),
undo_update (p_mms_product_id,bk_hash) as
(
    select d_mms_product_history.p_mms_product_id,
           d_mms_product_history.bk_hash
      from dbo.d_mms_product_history
      join undo_delete
        on d_mms_product_history.bk_hash = undo_delete.bk_hash
       and d_mms_product_history.dv_load_end_date_time = undo_delete.dv_load_date_time
),
p_mms_product_insert (p_mms_product_id,bk_hash,dv_load_date_time) as 
(
-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those have not been physically deleted yet - if they exist
-- Then find the PIT ids in the PIT table that correspond to the dimension/fact records to end-date
    select p_mms_product_id,
           bk_hash,
           dv_load_date_time
      from dbo.p_mms_product
     where dv_batch_id > @max_dv_batch_id
        or dv_batch_id = @current_dv_batch_id
),
p_mms_product_update (p_mms_product_id,bk_hash) as
(
    select p_mms_product.p_mms_product_id,
           p_mms_product.bk_hash
      from dbo.p_mms_product
      join p_mms_product_insert
        on p_mms_product.bk_hash = p_mms_product_insert.bk_hash
       and p_mms_product.dv_load_end_date_time = p_mms_product_insert.dv_load_date_time
)
select undo_delete.p_mms_product_id,
       bk_hash
  from undo_delete
union
select undo_update.p_mms_product_id,
       bk_hash
  from undo_update
union
select p_mms_product_insert.p_mms_product_id,
       bk_hash
  from p_mms_product_insert
union
select p_mms_product_update.p_mms_product_id,
       bk_hash
  from p_mms_product_update

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db, heap) as
select #p_mms_product_id_list.bk_hash,
       p_mms_product.bk_hash dim_mms_product_key,
       p_mms_product.product_id product_id,
       isnull(p_mms_product.dv_greatest_satellite_date_time, convert(datetime, '2000.01.01', 102)) effective_date_time,
       case when p_mms_product.dv_load_end_date_time = convert(datetime, '9999.12.31', 102) then p_mms_product.dv_load_end_date_time       else p_mms_product.dv_next_greatest_satellite_date_time   end expiration_date_time,
       case when s_mms_product_2.access_by_price_paid_flag = 1 then 'Y' else 'N' end access_by_price_paid_flag,
       case when s_mms_product.assess_as_dues_flag = 1 then 'Y' else 'N' end assess_as_dues_flag,
       case when s_mms_product.deferred_revenue_flag = 1 then 'Y' else 'N' end deferred_revenue_flag,
       l_mms_product.department_id department_id,
       case when s_mms_product.display_ui_flag = 1 then 'Y' else 'N' end display_ui_flag,
       case when s_mms_product_2.exclude_from_club_POS_flag = 1 then 'Y' else 'N' end exclude_from_club_POS_flag,
       isnull(s_mms_product.gl_account_number,'') gl_account_number,
       case when s_mms_product.gl_sub_account_number is null then '' else substring(gl_sub_account_number,1,3) end gl_department_code,
       l_mms_product.gl_over_ride_club_id gl_over_ride_club_id,
       case when s_mms_product.gl_sub_account_number is null then '' else substring(gl_sub_account_number,(len(gl_sub_account_number)-2),3) end gl_product_code,
       case when s_mms_product.jr_member_dues_flag = 1 then 'Y' else 'N' end junior_member_dues_flag,
       s_mms_Product_1.lt_buck_cost_percent lt_buck_cost_percent,
       case when s_mms_Product_1.lt_buck_eligible = 1 then 'Y' else 'N' end lt_buck_eligible_flag,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_product.department_id as int) as varchar(500)),'z#@$k%&P'))),2) mms_department_bk_hash,
       case when s_mms_product.package_product_flag = 1 then 'Y' else 'N' end package_product_flag,
       isnull(l_mms_product.pay_component,'') pay_component,
       case when s_mms_product.price_locked_flag = 1 then 'Y' else 'N' end price_locked_flag,
       ltrim(rtrim(s_mms_product.description)) product_description,
       s_mms_product.name product_name,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_product.val_product_status_id as int) as varchar(500)),'z#@$k%&P'))),2) r_mms_val_product_status_bk_hash,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_product.val_recurrent_product_type_id as int) as varchar(500)),'z#@$k%&P'))),2) r_mms_val_recurrent_product_type_bk_hash,
       isnull(l_mms_product.revenue_category,'') revenue_category,
       case when s_mms_product.product_id = 1492 then '5' else '1'  end sales_quantity_factor,
       isnull(s_mms_product.sku,'') sku,
       isnull(l_mms_product.spend_category,'') spend_category,
       case when s_mms_product.tip_allowed_flag = 1 then 'Y' else 'N' end tip_allowed_flag,
       isnull(l_mms_product.workday_account,'') workday_account,
       isnull(l_mms_product.workday_cost_center,'') workday_cost_center,
       isnull(l_mms_product.workday_offering,'') workday_offering,
       isnull(l_mms_product.workday_over_ride_region,'') workday_over_ride_region,
       isnull(l_mms_product.workday_revenue_product_group_account,'') workday_revenue_product_group_account,
       h_mms_product.dv_deleted,
       p_mms_product.p_mms_product_id,
       p_mms_product.dv_batch_id,
       p_mms_product.dv_load_date_time,
       p_mms_product.dv_load_end_date_time
  from dbo.h_mms_product
  join dbo.p_mms_product
    on h_mms_product.bk_hash = p_mms_product.bk_hash  join #p_mms_product_id_list
    on p_mms_product.p_mms_product_id = #p_mms_product_id_list.p_mms_product_id
   and p_mms_product.bk_hash = #p_mms_product_id_list.bk_hash
  join dbo.l_mms_product
    on p_mms_product.bk_hash = l_mms_product.bk_hash
   and p_mms_product.l_mms_product_id = l_mms_product.l_mms_product_id
  join dbo.s_mms_product
    on p_mms_product.bk_hash = s_mms_product.bk_hash
   and p_mms_product.s_mms_product_id = s_mms_product.s_mms_product_id
  join dbo.s_mms_product_1
    on p_mms_product.bk_hash = s_mms_product_1.bk_hash
   and p_mms_product.s_mms_product_1_id = s_mms_product_1.s_mms_product_1_id
  join dbo.s_mms_product_2
    on p_mms_product.bk_hash = s_mms_product_2.bk_hash
   and p_mms_product.s_mms_product_2_id = s_mms_product_2.s_mms_product_2_id
 where isnull(p_mms_product.dv_greatest_satellite_date_time, convert(datetime, '2000.01.01', 102))!= case when p_mms_product.dv_load_end_date_time = convert(datetime, '9999.12.31', 102) then p_mms_product.dv_load_end_date_time       else p_mms_product.dv_next_greatest_satellite_date_time   end


-- do as a single transaction
--   delete records from dimension where PIT_id = #PIT.PIT_id
--     Note that this also gets rid of any records where the existing effective_date_time equals the soon to be newly calculated expiration_date_time
--   insert records from all of the joins to the pit table and to #PIT.PIT_id
    begin tran
      delete dbo.d_mms_product_history
       where d_mms_product_history.p_mms_product_id in (select p_mms_product_id from #p_mms_product_id_list)

      insert dbo.d_mms_product_history(
                 bk_hash,
                 dim_mms_product_key,
                 product_id,
                 effective_date_time,
                 expiration_date_time,
                 access_by_price_paid_flag,
                 assess_as_dues_flag,
                 deferred_revenue_flag,
                 department_id,
                 display_ui_flag,
                 exclude_from_club_POS_flag,
                 gl_account_number,
                 gl_department_code,
                 gl_over_ride_club_id,
                 gl_product_code,
                 junior_member_dues_flag,
                 lt_buck_cost_percent,
                 lt_buck_eligible_flag,
                 mms_department_bk_hash,
                 package_product_flag,
                 pay_component,
                 price_locked_flag,
                 product_description,
                 product_name,
                 r_mms_val_product_status_bk_hash,
                 r_mms_val_recurrent_product_type_bk_hash,
                 revenue_category,
                 sales_quantity_factor,
                 sku,
                 spend_category,
                 tip_allowed_flag,
                 workday_account,
                 workday_cost_center,
                 workday_offering,
                 workday_over_ride_region,
                 workday_revenue_product_group_account,
                 deleted_flag,
                 p_mms_product_id,
                 dv_load_date_time,
                 dv_load_end_date_time,
                 dv_batch_id,
                 dv_inserted_date_time,
                 dv_insert_user)
      select bk_hash,
             dim_mms_product_key,
             product_id,
             effective_date_time,
             expiration_date_time,
             access_by_price_paid_flag,
             assess_as_dues_flag,
             deferred_revenue_flag,
             department_id,
             display_ui_flag,
             exclude_from_club_POS_flag,
             gl_account_number,
             gl_department_code,
             gl_over_ride_club_id,
             gl_product_code,
             junior_member_dues_flag,
             lt_buck_cost_percent,
             lt_buck_eligible_flag,
             mms_department_bk_hash,
             package_product_flag,
             pay_component,
             price_locked_flag,
             product_description,
             product_name,
             r_mms_val_product_status_bk_hash,
             r_mms_val_recurrent_product_type_bk_hash,
             revenue_category,
             sales_quantity_factor,
             sku,
             spend_category,
             tip_allowed_flag,
             workday_account,
             workday_cost_center,
             workday_offering,
             workday_over_ride_region,
             workday_revenue_product_group_account,
             dv_deleted,
             p_mms_product_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             getdate(),
             suser_sname()
        from #insert
    commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_mms_product_history)
--Done!
end
