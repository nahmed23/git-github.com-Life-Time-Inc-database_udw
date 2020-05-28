CREATE PROC [dbo].[proc_d_mms_club_history] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!

-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_mms_club_history);

if object_id('tempdb..#p_mms_club_id_list') is not null drop table #p_mms_club_id_list
create table dbo.#p_mms_club_id_list with(distribution=hash(bk_hash), location=user_db, heap) as
with undo_delete (p_mms_club_id,bk_hash,dv_load_date_time) as 
(
-- Find any updates with the current batch ID to undo in case of retry - just like in the PIT Proc
--   Find the records in the current batch
--   Find the records related to the current batch
--   Note that this needs to be done using the PIT ids within the fact/dimension base table since a workflow retry would have deleted those IDs from the PIT table and reinserted with new IDs
    select p_mms_club_id,
           bk_hash,
           dv_load_date_time
      from dbo.d_mms_club_history
     where dv_batch_id = @current_dv_batch_id
),
undo_update (p_mms_club_id,bk_hash) as
(
    select d_mms_club_history.p_mms_club_id,
           d_mms_club_history.bk_hash
      from dbo.d_mms_club_history
      join undo_delete
        on d_mms_club_history.bk_hash = undo_delete.bk_hash
       and d_mms_club_history.dv_load_end_date_time = undo_delete.dv_load_date_time
),
p_mms_club_insert (p_mms_club_id,bk_hash,dv_load_date_time) as 
(
-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those have not been physically deleted yet - if they exist
-- Then find the PIT ids in the PIT table that correspond to the dimension/fact records to end-date
    select p_mms_club_id,
           bk_hash,
           dv_load_date_time
      from dbo.p_mms_club
     where dv_batch_id > @max_dv_batch_id
        or dv_batch_id = @current_dv_batch_id
),
p_mms_club_update (p_mms_club_id,bk_hash) as
(
    select p_mms_club.p_mms_club_id,
           p_mms_club.bk_hash
      from dbo.p_mms_club
      join p_mms_club_insert
        on p_mms_club.bk_hash = p_mms_club_insert.bk_hash
       and p_mms_club.dv_load_end_date_time = p_mms_club_insert.dv_load_date_time
)
select undo_delete.p_mms_club_id,
       bk_hash
  from undo_delete
union
select undo_update.p_mms_club_id,
       bk_hash
  from undo_update
union
select p_mms_club_insert.p_mms_club_id,
       bk_hash
  from p_mms_club_insert
union
select p_mms_club_update.p_mms_club_id,
       bk_hash
  from p_mms_club_update

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db, heap) as
select #p_mms_club_id_list.bk_hash,
       p_mms_club.bk_hash dim_club_key,
       p_mms_club.club_id club_id,
       isnull(p_mms_club.dv_greatest_satellite_date_time, convert(datetime, '2000.01.01', 102)) effective_date_time,
       case when p_mms_club.dv_load_end_date_time = convert(datetime, '9999.12.31', 102) then p_mms_club.dv_load_end_date_time
                     else p_mms_club.dv_next_greatest_satellite_date_time
                end expiration_date_time,
       case when s_mms_club.allow_junior_check_in_flag = 1 then 'Y' else 'N' end allow_junior_check_in_flag,
       case when s_mms_club.assess_junior_member_dues_flag = 1 then 'Y' else 'N' end assess_junior_member_dues_flag,
       case when p_mms_club.bk_hash in ('-997','-998','-999') then p_mms_club.bk_hash
            when s_mms_club.check_in_group_level is null then '-998'
            else 'r_mms_val_check_in_group_'+convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(s_mms_club.check_in_group_level as int) as varchar(500)),'z#@$k%&P'))),2)
        end check_in_group_dim_description_key,
       isnull(s_mms_club.check_in_group_level,0) check_in_group_level,
       s_mms_club.child_center_weekly_limit child_center_weekly_limit,
       case when p_mms_club.bk_hash in ('-997','-998','-999') then p_mms_club.bk_hash
            when s_mms_club.club_deactivation_date is null then '-998'
            else convert(varchar,s_mms_club.club_deactivation_date,112)
        end club_close_dim_date_key,
       isnull(s_mms_club.club_code,'') club_code,
       isnull(s_mms_club.club_name,'') club_name,
       case when p_mms_club.bk_hash in ('-997','-998','-999') then p_mms_club.bk_hash
            when s_mms_club.club_activation_date is null then '-998'
            else convert(varchar,s_mms_club.club_activation_date,112)
        end club_open_dim_date_key,
       case when l_mms_club.val_pre_sale_id = 4 then 'Initial'
            when l_mms_club.val_pre_sale_id in (2,3,5,6) then 'Presale'
            when s_mms_club.club_deactivation_date <= getdate() then 'Closed'
            when l_mms_club.val_pre_sale_id = 1 then 'Open'
            else ''
        end club_status,
       case when l_mms_club.val_club_type_id in (3,6) or l_mms_club.club_id in (-1,99,100) then 'MMS Non-Club Location'
            else 'Club'
        end club_type,
       case when p_mms_club.bk_hash in ('-997','-998','-999') then p_mms_club.bk_hash
            when l_mms_club.val_currency_code_id is null then '-998'
            else 'r_mms_val_currency_code_'+convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_club.val_currency_code_id as int) as varchar(500)),'z#@$k%&P'))),2)
        end currency_code_dim_description_key,
       isnull(s_mms_club.domain_name_prefix,'') domain_name_prefix,
       r_mms_val_time_zone.dst_offset dst_offset,
       isnull(s_mms_club.formal_club_name,'') formal_club_name,
       l_mms_club.gl_club_id gl_club_id,
       l_mms_club.ig_store_id info_genesis_store_id,
       s_mms_club.marketing_club_level marketing_club_level,
       s_mms_club.marketing_map_region marketing_map_region,
       s_mms_club.max_junior_age max_junior_age,
       case when p_mms_club.bk_hash in ('-997','-998','-999') then p_mms_club.bk_hash
            when l_mms_club.val_member_activity_region_id is null then '-998'
            else 'r_mms_val_member_activity_region_'+convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_club.val_member_activity_region_id as int) as varchar(500)),'z#@$k%&P'))),2)
        end member_activities_region_dim_description_key,
       case when p_mms_club.bk_hash in ('-997','-998','-999') then p_mms_club.bk_hash
            when l_mms_club.val_pt_rcl_area_id is null then '-998'
            else 'r_mms_val_pt_rcl_area_'+convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_club.val_pt_rcl_area_id as int) as varchar(500)),'z#@$k%&P'))),2)
        end pt_rcl_area_dim_description_key,
       case when p_mms_club.bk_hash in ('-997','-998','-999') then p_mms_club.bk_hash
            when l_mms_club.val_region_id is null then '-998'
            else 'r_mms_val_region_'+convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_club.val_region_id as int) as varchar(500)),'z#@$k%&P'))),2)
        end region_dim_description_key,
       case when p_mms_club.bk_hash in ('-997','-998','-999') then p_mms_club.bk_hash
            when l_mms_club.val_sales_area_id is null then '-998'
            else 'r_mms_val_sales_area_'+convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_club.val_sales_area_id as int) as varchar(500)),'z#@$k%&P'))),2)
        end sales_area_dim_description_key,
       case when s_mms_club.sell_junior_member_dues_flag = 1 then 'Y' else 'N' end sell_junior_member_dues_flag,
       l_mms_club.site_id shortcurts_site_id,
       r_mms_val_time_zone.st_offset st_offset,
       l_mms_club.val_member_activity_region_id val_member_activity_region_id,
       l_mms_club.val_pt_rcl_area_id val_pt_rcl_area_id,
       l_mms_club.val_region_id val_region_id,
       l_mms_club.val_sales_area_id val_sales_area_id,
       l_mms_club.val_time_zone_id val_time_zone_id,
       s_mms_club.workday_region workday_region,
       h_mms_club.dv_deleted,
       p_mms_club.p_mms_club_id,
       p_mms_club.dv_batch_id,
       p_mms_club.dv_load_date_time,
       p_mms_club.dv_load_end_date_time
  from dbo.h_mms_club
  join dbo.p_mms_club
    on h_mms_club.bk_hash = p_mms_club.bk_hash  join #p_mms_club_id_list
    on p_mms_club.p_mms_club_id = #p_mms_club_id_list.p_mms_club_id
   and p_mms_club.bk_hash = #p_mms_club_id_list.bk_hash
  join dbo.l_mms_club
    on p_mms_club.bk_hash = l_mms_club.bk_hash
   and p_mms_club.l_mms_club_id = l_mms_club.l_mms_club_id
  join dbo.s_mms_club
    on p_mms_club.bk_hash = s_mms_club.bk_hash
   and p_mms_club.s_mms_club_id = s_mms_club.s_mms_club_id
 left join r_mms_val_time_zone on l_mms_club.val_time_zone_id = r_mms_val_time_zone.val_time_zone_id and r_mms_val_time_zone.dv_load_end_date_time = 'dec 31, 9999'
   and isnull(p_mms_club.dv_greatest_satellite_date_time, convert(datetime, '2000.01.01', 102))!= case when p_mms_club.dv_load_end_date_time = convert(datetime, '9999.12.31', 102) then p_mms_club.dv_load_end_date_time
              else p_mms_club.dv_next_greatest_satellite_date_time
         end


-- do as a single transaction
--   delete records from dimension where PIT_id = #PIT.PIT_id
--     Note that this also gets rid of any records where the existing effective_date_time equals the soon to be newly calculated expiration_date_time
--   insert records from all of the joins to the pit table and to #PIT.PIT_id
    begin tran
      delete dbo.d_mms_club_history
       where d_mms_club_history.p_mms_club_id in (select p_mms_club_id from #p_mms_club_id_list)

      insert dbo.d_mms_club_history(
                 bk_hash,
                 dim_club_key,
                 club_id,
                 effective_date_time,
                 expiration_date_time,
                 allow_junior_check_in_flag,
                 assess_junior_member_dues_flag,
                 check_in_group_dim_description_key,
                 check_in_group_level,
                 child_center_weekly_limit,
                 club_close_dim_date_key,
                 club_code,
                 club_name,
                 club_open_dim_date_key,
                 club_status,
                 club_type,
                 currency_code_dim_description_key,
                 domain_name_prefix,
                 dst_offset,
                 formal_club_name,
                 gl_club_id,
                 info_genesis_store_id,
                 marketing_club_level,
                 marketing_map_region,
                 max_junior_age,
                 member_activities_region_dim_description_key,
                 pt_rcl_area_dim_description_key,
                 region_dim_description_key,
                 sales_area_dim_description_key,
                 sell_junior_member_dues_flag,
                 shortcurts_site_id,
                 st_offset,
                 val_member_activity_region_id,
                 val_pt_rcl_area_id,
                 val_region_id,
                 val_sales_area_id,
                 val_time_zone_id,
                 workday_region,
                 deleted_flag,
                 p_mms_club_id,
                 dv_load_date_time,
                 dv_load_end_date_time,
                 dv_batch_id,
                 dv_inserted_date_time,
                 dv_insert_user)
      select bk_hash,
             dim_club_key,
             club_id,
             effective_date_time,
             expiration_date_time,
             allow_junior_check_in_flag,
             assess_junior_member_dues_flag,
             check_in_group_dim_description_key,
             check_in_group_level,
             child_center_weekly_limit,
             club_close_dim_date_key,
             club_code,
             club_name,
             club_open_dim_date_key,
             club_status,
             club_type,
             currency_code_dim_description_key,
             domain_name_prefix,
             dst_offset,
             formal_club_name,
             gl_club_id,
             info_genesis_store_id,
             marketing_club_level,
             marketing_map_region,
             max_junior_age,
             member_activities_region_dim_description_key,
             pt_rcl_area_dim_description_key,
             region_dim_description_key,
             sales_area_dim_description_key,
             sell_junior_member_dues_flag,
             shortcurts_site_id,
             st_offset,
             val_member_activity_region_id,
             val_pt_rcl_area_id,
             val_region_id,
             val_sales_area_id,
             val_time_zone_id,
             workday_region,
             dv_deleted,
             p_mms_club_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             getdate(),
             suser_sname()
        from #insert
    commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_mms_club_history)
--Done!
end
