CREATE PROC [dbo].[proc_d_ig_it_cfg_terminal_master] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_ig_it_cfg_terminal_master)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_ig_it_cfg_terminal_master_insert') is not null drop table #p_ig_it_cfg_terminal_master_insert
create table dbo.#p_ig_it_cfg_terminal_master_insert with(distribution=hash(bk_hash), location=user_db) as
select p_ig_it_cfg_terminal_master.p_ig_it_cfg_terminal_master_id,
       p_ig_it_cfg_terminal_master.bk_hash
  from dbo.p_ig_it_cfg_terminal_master
 where p_ig_it_cfg_terminal_master.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_ig_it_cfg_terminal_master.dv_batch_id > @max_dv_batch_id
        or p_ig_it_cfg_terminal_master.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_ig_it_cfg_terminal_master.bk_hash,
       p_ig_it_cfg_terminal_master.bk_hash dim_ig_it_cfg_terminal_master_key,
       p_ig_it_cfg_terminal_master.term_id term_id,
       case when p_ig_it_cfg_terminal_master.bk_hash in('-997', '-998', '-999') then p_ig_it_cfg_terminal_master.bk_hash
           when l_ig_it_cfg_terminal_master.alt_bargun_term_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_ig_it_cfg_terminal_master.alt_bargun_term_id as int) as varchar(500)),'z#@$k%&P'))),2)   end alt_bargun_dim_ig_it_cfg_terminal_master_key,
       l_ig_it_cfg_terminal_master.alt_bargun_term_id alt_bargun_term_id,
       case when p_ig_it_cfg_terminal_master.bk_hash in('-997', '-998', '-999') then p_ig_it_cfg_terminal_master.bk_hash
           when l_ig_it_cfg_terminal_master.alt_rcpt_term_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_ig_it_cfg_terminal_master.alt_rcpt_term_id as int) as varchar(500)),'z#@$k%&P'))),2)   end alt_rcpt_dim_ig_it_cfg_terminal_master_key,
       l_ig_it_cfg_terminal_master.alt_rcpt_term_id alt_rcpt_term_id,
       case when s_ig_it_cfg_terminal_master.bargun_bump_override_flag = 1 then 'Y' else 'N' end bargun_bump_override_flag,
       case when s_ig_it_cfg_terminal_master.bargun_print_drinks_on_bump_flag = 1 then 'Y' else 'N'  end bargun_print_drinks_on_bump_flag,
       case when s_ig_it_cfg_terminal_master.bargun_term_flag = 1 then 'Y' else 'N'  end bargun_term_flag,
        s_ig_it_cfg_terminal_master.current_version current_version,
       s_ig_it_cfg_terminal_master.default_table_layout_id default_table_layout_id,
       s_ig_it_cfg_terminal_master.first_table_no first_table_no,
       case when s_ig_it_cfg_terminal_master.fol_file_load_flag = 1 then 'Y' else 'N'  end fol_file_load_flag,
       case when s_ig_it_cfg_terminal_master.ga_file_load_flag = 1 then 'Y' else 'N'  end ga_file_load_flag,
        s_ig_it_cfg_terminal_master.ip_address ip_address,
       case when s_ig_it_cfg_terminal_master.is_default_api_terminal = 1 then 'Y' else 'N'  end is_default_api_terminal_flag,
       s_ig_it_cfg_terminal_master_1.last_ping_utc_date_time last_ping_utc_date_time,
       case when p_ig_it_cfg_terminal_master.bk_hash in('-997', '-998', '-999') then p_ig_it_cfg_terminal_master.bk_hash
           when s_ig_it_cfg_terminal_master_1.last_ping_utc_date_time is null then '-998'
        else convert(varchar, s_ig_it_cfg_terminal_master_1.last_ping_utc_date_time, 112)    end last_ping_utc_dim_date_key,
       case when p_ig_it_cfg_terminal_master.bk_hash in ('-997','-998','-999') then p_ig_it_cfg_terminal_master.bk_hash
       when s_ig_it_cfg_terminal_master_1.last_ping_utc_date_time is null then '-998'
       else '1' + replace(substring(convert(varchar,s_ig_it_cfg_terminal_master_1.last_ping_utc_date_time,114), 1, 5),':','') end last_ping_utc_dim_time_key,
       s_ig_it_cfg_terminal_master.num_tables num_tables,
       s_ig_it_cfg_terminal_master.payment_device_id payment_device_id,
        s_ig_it_cfg_terminal_master.ped_value ped_value,
       l_ig_it_cfg_terminal_master.primary_profit_center_id primary_profit_center_id,
       l_ig_it_cfg_terminal_master.profile_id profile_id,
       s_ig_it_cfg_terminal_master.receipt_printer_type receipt_printer_type,
       case when s_ig_it_cfg_terminal_master.rms_file_load_flag = 1 then 'Y' else 'N'  end rms_file_load_flag,
       s_ig_it_cfg_terminal_master_1.row_version row_version,
       s_ig_it_cfg_terminal_master.static_receipt_printer_id static_receipt_printer_id,
       case when s_ig_it_cfg_terminal_master.term_active_flag = 1 then 'Y' else 'N' end term_active_flag,
       l_ig_it_cfg_terminal_master.term_grp_id term_grp_id,
        s_ig_it_cfg_terminal_master.term_name term_name,
       l_ig_it_cfg_terminal_master.term_option_grp_id term_option_grp_id,
       l_ig_it_cfg_terminal_master.term_printer_grp_id term_printer_grp_id,
       s_ig_it_cfg_terminal_master.term_receipt_info term_receipt_info,
       l_ig_it_cfg_terminal_master.term_service_grp_id term_service_grp_id,
       case when s_ig_it_cfg_terminal_master.virtual_term_flag = 1 then 'Y' else 'N'  end virtual_term_flag,
       isnull(h_ig_it_cfg_terminal_master.dv_deleted,0) dv_deleted,
       p_ig_it_cfg_terminal_master.p_ig_it_cfg_terminal_master_id,
       p_ig_it_cfg_terminal_master.dv_batch_id,
       p_ig_it_cfg_terminal_master.dv_load_date_time,
       p_ig_it_cfg_terminal_master.dv_load_end_date_time
  from dbo.h_ig_it_cfg_terminal_master
  join dbo.p_ig_it_cfg_terminal_master
    on h_ig_it_cfg_terminal_master.bk_hash = p_ig_it_cfg_terminal_master.bk_hash
  join #p_ig_it_cfg_terminal_master_insert
    on p_ig_it_cfg_terminal_master.bk_hash = #p_ig_it_cfg_terminal_master_insert.bk_hash
   and p_ig_it_cfg_terminal_master.p_ig_it_cfg_terminal_master_id = #p_ig_it_cfg_terminal_master_insert.p_ig_it_cfg_terminal_master_id
  join dbo.l_ig_it_cfg_terminal_master
    on p_ig_it_cfg_terminal_master.bk_hash = l_ig_it_cfg_terminal_master.bk_hash
   and p_ig_it_cfg_terminal_master.l_ig_it_cfg_terminal_master_id = l_ig_it_cfg_terminal_master.l_ig_it_cfg_terminal_master_id
  join dbo.s_ig_it_cfg_terminal_master
    on p_ig_it_cfg_terminal_master.bk_hash = s_ig_it_cfg_terminal_master.bk_hash
   and p_ig_it_cfg_terminal_master.s_ig_it_cfg_terminal_master_id = s_ig_it_cfg_terminal_master.s_ig_it_cfg_terminal_master_id
  join dbo.s_ig_it_cfg_terminal_master_1
    on p_ig_it_cfg_terminal_master.bk_hash = s_ig_it_cfg_terminal_master_1.bk_hash
   and p_ig_it_cfg_terminal_master.s_ig_it_cfg_terminal_master_1_id = s_ig_it_cfg_terminal_master_1.s_ig_it_cfg_terminal_master_1_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_ig_it_cfg_terminal_master
   where d_ig_it_cfg_terminal_master.bk_hash in (select bk_hash from #p_ig_it_cfg_terminal_master_insert)

  insert dbo.d_ig_it_cfg_terminal_master(
             bk_hash,
             dim_ig_it_cfg_terminal_master_key,
             term_id,
             alt_bargun_dim_ig_it_cfg_terminal_master_key,
             alt_bargun_term_id,
             alt_rcpt_dim_ig_it_cfg_terminal_master_key,
             alt_rcpt_term_id,
             bargun_bump_override_flag,
             bargun_print_drinks_on_bump_flag,
             bargun_term_flag,
             current_version,
             default_table_layout_id,
             first_table_no,
             fol_file_load_flag,
             ga_file_load_flag,
             ip_address,
             is_default_api_terminal_flag,
             last_ping_utc_date_time,
             last_ping_utc_dim_date_key,
             last_ping_utc_dim_time_key,
             num_tables,
             payment_device_id,
             ped_value,
             primary_profit_center_id,
             profile_id,
             receipt_printer_type,
             rms_file_load_flag,
             row_version,
             static_receipt_printer_id,
             term_active_flag,
             term_grp_id,
             term_name,
             term_option_grp_id,
             term_printer_grp_id,
             term_receipt_info,
             term_service_grp_id,
             virtual_term_flag,
             deleted_flag,
             p_ig_it_cfg_terminal_master_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         dim_ig_it_cfg_terminal_master_key,
         term_id,
         alt_bargun_dim_ig_it_cfg_terminal_master_key,
         alt_bargun_term_id,
         alt_rcpt_dim_ig_it_cfg_terminal_master_key,
         alt_rcpt_term_id,
         bargun_bump_override_flag,
         bargun_print_drinks_on_bump_flag,
         bargun_term_flag,
         current_version,
         default_table_layout_id,
         first_table_no,
         fol_file_load_flag,
         ga_file_load_flag,
         ip_address,
         is_default_api_terminal_flag,
         last_ping_utc_date_time,
         last_ping_utc_dim_date_key,
         last_ping_utc_dim_time_key,
         num_tables,
         payment_device_id,
         ped_value,
         primary_profit_center_id,
         profile_id,
         receipt_printer_type,
         rms_file_load_flag,
         row_version,
         static_receipt_printer_id,
         term_active_flag,
         term_grp_id,
         term_name,
         term_option_grp_id,
         term_printer_grp_id,
         term_receipt_info,
         term_service_grp_id,
         virtual_term_flag,
         dv_deleted,
         p_ig_it_cfg_terminal_master_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_ig_it_cfg_terminal_master)
--Done!
end
