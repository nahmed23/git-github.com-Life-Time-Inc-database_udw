CREATE PROC [dbo].[proc_d_boss_asi_player] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_boss_asi_player)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_boss_asi_player_insert') is not null drop table #p_boss_asi_player_insert
create table dbo.#p_boss_asi_player_insert with(distribution=hash(bk_hash), location=user_db) as
select p_boss_asi_player.p_boss_asi_player_id,
       p_boss_asi_player.bk_hash
  from dbo.p_boss_asi_player
 where p_boss_asi_player.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_boss_asi_player.dv_batch_id > @max_dv_batch_id
        or p_boss_asi_player.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_boss_asi_player.bk_hash,
       p_boss_asi_player.asi_player_id asi_player_id,
       s_boss_asi_player.status asi_player_status,
       s_boss_asi_player.balance_due balance_due_amount,
       s_boss_asi_player.can_charge can_charge_flag,
       case when p_boss_asi_player.bk_hash in('-997', '-998', '-999') then p_boss_asi_player.bk_hash
           when s_boss_asi_player.cancel_date is null then '-998'
       	else convert(varchar, s_boss_asi_player.cancel_date, 112) 
       end cancel_dim_date_key,
       case when p_boss_asi_player.bk_hash in('-997', '-998', '-999') then p_boss_asi_player.bk_hash
           when s_boss_asi_player.check_in_date is null then '-998'
       	else convert(varchar, s_boss_asi_player.check_in_date, 112) 
       end check_in_dim_date_key,
       case when p_boss_asi_player.bk_hash in('-997', '-998', '-999') then p_boss_asi_player.bk_hash
           when s_boss_asi_player.check_in_date is null then '-998'
       	else '1' + replace(substring(convert(varchar,s_boss_asi_player.check_in_date,114), 1, 5),':','')
       end check_in_dim_time_key,
       s_boss_asi_player.checked_in checked_in_flag,
       l_boss_asi_player.contact_id contact_id,
       case when p_boss_asi_player.bk_hash in('-997', '-998', '-999') then p_boss_asi_player.bk_hash
           when s_boss_asi_player.created_at is null then '-998'
        else convert(varchar, s_boss_asi_player.created_at, 112)    end created_dim_date_key,
       case when p_boss_asi_player.bk_hash in ('-997','-998','-999') then p_boss_asi_player.bk_hash
       when s_boss_asi_player.created_at is null then '-998'
       else '1' + replace(substring(convert(varchar,s_boss_asi_player.created_at,114), 1, 5),':','') end created_dim_time_key,
       case when p_boss_asi_player.bk_hash in('-997', '-998', '-999') then p_boss_asi_player.bk_hash
           when s_boss_asi_player.dob is null then '-998'
       	else convert(varchar, s_boss_asi_player.dob, 112) 
       end date_of_birth_dim_date_key,
       case when l_boss_asi_player.bk_hash in ('-997', '-998', '-999') then p_boss_asi_player.bk_hash
           when l_boss_asi_player.reservation is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_boss_asi_player.reservation as int) as varchar(500)),'z#@$k%&P'))),2) end dim_boss_reservation_key,
       case when p_boss_asi_player.bk_hash in('-997', '-998', '-999') then p_boss_asi_player.bk_hash
           when l_boss_asi_player.mbr_code is not null and isnumeric(l_boss_asi_player.mbr_code) = 1  and convert(int, l_boss_asi_player.mbr_code) > 100000000         
               and len(ltrim(rtrim(l_boss_asi_player.mbr_code))) = 9 then convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(cast(ltrim(rtrim(l_boss_asi_player.mbr_code)) as int) as int) as varchar(500)),'z#@$k%&P'))),2)
       	else '-998'
       end dim_mms_member_key,
       h_boss_asi_player.dv_deleted dv_deleted_flag,
       s_boss_asi_player.email email_address,
       case when p_boss_asi_player.bk_hash in ('-997', '-998', '-999') then p_boss_asi_player.bk_hash
           when l_boss_asi_player.mms_trans_id is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(ltrim(rtrim(l_boss_asi_player.mms_trans_id)) as int) as varchar(500)),'z#@$k%&P'))),2) end fact_mms_sales_transaction_key,
       s_boss_asi_player.house_acct house_account,
       s_boss_asi_player.last_paid_date last_paid_date_time,
       l_boss_asi_player.mbr_code member_code,
       case when p_boss_asi_player.bk_hash in('-997', '-998', '-999')  then 'N' 
           when l_boss_asi_player.mbr_code is not null and isnumeric(l_boss_asi_player.mbr_code) = 1  and convert(int, l_boss_asi_player.mbr_code) > 100000000         
               and len(ltrim(rtrim(l_boss_asi_player.mbr_code))) = 9 then 'Y'
       	else 'N'
       end member_flag,
       s_boss_asi_player.mbr_type member_type,
       l_boss_asi_player.mbrship_type_id membership_type_id,
       s_boss_asi_player.mms_swipe mms_swipe_flag,
       isnull(s_boss_asi_player.notes,'') notes,
       s_boss_asi_player.origin origin,
       s_boss_asi_player.paid paid,
       s_boss_asi_player.phone phone_number,
       s_boss_asi_player.price price,
       l_boss_asi_player.recurrence_id recurrence_id,
       s_boss_asi_player.rostered_by rostered_by,
       s_boss_asi_player.sequence sequence,
       case when p_boss_asi_player.bk_hash in('-997', '-998', '-999') then p_boss_asi_player.bk_hash
           when s_boss_asi_player.start_date is null then '-998'
       	else convert(varchar, s_boss_asi_player.start_date, 112) 
       end start_dim_date_key,
       s_boss_asi_player.tax_amt tax_amount,
       s_boss_asi_player.trans trans,
       case when p_boss_asi_player.bk_hash in('-997', '-998', '-999') then p_boss_asi_player.bk_hash
           when s_boss_asi_player.date_used is null then '-998'
       	else convert(varchar, s_boss_asi_player.date_used, 112) 
       end used_dim_date_key,
       isnull(h_boss_asi_player.dv_deleted,0) dv_deleted,
       p_boss_asi_player.p_boss_asi_player_id,
       p_boss_asi_player.dv_batch_id,
       p_boss_asi_player.dv_load_date_time,
       p_boss_asi_player.dv_load_end_date_time
  from dbo.h_boss_asi_player
  join dbo.p_boss_asi_player
    on h_boss_asi_player.bk_hash = p_boss_asi_player.bk_hash
  join #p_boss_asi_player_insert
    on p_boss_asi_player.bk_hash = #p_boss_asi_player_insert.bk_hash
   and p_boss_asi_player.p_boss_asi_player_id = #p_boss_asi_player_insert.p_boss_asi_player_id
  join dbo.l_boss_asi_player
    on p_boss_asi_player.bk_hash = l_boss_asi_player.bk_hash
   and p_boss_asi_player.l_boss_asi_player_id = l_boss_asi_player.l_boss_asi_player_id
  join dbo.s_boss_asi_player
    on p_boss_asi_player.bk_hash = s_boss_asi_player.bk_hash
   and p_boss_asi_player.s_boss_asi_player_id = s_boss_asi_player.s_boss_asi_player_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_boss_asi_player
   where d_boss_asi_player.bk_hash in (select bk_hash from #p_boss_asi_player_insert)

  insert dbo.d_boss_asi_player(
             bk_hash,
             asi_player_id,
             asi_player_status,
             balance_due_amount,
             can_charge_flag,
             cancel_dim_date_key,
             check_in_dim_date_key,
             check_in_dim_time_key,
             checked_in_flag,
             contact_id,
             created_dim_date_key,
             created_dim_time_key,
             date_of_birth_dim_date_key,
             dim_boss_reservation_key,
             dim_mms_member_key,
             dv_deleted_flag,
             email_address,
             fact_mms_sales_transaction_key,
             house_account,
             last_paid_date_time,
             member_code,
             member_flag,
             member_type,
             membership_type_id,
             mms_swipe_flag,
             notes,
             origin,
             paid,
             phone_number,
             price,
             recurrence_id,
             rostered_by,
             sequence,
             start_dim_date_key,
             tax_amount,
             trans,
             used_dim_date_key,
             deleted_flag,
             p_boss_asi_player_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         asi_player_id,
         asi_player_status,
         balance_due_amount,
         can_charge_flag,
         cancel_dim_date_key,
         check_in_dim_date_key,
         check_in_dim_time_key,
         checked_in_flag,
         contact_id,
         created_dim_date_key,
         created_dim_time_key,
         date_of_birth_dim_date_key,
         dim_boss_reservation_key,
         dim_mms_member_key,
         dv_deleted_flag,
         email_address,
         fact_mms_sales_transaction_key,
         house_account,
         last_paid_date_time,
         member_code,
         member_flag,
         member_type,
         membership_type_id,
         mms_swipe_flag,
         notes,
         origin,
         paid,
         phone_number,
         price,
         recurrence_id,
         rostered_by,
         sequence,
         start_dim_date_key,
         tax_amount,
         trans,
         used_dim_date_key,
         dv_deleted,
         p_boss_asi_player_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_boss_asi_player)
--Done!
end
