CREATE PROC [dbo].[proc_d_mms_package] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_mms_package)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_mms_package_insert') is not null drop table #p_mms_package_insert
create table dbo.#p_mms_package_insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_package.p_mms_package_id,
       p_mms_package.bk_hash
  from dbo.p_mms_package
 where p_mms_package.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_mms_package.dv_batch_id > @max_dv_batch_id
        or p_mms_package.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_package.bk_hash,
       p_mms_package.bk_hash fact_mms_package_key,
       p_mms_package.package_id package_id,
       isnull(s_mms_package.balance_amount, 0) balance_amount,
       l_mms_package.club_id club_id,
       s_mms_package.created_date_time created_date_time,
       case when p_mms_package.bk_hash in ('-997', '-998', '-999') then p_mms_package.bk_hash 
       when s_mms_package.created_date_time is null then '-998'    
       else convert(varchar, s_mms_package.created_date_time, 112)   end created_dim_date_key,
       case when p_mms_package.bk_hash in ('-997', '-998', '-999') then p_mms_package.bk_hash 
       when s_mms_package.created_date_time is null then '-998'    
       else '1' + replace(substring(convert(varchar,s_mms_package.created_date_time,114), 1, 5),':','')   end created_dim_time_key,
       case when p_mms_package.bk_hash in ('-997','-998','-999') then p_mms_package.bk_hash 
      when l_mms_package.club_id is null then '-998'   
	  when l_mms_package.club_id in (0) then '-998'
	  else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_package.club_id as int) as varchar(500)),'z#@$k%&P'))),2)   end dim_club_key,
       case when p_mms_package.bk_hash in ('-997','-998','-999') then p_mms_package.bk_hash  
     when l_mms_package.member_id is null then '-998'   
	 when l_mms_package.member_id in (0) then '-998'  
     else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_package.member_id as int) as varchar(500)),'z#@$k%&P'))),2)   end dim_mms_member_key,
       case when p_mms_package.bk_hash in ('-997','-998','-999') then p_mms_package.bk_hash
       when l_mms_package.membership_id is null then '-998'
       when l_mms_package.membership_id in (0) then '-998'
       else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_package.membership_id as int) as varchar(500)),'z#@$k%&P'))),2)   end dim_mms_membership_key,
       case when p_mms_package.bk_hash in ('-997','-998','-999') then p_mms_package.bk_hash 
      when l_mms_package.product_id is null then '-998'    
	  when l_mms_package.product_id in (0) then '-998'   
	  else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_package.product_id as int) as varchar(500)),'z#@$k%&P'))),2)   end dim_mms_product_key,
       isnull(l_mms_package.employee_id, -998) employee_id,
       l_mms_package.external_package_id external_package_id,
       case when p_mms_package.bk_hash in ('-997','-998','-999') then p_mms_package.bk_hash 
      when l_mms_package.tran_item_id is null then '-998'   
	  when l_mms_package.tran_item_id in (0) then '-998'   
	  else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_package.tran_item_id as int) as varchar(500)),'z#@$k%&P'))),2)   end fact_mms_sales_transaction_item_key,
       case when p_mms_package.bk_hash in ('-997','-998','-999') then p_mms_package.bk_hash  
     when l_mms_package.mms_tran_id is null then '-998'  
     when l_mms_package.mms_tran_id in (0) then '-998'  
     else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_package.mms_tran_id as int) as varchar(500)),'z#@$k%&P'))),2)   end fact_mms_sales_transaction_key,
       s_mms_package.inserted_date_time inserted_date_time,
       case when p_mms_package.bk_hash in ('-997', '-998', '-999') then p_mms_package.bk_hash 
              when s_mms_package.inserted_date_time is null then '-998'    
              else convert(varchar, s_mms_package.inserted_date_time, 112)   end inserted_dim_date_key,
       case when p_mms_package.bk_hash in ('-997', '-998', '-999') then p_mms_package.bk_hash 
              when s_mms_package.inserted_date_time is null then '-998'    
              else '1' + replace(substring(convert(varchar,s_mms_package.inserted_date_time,114), 1, 5),':','')   end inserted_dim_time_key,
       isnull(l_mms_package.mms_tran_id, -998) mms_tran_id,
       isnull(s_mms_package.number_of_sessions,0) number_of_sessions,
       s_mms_package.package_edit_date_time package_edit_date_time,
       case when p_mms_package.bk_hash in ('-997', '-998', '-999') then p_mms_package.bk_hash 
       when s_mms_package.package_edit_date_time is null then '-998'    
       else convert(varchar, s_mms_package.package_edit_date_time, 112)   end package_edit_dim_date_key,
       case when p_mms_package.bk_hash in ('-997', '-998', '-999') then p_mms_package.bk_hash  
     when s_mms_package.package_edit_date_time is null then '-998'
         else '1' + replace(substring(convert(varchar,s_mms_package.package_edit_date_time,114), 1, 5),':','')   end package_edit_dim_time_key,
       case when s_mms_package.package_edited_flag = 1 then 'Y'
 else 'N'  
 end package_edited_flag,
       case when p_mms_package.bk_hash in ('-997','-998','-999') then p_mms_package.bk_hash   
    when l_mms_package.employee_id is null then '-998'
	when l_mms_package.employee_id in (0) then '-998' 
	else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_package.employee_id as int) as varchar(500)),'z#@$k%&P'))),2)   end package_entered_dim_employee_key,
       case when p_mms_package.bk_hash in ('-997','-998','-999') then p_mms_package.bk_hash   
    when l_mms_package.employee_id is null then '-998'
	when l_mms_package.employee_id in (0) then '-998' 
	else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_package.employee_id as int) as varchar(500)),'z#@$k%&P'))),2)   end package_entered_dim_team_member_key,
       case when p_mms_package.bk_hash in ('-997','-998','-999') then ltrim(rtrim(p_mms_package.bk_hash))
        when l_mms_package.val_package_status_id is null then ltrim(rtrim(('-998')))
        when l_mms_package.val_package_status_id in (0) then ltrim(rtrim(('-998')))  else ltrim(rtrim('r_mms_val_package_status_'+convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_package.val_package_status_id as int) as varchar(500)),'z#@$k%&P'))),2)))  end package_status_dim_description_key,
       isnull(s_mms_package.price_per_session, 0) price_per_session,
       isnull(s_mms_package.sessions_left, -998) sessions_remaining,
       isnull(l_mms_package.tran_item_id, 0) tran_item_id,
       s_mms_package.transaction_source transaction_source,
       s_mms_package.updated_date_time updated_date_time,
       case when p_mms_package.bk_hash in ('-997', '-998', '-999') then p_mms_package.bk_hash 
              when s_mms_package.updated_date_time is null then '-998'    
              else convert(varchar, s_mms_package.updated_date_time, 112)   end updated_dim_date_key,
       case when p_mms_package.bk_hash in ('-997', '-998', '-999') then p_mms_package.bk_hash 
              when s_mms_package.updated_date_time is null then '-998'    
              else '1' + replace(substring(convert(varchar,s_mms_package.updated_date_time,114), 1, 5),':','')   end updated_dim_time_key,
       isnull(l_mms_package.val_package_status_id, -998) val_package_status_id,
       isnull(h_mms_package.dv_deleted,0) dv_deleted,
       p_mms_package.p_mms_package_id,
       p_mms_package.dv_batch_id,
       p_mms_package.dv_load_date_time,
       p_mms_package.dv_load_end_date_time
  from dbo.h_mms_package
  join dbo.p_mms_package
    on h_mms_package.bk_hash = p_mms_package.bk_hash
  join #p_mms_package_insert
    on p_mms_package.bk_hash = #p_mms_package_insert.bk_hash
   and p_mms_package.p_mms_package_id = #p_mms_package_insert.p_mms_package_id
  join dbo.l_mms_package
    on p_mms_package.bk_hash = l_mms_package.bk_hash
   and p_mms_package.l_mms_package_id = l_mms_package.l_mms_package_id
  join dbo.s_mms_package
    on p_mms_package.bk_hash = s_mms_package.bk_hash
   and p_mms_package.s_mms_package_id = s_mms_package.s_mms_package_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_mms_package
   where d_mms_package.bk_hash in (select bk_hash from #p_mms_package_insert)

  insert dbo.d_mms_package(
             bk_hash,
             fact_mms_package_key,
             package_id,
             balance_amount,
             club_id,
             created_date_time,
             created_dim_date_key,
             created_dim_time_key,
             dim_club_key,
             dim_mms_member_key,
             dim_mms_membership_key,
             dim_mms_product_key,
             employee_id,
             external_package_id,
             fact_mms_sales_transaction_item_key,
             fact_mms_sales_transaction_key,
             inserted_date_time,
             inserted_dim_date_key,
             inserted_dim_time_key,
             mms_tran_id,
             number_of_sessions,
             package_edit_date_time,
             package_edit_dim_date_key,
             package_edit_dim_time_key,
             package_edited_flag,
             package_entered_dim_employee_key,
             package_entered_dim_team_member_key,
             package_status_dim_description_key,
             price_per_session,
             sessions_remaining,
             tran_item_id,
             transaction_source,
             updated_date_time,
             updated_dim_date_key,
             updated_dim_time_key,
             val_package_status_id,
             deleted_flag,
             p_mms_package_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         fact_mms_package_key,
         package_id,
         balance_amount,
         club_id,
         created_date_time,
         created_dim_date_key,
         created_dim_time_key,
         dim_club_key,
         dim_mms_member_key,
         dim_mms_membership_key,
         dim_mms_product_key,
         employee_id,
         external_package_id,
         fact_mms_sales_transaction_item_key,
         fact_mms_sales_transaction_key,
         inserted_date_time,
         inserted_dim_date_key,
         inserted_dim_time_key,
         mms_tran_id,
         number_of_sessions,
         package_edit_date_time,
         package_edit_dim_date_key,
         package_edit_dim_time_key,
         package_edited_flag,
         package_entered_dim_employee_key,
         package_entered_dim_team_member_key,
         package_status_dim_description_key,
         price_per_session,
         sessions_remaining,
         tran_item_id,
         transaction_source,
         updated_date_time,
         updated_dim_date_key,
         updated_dim_time_key,
         val_package_status_id,
         dv_deleted,
         p_mms_package_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_mms_package)
--Done!
end
