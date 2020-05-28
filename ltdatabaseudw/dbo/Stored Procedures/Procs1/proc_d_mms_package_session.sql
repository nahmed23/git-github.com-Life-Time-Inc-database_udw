CREATE PROC [dbo].[proc_d_mms_package_session] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_mms_package_session)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_mms_package_session_insert') is not null drop table #p_mms_package_session_insert
create table dbo.#p_mms_package_session_insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_package_session.p_mms_package_session_id,
       p_mms_package_session.bk_hash
  from dbo.p_mms_package_session
 where p_mms_package_session.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_mms_package_session.dv_batch_id > @max_dv_batch_id
        or p_mms_package_session.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_package_session.bk_hash,
       p_mms_package_session.bk_hash fact_mms_package_session_key,
       p_mms_package_session.package_session_id package_session_id,
       isnull(s_mms_package_session.comment,'') comment,
       case when p_mms_package_session.bk_hash in ('-997', '-998', '-999') then p_mms_package_session.bk_hash   
    when s_mms_package_session.created_date_time is null then '-998'   
	 else convert(char(8), s_mms_package_session.created_date_time, 112)   end created_dim_date_key,
       case when p_mms_package_session.bk_hash in ('-997', '-998', '-999') then p_mms_package_session.bk_hash   
    when s_mms_package_session.created_date_time is null then '-998'   
	 else '1' + replace(substring(convert(varchar,s_mms_package_session.created_date_time,114), 1, 5),':','')     end created_dim_time_key,
       case when p_mms_package_session.bk_hash in ('-997','-998','-999') then p_mms_package_session.bk_hash    
   when l_mms_package_session.club_id is null then '-998' 
   else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_mms_package_session.club_id as varchar(500)),'z#@$k%&P'))),2)   end delivered_dim_club_key,
       case when p_mms_package_session.bk_hash in ('-997', '-998', '-999') then p_mms_package_session.bk_hash   
    when s_mms_package_session.delivered_date_time is null then '-998'   
	 else convert(char(8), s_mms_package_session.delivered_date_time, 112)   end delivered_dim_date_key,
       case when p_mms_package_session.bk_hash in ('-997','-998','-999') then p_mms_package_session.bk_hash  
     when l_mms_package_session.delivered_employee_id is null then '-998'  
	 else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_mms_package_session.delivered_employee_id as varchar(500)),'z#@$k%&P'))),2)   end delivered_dim_employee_key,
       case when p_mms_package_session.bk_hash in ('-997','-998','-999') then p_mms_package_session.bk_hash  
     when l_mms_package_session.delivered_employee_id is null then '-998'  
	 else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_mms_package_session.delivered_employee_id as varchar(500)),'z#@$k%&P'))),2) end delivered_dim_team_member_key,
       case when p_mms_package_session.bk_hash in ('-997', '-998', '-999') then p_mms_package_session.bk_hash   
    when s_mms_package_session.delivered_date_time is null then '-998'   
	 else '1' + replace(substring(convert(varchar,s_mms_package_session.delivered_date_time,114), 1, 5),':','')  end delivered_dim_time_key,
        isnull(s_mms_package_session.session_price,0) delivered_session_price,
       case when p_mms_package_session.bk_hash in ('-997','-998','-999') then p_mms_package_session.bk_hash     
  when l_mms_package_session.package_id is null then '-998'   
  else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_mms_package_session.package_id as varchar(500)),'z#@$k%&P'))),2)   end fact_mms_package_key,
       l_mms_package_session.package_id package_id,
       case when p_mms_package_session.bk_hash in ('-997','-998','-999') then p_mms_package_session.bk_hash  
     when l_mms_package_session.club_id is null then '-998'  
	 else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_mms_package_session.club_id as varchar(500)),'z#@$k%&P'))),2)   end package_session_club_key,
       p_mms_package_session.p_mms_package_session_id,
       p_mms_package_session.dv_batch_id,
       p_mms_package_session.dv_load_date_time,
       p_mms_package_session.dv_load_end_date_time
  from dbo.h_mms_package_session
  join dbo.p_mms_package_session
    on h_mms_package_session.bk_hash = p_mms_package_session.bk_hash  join #p_mms_package_session_insert
    on p_mms_package_session.bk_hash = #p_mms_package_session_insert.bk_hash
   and p_mms_package_session.p_mms_package_session_id = #p_mms_package_session_insert.p_mms_package_session_id
  join dbo.l_mms_package_session
    on p_mms_package_session.bk_hash = l_mms_package_session.bk_hash
   and p_mms_package_session.l_mms_package_session_id = l_mms_package_session.l_mms_package_session_id
  join dbo.s_mms_package_session
    on p_mms_package_session.bk_hash = s_mms_package_session.bk_hash
   and p_mms_package_session.s_mms_package_session_id = s_mms_package_session.s_mms_package_session_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_mms_package_session
   where d_mms_package_session.bk_hash in (select bk_hash from #p_mms_package_session_insert)

  insert dbo.d_mms_package_session(
             bk_hash,
             fact_mms_package_session_key,
             package_session_id,
             comment,
             created_dim_date_key,
             created_dim_time_key,
             delivered_dim_club_key,
             delivered_dim_date_key,
             delivered_dim_employee_key,
             delivered_dim_team_member_key,
             delivered_dim_time_key,
             delivered_session_price,
             fact_mms_package_key,
             package_id,
             package_session_club_key,
             p_mms_package_session_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         fact_mms_package_session_key,
         package_session_id,
         comment,
         created_dim_date_key,
         created_dim_time_key,
         delivered_dim_club_key,
         delivered_dim_date_key,
         delivered_dim_employee_key,
         delivered_dim_team_member_key,
         delivered_dim_time_key,
         delivered_session_price,
         fact_mms_package_key,
         package_id,
         package_session_club_key,
         p_mms_package_session_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_mms_package_session)
--Done!
end
