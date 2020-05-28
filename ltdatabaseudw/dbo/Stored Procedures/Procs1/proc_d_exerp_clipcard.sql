CREATE PROC [dbo].[proc_d_exerp_clipcard] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_exerp_clipcard)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_exerp_clipcard_insert') is not null drop table #p_exerp_clipcard_insert
create table dbo.#p_exerp_clipcard_insert with(distribution=hash(bk_hash), location=user_db) as
select p_exerp_clipcard.p_exerp_clipcard_id,
       p_exerp_clipcard.bk_hash
  from dbo.p_exerp_clipcard
 where p_exerp_clipcard.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_exerp_clipcard.dv_batch_id > @max_dv_batch_id
        or p_exerp_clipcard.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_exerp_clipcard.bk_hash,
       p_exerp_clipcard.clipcard_id clipcard_id,
       case when p_exerp_clipcard.bk_hash in('-997', '-998', '-999') then p_exerp_clipcard.bk_hash      
       when l_exerp_clipcard.assigned_person_id is null then '-998'   
       else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_exerp_clipcard.assigned_person_id as varchar(4000)),'z#@$k%&P'))),2)   end assigned_dim_employee_key,
       case when s_exerp_clipcard.blocked = 1 then 'Y'        else 'N'  end blocked_flag,
       case when p_exerp_clipcard.bk_hash in('-997', '-998', '-999') then p_exerp_clipcard.bk_hash
            when s_exerp_clipcard.cancel_datetime is null then '-998'
            else convert(varchar, s_exerp_clipcard.cancel_datetime, 112)  end cancel_dim_date_key,
       case when p_exerp_clipcard.bk_hash in ('-997','-998','-999') then p_exerp_clipcard.bk_hash
       when s_exerp_clipcard.cancel_datetime is null then '-998'
       else '1' + replace(substring(convert(varchar,s_exerp_clipcard.cancel_datetime,114), 1, 5),':','')  end cancel_dim_time_key,
       case when s_exerp_clipcard.cancelled = 1 then 'Y'        else 'N'  end cancelled_flag,
       s_exerp_clipcard.clips_initial clipcard_clips_initial,
       s_exerp_clipcard.clips_left clipcard_clips_left,
       l_exerp_clipcard_1.comment comment,
       case when p_exerp_clipcard.bk_hash in ('-997','-998','-999') then p_exerp_clipcard.bk_hash         
        when l_exerp_clipcard.center_id is null then '-998'      
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_exerp_clipcard.center_id as int) as varchar(500)),'z#@$k%&P'))),2)   end dim_club_key,
       case when p_exerp_clipcard.bk_hash in('-997', '-998', '-999') then p_exerp_clipcard.bk_hash 
              when ((l_exerp_clipcard.person_id is null) OR (l_exerp_clipcard.person_id LIKE '%e%') or (l_exerp_clipcard.person_id LIKE '%OLDe%')
       	    or (len(l_exerp_clipcard.person_id) > 9)  or (d_exerp_person.person_type = 'STAFF' and l_exerp_clipcard.person_id not LIKE '%e%') 
       		  or (d_exerp_person.person_type = 'STAFF') or (isnumeric(l_exerp_clipcard.person_id) = 0)) then '-998' 
       		       else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_exerp_clipcard.person_id as int) as varchar(500)),'z#@$k%&P'))),2) end dim_mms_member_key,
       s_exerp_clipcard.ets ets,
       case when p_exerp_clipcard.bk_hash in('-997', '-998', '-999') then p_exerp_clipcard.bk_hash
        when l_exerp_clipcard.sale_log_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_exerp_clipcard.sale_log_id as varchar(4000)),'z#@$k%&P'))),2)  end fact_exerp_transaction_log_key,
       case when p_exerp_clipcard.bk_hash in('-997', '-998', '-999') then p_exerp_clipcard.bk_hash
            when s_exerp_clipcard.valid_from_datetime is null then '-998'
            else convert(varchar, s_exerp_clipcard.valid_from_datetime, 112)  end valid_from_dim_date_key,
       case when p_exerp_clipcard.bk_hash in ('-997','-998','-999') then p_exerp_clipcard.bk_hash
            when s_exerp_clipcard.valid_from_datetime is null then '-998'
            else '1' + replace(substring(convert(varchar,s_exerp_clipcard.valid_from_datetime,114), 1, 5),':','')
        end valid_from_dim_time_key,
       case when p_exerp_clipcard.bk_hash in('-997', '-998', '-999') then p_exerp_clipcard.bk_hash
            when s_exerp_clipcard.valid_until_datetime is null then '-998'
            else convert(varchar, s_exerp_clipcard.valid_until_datetime, 112)
        end valid_until_dim_date_key,
       case when p_exerp_clipcard.bk_hash in ('-997','-998','-999') then p_exerp_clipcard.bk_hash
            when s_exerp_clipcard.valid_until_datetime is null then '-998'
            else '1' + replace(substring(convert(varchar,s_exerp_clipcard.valid_until_datetime,114), 1, 5),':','')
        end valid_until_dim_time_key,
       isnull(h_exerp_clipcard.dv_deleted,0) dv_deleted,
       p_exerp_clipcard.p_exerp_clipcard_id,
       p_exerp_clipcard.dv_batch_id,
       p_exerp_clipcard.dv_load_date_time,
       p_exerp_clipcard.dv_load_end_date_time
  from dbo.h_exerp_clipcard
  join dbo.p_exerp_clipcard
    on h_exerp_clipcard.bk_hash = p_exerp_clipcard.bk_hash
  join #p_exerp_clipcard_insert
    on p_exerp_clipcard.bk_hash = #p_exerp_clipcard_insert.bk_hash
   and p_exerp_clipcard.p_exerp_clipcard_id = #p_exerp_clipcard_insert.p_exerp_clipcard_id
  join dbo.l_exerp_clipcard
    on p_exerp_clipcard.bk_hash = l_exerp_clipcard.bk_hash
   and p_exerp_clipcard.l_exerp_clipcard_id = l_exerp_clipcard.l_exerp_clipcard_id
  join dbo.l_exerp_clipcard_1
    on p_exerp_clipcard.bk_hash = l_exerp_clipcard_1.bk_hash
   and p_exerp_clipcard.l_exerp_clipcard_1_id = l_exerp_clipcard_1.l_exerp_clipcard_1_id
  join dbo.s_exerp_clipcard
    on p_exerp_clipcard.bk_hash = s_exerp_clipcard.bk_hash
   and p_exerp_clipcard.s_exerp_clipcard_id = s_exerp_clipcard.s_exerp_clipcard_id
 left join 	d_exerp_person		on l_exerp_clipcard.person_id = d_exerp_person.person_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_exerp_clipcard
   where d_exerp_clipcard.bk_hash in (select bk_hash from #p_exerp_clipcard_insert)

  insert dbo.d_exerp_clipcard(
             bk_hash,
             clipcard_id,
             assigned_dim_employee_key,
             blocked_flag,
             cancel_dim_date_key,
             cancel_dim_time_key,
             cancelled_flag,
             clipcard_clips_initial,
             clipcard_clips_left,
             comment,
             dim_club_key,
             dim_mms_member_key,
             ets,
             fact_exerp_transaction_log_key,
             valid_from_dim_date_key,
             valid_from_dim_time_key,
             valid_until_dim_date_key,
             valid_until_dim_time_key,
             deleted_flag,
             p_exerp_clipcard_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         clipcard_id,
         assigned_dim_employee_key,
         blocked_flag,
         cancel_dim_date_key,
         cancel_dim_time_key,
         cancelled_flag,
         clipcard_clips_initial,
         clipcard_clips_left,
         comment,
         dim_club_key,
         dim_mms_member_key,
         ets,
         fact_exerp_transaction_log_key,
         valid_from_dim_date_key,
         valid_from_dim_time_key,
         valid_until_dim_date_key,
         valid_until_dim_time_key,
         dv_deleted,
         p_exerp_clipcard_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_exerp_clipcard)
--Done!
end
