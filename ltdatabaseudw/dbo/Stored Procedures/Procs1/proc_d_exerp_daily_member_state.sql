CREATE PROC [dbo].[proc_d_exerp_daily_member_state] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_exerp_daily_member_state)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_exerp_daily_member_state_insert') is not null drop table #p_exerp_daily_member_state_insert
create table dbo.#p_exerp_daily_member_state_insert with(distribution=hash(bk_hash), location=user_db) as
select p_exerp_daily_member_state.p_exerp_daily_member_state_id,
       p_exerp_daily_member_state.bk_hash
  from dbo.p_exerp_daily_member_state
 where p_exerp_daily_member_state.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_exerp_daily_member_state.dv_batch_id > @max_dv_batch_id
        or p_exerp_daily_member_state.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_exerp_daily_member_state.bk_hash,
       p_exerp_daily_member_state.daily_member_state_id daily_member_state_id,
       case when p_exerp_daily_member_state.bk_hash in('-997', '-998', '-999') then p_exerp_daily_member_state.bk_hash
            when s_exerp_daily_member_state.cancel_datetime is null then '-998'
         else convert(varchar, s_exerp_daily_member_state.cancel_datetime, 112)    end cancel_dim_date_key,
       case when p_exerp_daily_member_state.bk_hash in ('-997','-998','-999') then p_exerp_daily_member_state.bk_hash
        when s_exerp_daily_member_state.cancel_datetime is null then '-998'
        else '1' + replace(substring(convert(varchar,s_exerp_daily_member_state.cancel_datetime,114), 1, 5),':','') end cancel_dim_time_key,
       l_exerp_daily_member_state.center_id center_id,
       case when p_exerp_daily_member_state.bk_hash in('-997', '-998', '-999') then p_exerp_daily_member_state.bk_hash
            when l_exerp_daily_member_state.center_id is null then '-998'
         else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_exerp_daily_member_state.center_id as int) as varchar(500)),'z#@$k%&P'))),2)   end d_exerp_center_bk_hash,
       s_exerp_daily_member_state.change daily_member_state_change,
       case when p_exerp_daily_member_state.bk_hash in('-997', '-998', '-999') then p_exerp_daily_member_state.bk_hash
            when s_exerp_daily_member_state.date is null then '-998'
         else convert(varchar, s_exerp_daily_member_state.date, 112)    end daily_member_state_date_dim_date_key,
       case when p_exerp_daily_member_state.bk_hash in ('-997','-998','-999') then p_exerp_daily_member_state.bk_hash
        when s_exerp_daily_member_state.date is null then '-998'
        else '1' + replace(substring(convert(varchar,s_exerp_daily_member_state.date,114), 1, 5),':','') end daily_member_state_date_dim_time_key,
       case when p_exerp_daily_member_state.bk_hash in('-997', '-998', '-999') then p_exerp_daily_member_state.bk_hash 
              when ((l_exerp_daily_member_state.person_id is null) OR (l_exerp_daily_member_state.person_id LIKE '%e%') or (l_exerp_daily_member_state.person_id LIKE '%OLDe%') or (len(l_exerp_daily_member_state.person_id) > 9) or (d_exerp_person.person_type = 'STAFF' and l_exerp_daily_member_state.person_id not LIKE '%e%') or (d_exerp_person.person_type = 'STAFF')  or (isnumeric(l_exerp_daily_member_state.person_id) = 0)) then '-998' 
       		       else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_exerp_daily_member_state.person_id as int) as varchar(500)),'z#@$k%&P'))),2) end dim_mms_member_key,
       case when p_exerp_daily_member_state.bk_hash in('-997', '-998', '-999') then p_exerp_daily_member_state.bk_hash
            when s_exerp_daily_member_state.entry_datetime is null then '-998'
         else convert(varchar, s_exerp_daily_member_state.entry_datetime, 112)    end entry_dim_date_key,
       case when p_exerp_daily_member_state.bk_hash in ('-997','-998','-999') then p_exerp_daily_member_state.bk_hash
        when s_exerp_daily_member_state.entry_datetime is null then '-998'
        else '1' + replace(substring(convert(varchar,s_exerp_daily_member_state.entry_datetime,114), 1, 5),':','') end entry_dim_time_key,
       s_exerp_daily_member_state.ets ets,
       s_exerp_daily_member_state.extra_number_delta extra_number_delta,
       l_exerp_daily_member_state.home_center_person_id home_center_person_id,
       s_exerp_daily_member_state.member_number_delta member_number_delta,
       l_exerp_daily_member_state.person_id person_id,
       s_exerp_daily_member_state.secondary_member_number_delta secondary_member_number_delta,
       isnull(h_exerp_daily_member_state.dv_deleted,0) dv_deleted,
       p_exerp_daily_member_state.p_exerp_daily_member_state_id,
       p_exerp_daily_member_state.dv_batch_id,
       p_exerp_daily_member_state.dv_load_date_time,
       p_exerp_daily_member_state.dv_load_end_date_time
  from dbo.h_exerp_daily_member_state
  join dbo.p_exerp_daily_member_state
    on h_exerp_daily_member_state.bk_hash = p_exerp_daily_member_state.bk_hash
  join #p_exerp_daily_member_state_insert
    on p_exerp_daily_member_state.bk_hash = #p_exerp_daily_member_state_insert.bk_hash
   and p_exerp_daily_member_state.p_exerp_daily_member_state_id = #p_exerp_daily_member_state_insert.p_exerp_daily_member_state_id
  join dbo.l_exerp_daily_member_state
    on p_exerp_daily_member_state.bk_hash = l_exerp_daily_member_state.bk_hash
   and p_exerp_daily_member_state.l_exerp_daily_member_state_id = l_exerp_daily_member_state.l_exerp_daily_member_state_id
  join dbo.s_exerp_daily_member_state
    on p_exerp_daily_member_state.bk_hash = s_exerp_daily_member_state.bk_hash
   and p_exerp_daily_member_state.s_exerp_daily_member_state_id = s_exerp_daily_member_state.s_exerp_daily_member_state_id
 left join 	d_exerp_person		on l_exerp_daily_member_state.person_id = d_exerp_person.person_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_exerp_daily_member_state
   where d_exerp_daily_member_state.bk_hash in (select bk_hash from #p_exerp_daily_member_state_insert)

  insert dbo.d_exerp_daily_member_state(
             bk_hash,
             daily_member_state_id,
             cancel_dim_date_key,
             cancel_dim_time_key,
             center_id,
             d_exerp_center_bk_hash,
             daily_member_state_change,
             daily_member_state_date_dim_date_key,
             daily_member_state_date_dim_time_key,
             dim_mms_member_key,
             entry_dim_date_key,
             entry_dim_time_key,
             ets,
             extra_number_delta,
             home_center_person_id,
             member_number_delta,
             person_id,
             secondary_member_number_delta,
             deleted_flag,
             p_exerp_daily_member_state_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         daily_member_state_id,
         cancel_dim_date_key,
         cancel_dim_time_key,
         center_id,
         d_exerp_center_bk_hash,
         daily_member_state_change,
         daily_member_state_date_dim_date_key,
         daily_member_state_date_dim_time_key,
         dim_mms_member_key,
         entry_dim_date_key,
         entry_dim_time_key,
         ets,
         extra_number_delta,
         home_center_person_id,
         member_number_delta,
         person_id,
         secondary_member_number_delta,
         dv_deleted,
         p_exerp_daily_member_state_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_exerp_daily_member_state)
--Done!
end
