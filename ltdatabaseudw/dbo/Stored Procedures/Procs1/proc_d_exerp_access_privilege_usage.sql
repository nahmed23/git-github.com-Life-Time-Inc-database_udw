CREATE PROC [dbo].[proc_d_exerp_access_privilege_usage] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_exerp_access_privilege_usage)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_exerp_access_privilege_usage_insert') is not null drop table #p_exerp_access_privilege_usage_insert
create table dbo.#p_exerp_access_privilege_usage_insert with(distribution=hash(bk_hash), location=user_db) as
select p_exerp_access_privilege_usage.p_exerp_access_privilege_usage_id,
       p_exerp_access_privilege_usage.bk_hash
  from dbo.p_exerp_access_privilege_usage
 where p_exerp_access_privilege_usage.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_exerp_access_privilege_usage.dv_batch_id > @max_dv_batch_id
        or p_exerp_access_privilege_usage.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_exerp_access_privilege_usage.bk_hash,
       p_exerp_access_privilege_usage.bk_hash dim_exerp_access_privilege_usage_key,
       p_exerp_access_privilege_usage.access_privilege_usage_id access_privilege_usage_id,
       isnull(s_exerp_access_privilege_usage.state, '') access_privilege_usage_state,
       case when p_exerp_access_privilege_usage.bk_hash in('-997','-998','-999') then p_exerp_access_privilege_usage.bk_hash
    when l_exerp_access_privilege_usage.deduction_key is null then '-998'
	when isnumeric(l_exerp_access_privilege_usage.deduction_key)=1 then
	convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_exerp_access_privilege_usage.deduction_key as int) as varchar(500)),'z#@$k%&P'))),2) else '-998'   end deduction_fact_exerp_clipcard_usage_key,
       case when p_exerp_access_privilege_usage.bk_hash in('-997','-998','-999') then p_exerp_access_privilege_usage.bk_hash
    when l_exerp_access_privilege_usage.deduction_key is null then '-998'
	when  l_exerp_access_privilege_usage.deduction_key like '%inv%' then
	convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_exerp_access_privilege_usage.deduction_key as varchar(4000)),'z#@$k%&P'))),2) else '-998'   end deduction_fact_exerp_transaction_log_key,
       l_exerp_access_privilege_usage.deduction_key deduction_key,
       case when p_exerp_access_privilege_usage.bk_hash in ('-997','-998','-999') then p_exerp_access_privilege_usage.bk_hash     
            when l_exerp_access_privilege_usage.center_id is null then '-998'   
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_exerp_access_privilege_usage.center_id as int) as varchar(500)),'z#@$k%&P'))),2)
        end dim_club_key,
       s_exerp_access_privilege_usage.ets ets,
       case when p_exerp_access_privilege_usage.bk_hash in('-997','-998','-999') then p_exerp_access_privilege_usage.bk_hash
    when l_exerp_access_privilege_usage.punishment_key is null then '-998'
	when isnumeric(l_exerp_access_privilege_usage.punishment_key)=1 then
	convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_exerp_access_privilege_usage.punishment_key as int) as varchar(500)),'z#@$k%&P'))),2) else '-998'   end punishment_fact_exerp_clipcard_usage_key,
       case when p_exerp_access_privilege_usage.bk_hash in('-997','-998','-999') then p_exerp_access_privilege_usage.bk_hash
    when l_exerp_access_privilege_usage.punishment_key is null then '-998'
	when  l_exerp_access_privilege_usage.punishment_key like '%inv%' then
	convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_exerp_access_privilege_usage.punishment_key as varchar(4000)),'z#@$k%&P'))),2) else '-998'   end punishment_fact_exerp_transaction_log_key,
       l_exerp_access_privilege_usage.punishment_key punishment_key,
       case when p_exerp_access_privilege_usage.bk_hash in('-997','-998','-999') then p_exerp_access_privilege_usage.bk_hash
    when l_exerp_access_privilege_usage.source_id is null then '-998'
	when  s_exerp_access_privilege_usage.source_type = 'CLIPCARD' then
	convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_exerp_access_privilege_usage.source_id as varchar(4000)),'z#@$k%&P'))),2) else '-998'   end source_dim_exerp_clipcard_key,
       case when p_exerp_access_privilege_usage.bk_hash in('-997','-998','-999') then p_exerp_access_privilege_usage.bk_hash
    when l_exerp_access_privilege_usage.source_id is null then '-998'
	when  s_exerp_access_privilege_usage.source_type = 'SUBSCRIPTION_ADDON' and isnumeric(l_exerp_access_privilege_usage.source_id)=1 then
	convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_exerp_access_privilege_usage.source_id as int) as varchar(500)),'z#@$k%&P'))),2) else '-998'   end source_dim_exerp_subscription_addon_key,
       case when p_exerp_access_privilege_usage.bk_hash in('-997','-998','-999') then p_exerp_access_privilege_usage.bk_hash
    when l_exerp_access_privilege_usage.source_id is null then '-998'
	when  s_exerp_access_privilege_usage.source_type = 'SUBSCRIPTION' then
	convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_exerp_access_privilege_usage.source_id as varchar(4000)),'z#@$k%&P'))),2) else '-998'   end source_dim_exerp_subscription_key,
       case when p_exerp_access_privilege_usage.bk_hash in('-997','-998','-999') then p_exerp_access_privilege_usage.bk_hash
    when l_exerp_access_privilege_usage.source_id is null then '-998'
	when  s_exerp_access_privilege_usage.source_type = 'ACCESS_PRODUCT' then
	convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_exerp_access_privilege_usage.source_id as varchar(4000)),'z#@$k%&P'))),2) else '-998'   end source_fact_exerp_transaction_log_key,
       l_exerp_access_privilege_usage.source_id source_id,
       isnull(s_exerp_access_privilege_usage.source_type, '') source_type,
       case when p_exerp_access_privilege_usage.bk_hash in('-997','-998','-999') then p_exerp_access_privilege_usage.bk_hash
    when l_exerp_access_privilege_usage.target_id is null then '-998'
	when  s_exerp_access_privilege_usage.target_type = 'BOOKING' then
	convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_exerp_access_privilege_usage.target_id as varchar(4000)),'z#@$k%&P'))),2) else '-998'   end target_dim_exerp_booking_key,
       case when p_exerp_access_privilege_usage.bk_hash in('-997','-998','-999') then p_exerp_access_privilege_usage.bk_hash
    when l_exerp_access_privilege_usage.target_id is null then '-998'
	when  s_exerp_access_privilege_usage.target_type = 'ATTEND' then
	convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_exerp_access_privilege_usage.target_id as varchar(4000)),'z#@$k%&P'))),2) else '-998'   end target_fact_exerp_attend_key,
       case when p_exerp_access_privilege_usage.bk_hash in('-997','-998','-999') then p_exerp_access_privilege_usage.bk_hash
    when l_exerp_access_privilege_usage.target_id is null then '-998'
	when  s_exerp_access_privilege_usage.target_type = 'PARTICIPATION' then
	convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_exerp_access_privilege_usage.target_id as varchar(4000)),'z#@$k%&P'))),2) else '-998'   end target_fact_exerp_participation_key,
       l_exerp_access_privilege_usage.target_id target_id,
       isnull(s_exerp_access_privilege_usage.target_type, '') target_type,
       isnull(h_exerp_access_privilege_usage.dv_deleted,0) dv_deleted,
       p_exerp_access_privilege_usage.p_exerp_access_privilege_usage_id,
       p_exerp_access_privilege_usage.dv_batch_id,
       p_exerp_access_privilege_usage.dv_load_date_time,
       p_exerp_access_privilege_usage.dv_load_end_date_time
  from dbo.h_exerp_access_privilege_usage
  join dbo.p_exerp_access_privilege_usage
    on h_exerp_access_privilege_usage.bk_hash = p_exerp_access_privilege_usage.bk_hash
  join #p_exerp_access_privilege_usage_insert
    on p_exerp_access_privilege_usage.bk_hash = #p_exerp_access_privilege_usage_insert.bk_hash
   and p_exerp_access_privilege_usage.p_exerp_access_privilege_usage_id = #p_exerp_access_privilege_usage_insert.p_exerp_access_privilege_usage_id
  join dbo.l_exerp_access_privilege_usage
    on p_exerp_access_privilege_usage.bk_hash = l_exerp_access_privilege_usage.bk_hash
   and p_exerp_access_privilege_usage.l_exerp_access_privilege_usage_id = l_exerp_access_privilege_usage.l_exerp_access_privilege_usage_id
  join dbo.s_exerp_access_privilege_usage
    on p_exerp_access_privilege_usage.bk_hash = s_exerp_access_privilege_usage.bk_hash
   and p_exerp_access_privilege_usage.s_exerp_access_privilege_usage_id = s_exerp_access_privilege_usage.s_exerp_access_privilege_usage_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_exerp_access_privilege_usage
   where d_exerp_access_privilege_usage.bk_hash in (select bk_hash from #p_exerp_access_privilege_usage_insert)

  insert dbo.d_exerp_access_privilege_usage(
             bk_hash,
             dim_exerp_access_privilege_usage_key,
             access_privilege_usage_id,
             access_privilege_usage_state,
             deduction_fact_exerp_clipcard_usage_key,
             deduction_fact_exerp_transaction_log_key,
             deduction_key,
             dim_club_key,
             ets,
             punishment_fact_exerp_clipcard_usage_key,
             punishment_fact_exerp_transaction_log_key,
             punishment_key,
             source_dim_exerp_clipcard_key,
             source_dim_exerp_subscription_addon_key,
             source_dim_exerp_subscription_key,
             source_fact_exerp_transaction_log_key,
             source_id,
             source_type,
             target_dim_exerp_booking_key,
             target_fact_exerp_attend_key,
             target_fact_exerp_participation_key,
             target_id,
             target_type,
             deleted_flag,
             p_exerp_access_privilege_usage_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         dim_exerp_access_privilege_usage_key,
         access_privilege_usage_id,
         access_privilege_usage_state,
         deduction_fact_exerp_clipcard_usage_key,
         deduction_fact_exerp_transaction_log_key,
         deduction_key,
         dim_club_key,
         ets,
         punishment_fact_exerp_clipcard_usage_key,
         punishment_fact_exerp_transaction_log_key,
         punishment_key,
         source_dim_exerp_clipcard_key,
         source_dim_exerp_subscription_addon_key,
         source_dim_exerp_subscription_key,
         source_fact_exerp_transaction_log_key,
         source_id,
         source_type,
         target_dim_exerp_booking_key,
         target_fact_exerp_attend_key,
         target_fact_exerp_participation_key,
         target_id,
         target_type,
         dv_deleted,
         p_exerp_access_privilege_usage_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_exerp_access_privilege_usage)
--Done!
end
