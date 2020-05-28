CREATE PROC [dbo].[proc_d_chronotrack_event] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_chronotrack_event)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_chronotrack_event_insert') is not null drop table #p_chronotrack_event_insert
create table dbo.#p_chronotrack_event_insert with(distribution=hash(bk_hash), location=user_db) as
select p_chronotrack_event.p_chronotrack_event_id,
       p_chronotrack_event.bk_hash
  from dbo.p_chronotrack_event
 where p_chronotrack_event.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_chronotrack_event.dv_batch_id > @max_dv_batch_id
        or p_chronotrack_event.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_chronotrack_event.bk_hash,
       p_chronotrack_event.event_id event_id,
       case when s_chronotrack_event.check_in = 1 then 'Y' else 'N' end check_in_flag,
       s_chronotrack_event.check_payable check_payable,
       s_chronotrack_event.ctime create_time,
       l_chronotrack_event.currency_id currency_id,
       case when p_chronotrack_event.bk_hash in('-997', '-998', '-999') then p_chronotrack_event.bk_hash
           when l_chronotrack_event.location_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_chronotrack_event.location_id as bigint) as varchar(500)),'z#@$k%&P'))),2)   end d_chronotrack_location_bk_hash,
       s_chronotrack_event.date_format date_format,
       s_chronotrack_event.description description,
       case when s_chronotrack_event.enable_teams = 1 then 'Y' else 'N' end enable_teams_flag,
       s_chronotrack_event.end_datetime end_datetime,
       case when p_chronotrack_event.bk_hash in('-997', '-998', '-999') then p_chronotrack_event.bk_hash
           when s_chronotrack_event.end_datetime is null then '-998'
       	when  convert(varchar, s_chronotrack_event.end_datetime, 112) > 20991231 then '99991231' 
           when convert(varchar, s_chronotrack_event.end_datetime, 112)< 19000101 then '19000101' 
        else convert(varchar, s_chronotrack_event.end_datetime, 112)    end end_dim_date_key,
       case when p_chronotrack_event.bk_hash in ('-997','-998','-999') then p_chronotrack_event.bk_hash
       when s_chronotrack_event.end_datetime is null then '-998'
       else '1' + replace(substring(convert(varchar,s_chronotrack_event.end_datetime,114), 1, 5),':','') end end_dim_time_key,
       s_chronotrack_event.end_time end_time,
       l_chronotrack_event.event_group_id event_group_id,
       s_chronotrack_event.name event_name,
       l_chronotrack_event.external_id external_id,
       case when s_chronotrack_event.is_membership = 1 then 'Y' else 'N' end is_membership_flag,
       case when s_chronotrack_event.is_published_athlinks = 1 then 'Y' else 'N' end is_published_athlinks_flag,
       case when s_chronotrack_event.is_published = 1 then 'Y' else 'N' end is_published_flag,
       case when s_chronotrack_event.is_test_event = 1 then 'Y' else 'N' end is_test_event_flag,
       l_chronotrack_event.language_id language_id,
       case when p_chronotrack_event.bk_hash in('-997', '-998', '-999') then p_chronotrack_event.bk_hash
           when l_chronotrack_event.last_yrs_event_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_chronotrack_event.last_yrs_event_id as bigint) as varchar(500)),'z#@$k%&P'))),2)   end last_yrs_d_chronotrack_event_bk_hash,
       l_chronotrack_event.last_yrs_event_id last_yrs_event_id,
       l_chronotrack_event.location_id location_id,
       s_chronotrack_event.marketing_consent_question marketing_consent_question,
       s_chronotrack_event.marketing_consent_question_text marketing_consent_question_text,
       s_chronotrack_event.marketing_email_question marketing_email_question,
       s_chronotrack_event.marketing_email_question_text marketing_email_question_text,
       s_chronotrack_event.marketing_emails_event_name marketing_emails_event_name,
       s_chronotrack_event.max_races max_races,
       s_chronotrack_event.max_reg_choices max_reg_choices,
       s_chronotrack_event.min_interval_duration min_interval_duration,
       s_chronotrack_event.mtime modified_time,
       s_chronotrack_event.name_prefix name_prefix,
       l_chronotrack_event.online_payee_id online_payee_id,
       l_chronotrack_event.onsite_payee_id onsite_payee_id,
       l_chronotrack_event.organizer_id organizer_id,
       case when p_chronotrack_event.bk_hash in('-997', '-998', '-999') then p_chronotrack_event.bk_hash
           when l_chronotrack_event.parent_event_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_chronotrack_event.parent_event_id as bigint) as varchar(500)),'z#@$k%&P'))),2)   end parent_d_chronotrack_event_bk_hash,
       l_chronotrack_event.parent_event_id parent_event_id,
       s_chronotrack_event.payee payee,
       case when p_chronotrack_event.bk_hash in('-997', '-998', '-999') then p_chronotrack_event.bk_hash
           when l_chronotrack_event.payment_location_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_chronotrack_event.payment_location_id as bigint) as varchar(500)),'z#@$k%&P'))),2)   end payment_d_chronotrack_location_bk_hash,
       l_chronotrack_event.payment_location_id payment_location_id,
       l_chronotrack_event.series_id series_id,
       s_chronotrack_event.start_datetime start_datetime,
       case when p_chronotrack_event.bk_hash in('-997', '-998', '-999') then p_chronotrack_event.bk_hash
           when s_chronotrack_event.start_datetime is null then '-998'
       	when  convert(varchar, s_chronotrack_event.start_datetime, 112) > 20991231 then '99991231' 
           when convert(varchar, s_chronotrack_event.start_datetime, 112)< 19000101 then '19000101' 
        else convert(varchar, s_chronotrack_event.start_datetime, 112)    end start_dim_date_key,
       case when p_chronotrack_event.bk_hash in ('-997','-998','-999') then p_chronotrack_event.bk_hash
       when s_chronotrack_event.start_datetime is null then '-998'
       else '1' + replace(substring(convert(varchar,s_chronotrack_event.start_datetime,114), 1, 5),':','') end start_dim_time_key,
       s_chronotrack_event.start_time start_time,
       s_chronotrack_event.tag tag,
       l_chronotrack_event.timer_id timer_id,
       isnull(h_chronotrack_event.dv_deleted,0) dv_deleted,
       p_chronotrack_event.p_chronotrack_event_id,
       p_chronotrack_event.dv_batch_id,
       p_chronotrack_event.dv_load_date_time,
       p_chronotrack_event.dv_load_end_date_time
  from dbo.h_chronotrack_event
  join dbo.p_chronotrack_event
    on h_chronotrack_event.bk_hash = p_chronotrack_event.bk_hash
  join #p_chronotrack_event_insert
    on p_chronotrack_event.bk_hash = #p_chronotrack_event_insert.bk_hash
   and p_chronotrack_event.p_chronotrack_event_id = #p_chronotrack_event_insert.p_chronotrack_event_id
  join dbo.l_chronotrack_event
    on p_chronotrack_event.bk_hash = l_chronotrack_event.bk_hash
   and p_chronotrack_event.l_chronotrack_event_id = l_chronotrack_event.l_chronotrack_event_id
  join dbo.s_chronotrack_event
    on p_chronotrack_event.bk_hash = s_chronotrack_event.bk_hash
   and p_chronotrack_event.s_chronotrack_event_id = s_chronotrack_event.s_chronotrack_event_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_chronotrack_event
   where d_chronotrack_event.bk_hash in (select bk_hash from #p_chronotrack_event_insert)

  insert dbo.d_chronotrack_event(
             bk_hash,
             event_id,
             check_in_flag,
             check_payable,
             create_time,
             currency_id,
             d_chronotrack_location_bk_hash,
             date_format,
             description,
             enable_teams_flag,
             end_datetime,
             end_dim_date_key,
             end_dim_time_key,
             end_time,
             event_group_id,
             event_name,
             external_id,
             is_membership_flag,
             is_published_athlinks_flag,
             is_published_flag,
             is_test_event_flag,
             language_id,
             last_yrs_d_chronotrack_event_bk_hash,
             last_yrs_event_id,
             location_id,
             marketing_consent_question,
             marketing_consent_question_text,
             marketing_email_question,
             marketing_email_question_text,
             marketing_emails_event_name,
             max_races,
             max_reg_choices,
             min_interval_duration,
             modified_time,
             name_prefix,
             online_payee_id,
             onsite_payee_id,
             organizer_id,
             parent_d_chronotrack_event_bk_hash,
             parent_event_id,
             payee,
             payment_d_chronotrack_location_bk_hash,
             payment_location_id,
             series_id,
             start_datetime,
             start_dim_date_key,
             start_dim_time_key,
             start_time,
             tag,
             timer_id,
             deleted_flag,
             p_chronotrack_event_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         event_id,
         check_in_flag,
         check_payable,
         create_time,
         currency_id,
         d_chronotrack_location_bk_hash,
         date_format,
         description,
         enable_teams_flag,
         end_datetime,
         end_dim_date_key,
         end_dim_time_key,
         end_time,
         event_group_id,
         event_name,
         external_id,
         is_membership_flag,
         is_published_athlinks_flag,
         is_published_flag,
         is_test_event_flag,
         language_id,
         last_yrs_d_chronotrack_event_bk_hash,
         last_yrs_event_id,
         location_id,
         marketing_consent_question,
         marketing_consent_question_text,
         marketing_email_question,
         marketing_email_question_text,
         marketing_emails_event_name,
         max_races,
         max_reg_choices,
         min_interval_duration,
         modified_time,
         name_prefix,
         online_payee_id,
         onsite_payee_id,
         organizer_id,
         parent_d_chronotrack_event_bk_hash,
         parent_event_id,
         payee,
         payment_d_chronotrack_location_bk_hash,
         payment_location_id,
         series_id,
         start_datetime,
         start_dim_date_key,
         start_dim_time_key,
         start_time,
         tag,
         timer_id,
         dv_deleted,
         p_chronotrack_event_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_chronotrack_event)
--Done!
end
