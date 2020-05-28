CREATE PROC [dbo].[proc_load_history_mms_membership_communication_preference] AS
begin

set nocount on
set xact_abort on

/*
-- prepare the dv tables
truncate table dbo.h_mms_membership_communication_preference
truncate table dbo.l_mms_membership_communication_preference
truncate table dbo.s_mms_membership_communication_preference
truncate table dbo.p_mms_membership_communication_preference
truncate table dbo.stage_mms_MembershipCommunicationPreference


exec proc_util_create_base_records @table_name = 'h_mms_membership_communication_preference'
exec proc_util_create_base_records @table_name = 'l_mms_membership_communication_preference'
exec proc_util_create_base_records @table_name = 's_mms_membership_communication_preference'
exec proc_p_mms_membership_communication_preference @current_dv_batch_id = -1
go
select getdate()
*/

--Select the records from MMSMembershipCommunicationPreference to be staged and inserted into the dv tables
--We only want 1 record per MMSMembershipCommunicationPreference for any particular timestamp
--  Do row_number ranking
--    Partition by the following
--      MembershipCommunicationPreferenceID
--      Calculated update_insert_date
--        If MMSUpdatedDateTime is null then MMSInsertedDateTime
--        Else if the hour portion of MMSUpdatedDateTime is 0 or 1 then add 3 hours to the date portion of MMSUpdatedDateTime
--        Else add 27 hours to the date portion of MMSUpdatedDateTime
--    Order by MembershipCommunicationPreferenceKey in descending order (this keeps the most recent record where one or more are in LTFDWStg with the same date)
--  Only keep the records with rank = 1

if object_id('tempdb.dbo.#stage_mms_MembershipCommunicationPreference') is not null drop table #stage_mms_MembershipCommunicationPreference
create table dbo.#stage_mms_MembershipCommunicationPreference with (location=user_db, distribution = hash(MembershipCommunicationPreferenceID)) as
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(x.MembershipCommunicationPreferenceID as varchar(500)),'z#@$k%&P'))
                                  ),2
               ) bk_hash,
       x.*,
       row_number() over(partition by MembershipCommunicationPreferenceID order by x.update_insert_date) rank2,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(x.MembershipCommunicationPreferenceID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(x.MembershipID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(x.ValCommunicationPreferenceID as varchar(500)),'z#@$k%&P'))
                                  ),2
               ) l_mms_membership_communication_preference_hash,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(x.MembershipCommunicationPreferenceID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(x.ActiveFlag as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,x.MMSInsertedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,x.MMSUpdatedDateTime,120),'z#@$k%&P'))
                                  ),2
               ) s_mms_membership_communication_preference_hash
  from (select row_number() over(partition by MembershipCommunicationPreferenceID,
                                              case when MMSUpdatedDateTime is null then MMSInsertedDateTime
                                                   when datepart(hh, MMSUpdatedDateTime) in (0, 1) then dateadd(hh, 3, convert(datetime, convert(date, MMSUpdatedDateTime)))
                                                   else dateadd(hh, 27, convert(datetime, convert(date, MMSUpdatedDateTime)))
                                               end
                                 order by MMSMembershipCommunicationPreferenceKey desc) rank1,
              case when MMSUpdatedDateTime is null then MMSInsertedDateTime
                   when datepart(hh, MMSUpdatedDateTime) in (0, 1) then dateadd(hh, 3, convert(datetime, convert(date, MMSUpdatedDateTime)))
                   else dateadd(hh, 27, convert(datetime, convert(date, MMSUpdatedDateTime)))
               end update_insert_date,
               *
          from MMSMembershipCommunicationPreference) x
 where rank1 = 1
--go
--select getdate()   	----commenting out as we are converting into stored procedure

/*
select * from h_mms_membership_communication_preference
select * from l_mms_membership_communication_preference
select * from s_mms_membership_communication_preference
select * from p_mms_membership_communication_preference
*/

-- Create the h records.
-- Only use records where rank2 = 1 (first record in a series: min(update_insert_date))
-- dv_load_date_time is the MMSInsertedDateTime or Jan 1, 1980 if MMSInsertedDateTime is null.
-- dv_batch_id is the dv_load_date_time converted to YYYYMMDDHHMISS 

insert into dbo.h_mms_membership_communication_preference(
       h_mms_membership_communication_preference_id, 
       bk_hash,
       membership_communication_preference_id, 
       dv_load_date_time, 
       dv_batch_id, 
       dv_r_load_source_id, 
       dv_inserted_date_time, 
       dv_insert_user)
select row_number() over(order by x.dv_batch_id, x.membership_communication_preference_id) h_mms_membership_communication_preference_id,
       x.*
  from (select bk_hash,
               MembershipCommunicationPreferenceID membership_communication_preference_id,
               isnull(MMSInsertedDateTime, convert(datetime,'jan 1, 1980',107)) dv_load_date_time, 
               case when MMSInsertedDateTime is null then 19800101000000
                    else replace(replace(replace(convert(varchar, MMSInsertedDateTime,120 ), '-', ''),' ', ''), ':', '')
                end dv_batch_id,
               2 dv_r_load_source_id, 
               getdate() dv_inserted_date_time, 
               suser_sname() dv_insert_user 
          from dbo.#stage_mms_MembershipCommunicationPreference
         where rank2 = 1) x
--go
--select getdate()					----commenting out as we are converting into stored procedure

-- Create the l records.
-- Eliminate records in a series that have the same hash as the prior in the series
-- Calculate dv_load_date_time
--   If this is the first record of a series then MMSInsertedDateTime or Jan 1, 1980 if MMSInsertedDateTime is null.
--   Else if the hour portion of MMSUpdatedDateTime is 0 or 1 then add 3 hours to the date portion of MMSUpdatedDateTime
--   Else add 27 hours to the date portion of MMSUpdatedDateTime
-- dv_batch_id is the dv_load_date_time converted to YYYYMMDDHHMISS

insert into l_mms_membership_communication_preference(
       l_mms_membership_communication_preference_id,
       bk_hash,
       membership_communication_preference_id,
       membership_id,
       val_communication_preference_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select row_number() over(order by x.dv_batch_id, x.membership_communication_preference_id) l_mms_membership_communication_preference_id,
       x.*
  from (select #stage_mms_MembershipCommunicationPreference.bk_hash,
               #stage_mms_MembershipCommunicationPreference.MembershipCommunicationPreferenceID membership_communication_preference_id,
               #stage_mms_MembershipCommunicationPreference.MembershipID membership_id,
               #stage_mms_MembershipCommunicationPreference.ValCommunicationPreferenceID val_communication_preference_id,
               case when #stage_mms_MembershipCommunicationPreference.rank2 = 1 then
                         case when #stage_mms_MembershipCommunicationPreference.MMSInsertedDateTime is null then convert(datetime,'jan 1, 1980',107)
                              else #stage_mms_MembershipCommunicationPreference.MMSInsertedDateTime
                          end
                    when datepart(hh, #stage_mms_MembershipCommunicationPreference.MMSUpdatedDateTime) in (0, 1) then dateadd(hh, 3, convert(datetime, convert(date, #stage_mms_MembershipCommunicationPreference.MMSUpdatedDateTime)))
                    else dateadd(hh, 27, convert(datetime, convert(date, #stage_mms_MembershipCommunicationPreference.MMSUpdatedDateTime)))
                end dv_load_date_time,
               case when #stage_mms_MembershipCommunicationPreference.rank2 = 1 then
                         case when #stage_mms_MembershipCommunicationPreference.MMSInsertedDateTime is null then 19800101000000
                              else replace(replace(replace(convert(varchar, #stage_mms_MembershipCommunicationPreference.MMSInsertedDateTime,120), '-', ''),' ', ''), ':', '')
                          end
                    when datepart(hh, #stage_mms_MembershipCommunicationPreference.MMSUpdatedDateTime) in (0, 1) then replace(replace(replace(convert(varchar, dateadd(hh, 3, convert(datetime, convert(date, #stage_mms_MembershipCommunicationPreference.MMSUpdatedDateTime))),120), '-', ''),' ', ''), ':', '')
                    else replace(replace(replace(convert(varchar, dateadd(hh, 27, convert(datetime, convert(date, #stage_mms_MembershipCommunicationPreference.MMSUpdatedDateTime))),120), '-', ''),' ', ''), ':', '')
                end dv_batch_id,
               2 dv_r_load_source_id, 
               #stage_mms_MembershipCommunicationPreference.l_mms_membership_communication_preference_hash,
               getdate() dv_inserted_date_time, 
               suser_sname() dv_insert_user 
          from dbo.#stage_mms_MembershipCommunicationPreference
          left join dbo.#stage_mms_MembershipCommunicationPreference prior
            on #stage_mms_MembershipCommunicationPreference.MembershipCommunicationPreferenceID = prior.MembershipCommunicationPreferenceID
           and #stage_mms_MembershipCommunicationPreference.rank2 = prior.rank2 + 1
         where #stage_mms_MembershipCommunicationPreference.l_mms_membership_communication_preference_hash != isnull(prior.l_mms_membership_communication_preference_hash, ''))x
--go
---select getdate()										----commenting out as we are converting into stored procedure

-- Create the s records.
-- Eliminate records in a series that have the same hash as the prior in the series
-- Calculate dv_load_date_time
--   If this is the first record of a series then MMSInsertedDateTime or Jan 1, 1980 if MMSInsertedDateTime is null.
--   Else if the hour portion of MMSUpdatedDateTime is 0 or 1 then add 3 hours to the date portion of MMSUpdatedDateTime
--   Else add 27 hours to the date portion of MMSUpdatedDateTime
-- dv_batch_id is the dv_load_date_time converted to YYYYMMDDHHMISS


insert into s_mms_membership_communication_preference(
       s_mms_membership_communication_preference_id,
       bk_hash,
       membership_communication_preference_id,
       --membership_id,
       active_flag,
       inserted_date_time,
       updated_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select row_number() over(order by x.dv_batch_id, x.membership_communication_preference_id) s_mms_membership_communication_preference_id,
       x.*
  from (select #stage_mms_MembershipCommunicationPreference.bk_hash,
               #stage_mms_MembershipCommunicationPreference.MembershipCommunicationPreferenceID membership_communication_preference_id,
               --#stage_mms_MembershipCommunicationPreference.MembershipID membership_id,
               #stage_mms_MembershipCommunicationPreference.ActiveFlag active_flag,
               #stage_mms_MembershipCommunicationPreference.MMSInsertedDateTime inserted_date_time,
               #stage_mms_MembershipCommunicationPreference.MMSUpdatedDateTime updated_date_time,
               case when #stage_mms_MembershipCommunicationPreference.rank2 = 1 then
                         case when #stage_mms_MembershipCommunicationPreference.MMSInsertedDateTime is null then convert(datetime,'jan 1, 1980',107)
                              else #stage_mms_MembershipCommunicationPreference.MMSInsertedDateTime
                          end
                    when datepart(hh, #stage_mms_MembershipCommunicationPreference.MMSUpdatedDateTime) in (0, 1) then dateadd(hh, 3, convert(datetime, convert(date, #stage_mms_MembershipCommunicationPreference.MMSUpdatedDateTime)))
                    else dateadd(hh, 27, convert(datetime, convert(date, #stage_mms_MembershipCommunicationPreference.MMSUpdatedDateTime)))
                end dv_load_date_time,
               case when #stage_mms_MembershipCommunicationPreference.rank2 = 1 then
                         case when #stage_mms_MembershipCommunicationPreference.MMSInsertedDateTime is null then 19800101000000
                              else replace(replace(replace(convert(varchar, #stage_mms_MembershipCommunicationPreference.MMSInsertedDateTime,120), '-', ''),' ', ''), ':', '')
                          end
                    when datepart(hh, #stage_mms_MembershipCommunicationPreference.MMSUpdatedDateTime) in (0, 1) then replace(replace(replace(convert(varchar, dateadd(hh, 3, convert(datetime, convert(date, #stage_mms_MembershipCommunicationPreference.MMSUpdatedDateTime))),120), '-', ''),' ', ''), ':', '')
                    else replace(replace(replace(convert(varchar, dateadd(hh, 27, convert(datetime, convert(date, #stage_mms_MembershipCommunicationPreference.MMSUpdatedDateTime))),120), '-', ''),' ', ''), ':', '')
                end dv_batch_id,
               2 dv_r_load_source_id, 
               #stage_mms_MembershipCommunicationPreference.s_mms_membership_communication_preference_hash,
               getdate() dv_inserted_date_time, 
               suser_sname() dv_insert_user
          from dbo.#stage_mms_MembershipCommunicationPreference
          left join dbo.#stage_mms_MembershipCommunicationPreference prior
            on #stage_mms_MembershipCommunicationPreference.MembershipCommunicationPreferenceID = prior.MembershipCommunicationPreferenceID
           and #stage_mms_MembershipCommunicationPreference.rank2 = prior.rank2 + 1
         where #stage_mms_MembershipCommunicationPreference.s_mms_membership_communication_preference_hash != isnull(prior.s_mms_membership_communication_preference_hash, ''))x
--go
--select getdate()

/*
-- Make sure there are no duplicates as this will potentially cause the pit table to grow out of control
select distinct count(*) over() from [MMSMembershipCommunicationPreference] group by [MMSMembershipCommunicationPreferenceID] having count(*) > 1
select mms_membership_communication_preference_id from h_mms_membership_communication_preference group by membership_id having count(*) > 1
select mms_membership_communication_preference_id, dv_load_date_time from l_mms_membership_communication_preference group by mms_membership_communication_preference_id, dv_load_date_time having count(*) > 1
select mms_membership_communication_preference_id, dv_load_date_time from s_mms_membership_communication_preference group by mms_membership_communication_preference_id, dv_load_date_time having count(*) > 1
select mms_membership_communication_preference_id, dv_load_date_time from p_mms_membership_communication_preference group by mms_membership_communication_preference_id, dv_load_date_time having count(*) > 1
*/

update dv_sequence_number
   set max_sequence_number = (select max(h_mms_membership_communication_preference_id) from h_mms_membership_communication_preference)
 where table_name = 'h_mms_membership_communication_preference'
--go
update dv_sequence_number
   set max_sequence_number = (select max(l_mms_membership_communication_preference_id) from l_mms_membership_communication_preference)
 where table_name = 'l_mms_membership_communication_preference'
--go
update dv_sequence_number
   set max_sequence_number = (select max(s_mms_membership_communication_preference_id) from s_mms_membership_communication_preference)
 where table_name = 's_mms_membership_communication_preference'
-- p_mms_membership is updated by the pit proc
--go
---select getdate()										----commenting out as we are converting into stored procedure
/*
-- Dev/QA only code to handle bad data in staging
-- Delete any records where the dv_load_date_time for a record is less than the dv_load_date_time for the prior record in a series
--  There don't appear to be any in Dev, but try this in QA and prod too
select count(*)
  from (select MembershipCommunicationPreferenceID,
               rank2,
               case when #stage_mms_MembershipCommunicationPreference.rank2 = 1 then
                         case when #stage_mms_MembershipCommunicationPreference.MMSInsertedDateTime is null then convert(datetime,'jan 1, 1980',107)
                              else #stage_mms_MembershipCommunicationPreference.MMSInsertedDateTime
                          end
                    when datepart(hh, #stage_mms_MembershipCommunicationPreference.MMSUpdatedDateTime) in (0, 1) then dateadd(hh, 3, convert(datetime, convert(date, #stage_mms_MembershipCommunicationPreference.MMSUpdatedDateTime)))
                    else dateadd(hh, 27, convert(datetime, convert(date, #stage_mms_MembershipCommunicationPreference.MMSUpdatedDateTime)))
                end dv_load_date_time
          from #stage_mms_MembershipCommunicationPreference) this
  left join (select MembershipCommunicationPreferenceID,
               rank2,
               case when #stage_mms_MembershipCommunicationPreference.rank2 = 1 then
                         case when #stage_mms_MembershipCommunicationPreference.MMSInsertedDateTime is null then convert(datetime,'jan 1, 1980',107)
                              else #stage_mms_MembershipCommunicationPreference.MMSInsertedDateTime
                          end
                    when datepart(hh, #stage_mms_MembershipCommunicationPreference.MMSUpdatedDateTime) in (0, 1) then dateadd(hh, 3, convert(datetime, convert(date, #stage_mms_MembershipCommunicationPreference.MMSUpdatedDateTime)))
                    else dateadd(hh, 27, convert(datetime, convert(date, #stage_mms_MembershipCommunicationPreference.MMSUpdatedDateTime)))
                end dv_load_date_time
          from #stage_mms_MembershipCommunicationPreference) prior
    on this.MembershipCommunicationPreferenceID = prior.MembershipCommunicationPreferenceID
   and this.rank2 = prior.rank2 + 1
 where this.dv_load_date_time < isnull(prior.dv_load_date_time, 'jan 1, 1900')
*/

-- Populate the pit table
truncate table p_mms_membership_communication_preference	                    ---Truncating pit table as suggested by Brian,slightly doubtful on functionality if otherwise
exec proc_p_mms_membership_communication_preference @current_dv_batch_id = -1
--go
--select getdate()													----commenting out as we are converting into stored procedure

end

