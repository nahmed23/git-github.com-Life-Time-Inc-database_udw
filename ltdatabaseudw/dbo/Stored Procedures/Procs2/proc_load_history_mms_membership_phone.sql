CREATE PROC [dbo].[proc_load_history_mms_membership_phone] AS
begin

set nocount on
set xact_abort on
/*
-- prepare the dv tables
truncate table dbo.h_mms_membership_phone
truncate table dbo.l_mms_membership_phone
truncate table dbo.s_mms_membership_phone


exec proc_util_create_base_records @table_name = 'h_mms_membership_phone'
exec proc_util_create_base_records @table_name = 'l_mms_membership_phone'
exec proc_util_create_base_records @table_name = 's_mms_membership_phone'
exec proc_p_mms_membership_phone @current_dv_batch_id = -1
go
select getdate()
*/

--Select the records from MMSMembershipPhone to be staged and inserted into the dv tables
--We only want 1 record per MMSMembershipPhone for any particular timestamp
--  Do row_number ranking
--    Partition by the following
--      MembershipPhoneID
--      Calculated update_insert_date
--        If MMSUpdatedDateTime is null then MMSInsertedDateTime
--        Else if the hour portion of MMSUpdatedDateTime is 0 or 1 then add 3 hours to the date portion of MMSUpdatedDateTime
--        Else add 27 hours to the date portion of MMSUpdatedDateTime
--    Order by MMSMembershipPhoneKey in descending order (this keeps the most recent record where one or more are in LTFDWStg with the same date)
--  Only keep the records with rank = 1
if object_id('tempdb.dbo.#stage_mms_MembershipPhone') is not null drop table #stage_mms_MembershipPhone
create table dbo.#stage_mms_MembershipPhone with (location=user_db, distribution = hash(MembershipPhoneID)) as
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(MembershipPhoneID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       x.*,
       row_number() over(partition by MembershipPhoneID order by x.update_insert_date) rank2,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(x.MembershipPhoneID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(x.MembershipID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(x.ValPhoneTypeID as varchar(500)),'z#@$k%&P'))
                                  ),2
               ) l_mms_membership_phone_hash,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(x.MembershipPhoneID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(x.AreaCode,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(x.Number,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,x.MMSInsertedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,x.MMSUpdatedDateTime,120),'z#@$k%&P'))
                                  ),2
               ) s_mms_membership_phone_hash
  from (select row_number() over(partition by MembershipPhoneID,
                                              case when MMSUpdatedDateTime is null then MMSInsertedDateTime
                                                   when datepart(hh, MMSUpdatedDateTime) in (0, 1) then dateadd(hh, 3, convert(datetime, convert(date, MMSUpdatedDateTime)))
                                                   else dateadd(hh, 27, convert(datetime, convert(date, MMSUpdatedDateTime)))
                                               end
                                 order by [MMSMembershipPhoneKey] desc) rank1,
              case when MMSUpdatedDateTime is null then MMSInsertedDateTime
                   when datepart(hh, MMSUpdatedDateTime) in (0, 1) then dateadd(hh, 3, convert(datetime, convert(date, MMSUpdatedDateTime)))
                   else dateadd(hh, 27, convert(datetime, convert(date, MMSUpdatedDateTime)))
               end update_insert_date,
               *
          from MMSMembershipPhone) x
 where rank1 = 1
--go
--select getdate()							----commenting out as we are converting into stored procedure

/*
select * from h_mms_membership_phone
select * from l_mms_membership_phone
select * from s_mms_membership_phone
select * from p_mms_membership_phone
*/

-- Create the h records.
-- Only use records where rank2 = 1 (first record in a series: min(update_insert_date))
-- dv_load_date_time is the MMSInsertedDateTime or Jan 1, 1980 if MMSInsertedDateTime is null.
-- dv_batch_id is the dv_load_date_time converted to YYYYMMDDHHMISS
insert into dbo.h_mms_membership_phone(
      h_mms_membership_phone_id,
	bk_hash,
	membership_phone_id,
dv_load_date_time, 
       dv_batch_id, 
       dv_r_load_source_id, 
       dv_inserted_date_time, 
       dv_insert_user)
select row_number() over(order by x.dv_batch_id, x.membership_phone_id) h_mms_membership_phone_id,
       x.*
  from (select bk_hash,
               MembershipPhoneID membership_phone_id,
               isnull(MMSInsertedDateTime, convert(datetime,'jan 1, 1980',107)) dv_load_date_time, 
               case when MMSInsertedDateTime is null then 19800101000000
                    else replace(replace(replace(convert(varchar, MMSInsertedDateTime,120 ), '-', ''),' ', ''), ':', '')
                end dv_batch_id,
               2 dv_r_load_source_id, 
               getdate() dv_inserted_date_time, 
               suser_sname() dv_insert_user 
          from dbo.#stage_mms_MembershipPhone
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
insert into l_mms_membership_phone (
	l_mms_membership_phone_id,
	bk_hash,
	membership_phone_id,
	membership_id,
	val_phone_type_id,
	dv_load_date_time,
	dv_batch_id,
	dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select row_number() over(order by x.dv_batch_id, x.membership_phone_id) l_mms_membership_phone_id,
       x.*
  from (select #stage_mms_MembershipPhone.bk_hash,
               #stage_mms_MembershipPhone.MembershipPhoneID membership_phone_id,
               #stage_mms_MembershipPhone.MembershipID membership_id,
               #stage_mms_MembershipPhone.ValPhoneTypeID val_phone_type_id,
                              case when #stage_mms_MembershipPhone.rank2 = 1 then
                         case when #stage_mms_MembershipPhone.MMSInsertedDateTime is null then convert(datetime,'jan 1, 1980',107)
                              else #stage_mms_MembershipPhone.MMSInsertedDateTime
                          end
                    when datepart(hh, #stage_mms_MembershipPhone.MMSUpdatedDateTime) in (0, 1) then dateadd(hh, 3, convert(datetime, convert(date, #stage_mms_MembershipPhone.MMSUpdatedDateTime)))
                    else dateadd(hh, 27, convert(datetime, convert(date, #stage_mms_MembershipPhone.MMSUpdatedDateTime)))
                end dv_load_date_time,
               case when #stage_mms_MembershipPhone.rank2 = 1 then
                         case when #stage_mms_MembershipPhone.MMSInsertedDateTime is null then 19800101000000
                              else replace(replace(replace(convert(varchar, #stage_mms_MembershipPhone.MMSInsertedDateTime,120), '-', ''),' ', ''), ':', '')
                          end
                    when datepart(hh, #stage_mms_MembershipPhone.MMSUpdatedDateTime) in (0, 1) then replace(replace(replace(convert(varchar, dateadd(hh, 3, convert(datetime, convert(date, #stage_mms_MembershipPhone.MMSUpdatedDateTime))),120), '-', ''),' ', ''), ':', '')
                    else replace(replace(replace(convert(varchar, dateadd(hh, 27, convert(datetime, convert(date, #stage_mms_MembershipPhone.MMSUpdatedDateTime))),120), '-', ''),' ', ''), ':', '')
                end dv_batch_id,
               2 dv_r_load_source_id, 
               #stage_mms_MembershipPhone.l_mms_membership_phone_hash,
               getdate() dv_inserted_date_time, 
               suser_sname() dv_insert_user 
          from dbo.#stage_mms_MembershipPhone
          left join dbo.#stage_mms_MembershipPhone prior
            on #stage_mms_MembershipPhone.MembershipPhoneID = prior.MembershipPhoneID
           and #stage_mms_MembershipPhone.rank2 = prior.rank2 + 1
         where #stage_mms_MembershipPhone.l_mms_membership_phone_hash != isnull(prior.l_mms_membership_phone_hash, ''))x
--go
---select getdate()										----commenting out as we are converting into stored procedure

-- Create the s records.
-- Eliminate records in a series that have the same hash as the prior in the series
-- Calculate dv_load_date_time
--   If this is the first record of a series then MMSInsertedDateTime or Jan 1, 1980 if MMSInsertedDateTime is null.
--   Else if the hour portion of MMSUpdatedDateTime is 0 or 1 then add 3 hours to the date portion of MMSUpdatedDateTime
--   Else add 27 hours to the date portion of MMSUpdatedDateTime
-- dv_batch_id is the dv_load_date_time converted to YYYYMMDDHHMISS
insert into s_mms_membership_phone (
       s_mms_membership_phone_id,
bk_hash,
membership_phone_id,
area_code,
number,
       inserted_date_time,
       updated_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select row_number() over(order by x.dv_batch_id, x.membership_phone_id) s_mms_membership_phone_id,
       x.*
  from (select #stage_mms_MembershipPhone.bk_hash,
               #stage_mms_MembershipPhone.MembershipPhoneID membership_phone_id,
               #stage_mms_MembershipPhone.AreaCode area_code,
               #stage_mms_MembershipPhone.Number number,
               #stage_mms_MembershipPhone.MMSInsertedDateTime inserted_date_time,
               #stage_mms_MembershipPhone.MMSUpdatedDateTime updated_date_time,
               case when #stage_mms_MembershipPhone.rank2 = 1 then
                         case when #stage_mms_MembershipPhone.MMSInsertedDateTime is null then convert(datetime,'jan 1, 1980',107)
                              else #stage_mms_MembershipPhone.MMSInsertedDateTime
                          end
                    when datepart(hh, #stage_mms_MembershipPhone.MMSUpdatedDateTime) in (0, 1) then dateadd(hh, 3, convert(datetime, convert(date, #stage_mms_MembershipPhone.MMSUpdatedDateTime)))
                    else dateadd(hh, 27, convert(datetime, convert(date, #stage_mms_MembershipPhone.MMSUpdatedDateTime)))
                end dv_load_date_time,
               case when #stage_mms_MembershipPhone.rank2 = 1 then
                         case when #stage_mms_MembershipPhone.MMSInsertedDateTime is null then 19800101000000
                              else replace(replace(replace(convert(varchar, #stage_mms_MembershipPhone.MMSInsertedDateTime,120), '-', ''),' ', ''), ':', '')
                          end
                    when datepart(hh, #stage_mms_MembershipPhone.MMSUpdatedDateTime) in (0, 1) then replace(replace(replace(convert(varchar, dateadd(hh, 3, convert(datetime, convert(date, #stage_mms_MembershipPhone.MMSUpdatedDateTime))),120), '-', ''),' ', ''), ':', '')
                    else replace(replace(replace(convert(varchar, dateadd(hh, 27, convert(datetime, convert(date, #stage_mms_MembershipPhone.MMSUpdatedDateTime))),120), '-', ''),' ', ''), ':', '')
                end dv_batch_id,
               2 dv_r_load_source_id, 
               #stage_mms_MembershipPhone.s_mms_membership_phone_hash,
               getdate() dv_inserted_date_time, 
               suser_sname() dv_insert_user
          from dbo.#stage_mms_MembershipPhone
          left join dbo.#stage_mms_MembershipPhone prior
            on #stage_mms_MembershipPhone.MembershipPhoneID = prior.MembershipPhoneID
           and #stage_mms_MembershipPhone.rank2 = prior.rank2 + 1
         where #stage_mms_MembershipPhone.s_mms_membership_phone_hash != isnull(prior.s_mms_membership_phone_hash, ''))x
--go
--select getdate()									----commenting out as we are converting into stored procedure

/*
-- Make sure there are no duplicates as this will potentially cause the pit table to grow out of control
select distinct count(*) over() from [MMSMembershipPhone] group by [MembershipPhoneID] having count(*) > 1
select membership_phone_id from h_mms_membership_phone group by membership_id having count(*) > 1
select membership_phone_id, dv_load_date_time from l_mms_membership_phone group by membership_phone_id, dv_load_date_time having count(*) > 1
select membership_phone_id, dv_load_date_time from s_mms_membership_phone group by membership_phone_id, dv_load_date_time having count(*) > 1
select membership_phone_id, dv_load_date_time from p_mms_membership_phone group by membership_phone_id, dv_load_date_time having count(*) > 1
*/


update dv_sequence_number
   set max_sequence_number = (select max(h_mms_membership_phone_id) from h_mms_membership_phone)
 where table_name = 'h_mms_membership_phone'
--go
update dv_sequence_number
   set max_sequence_number = (select max(l_mms_membership_phone_id) from l_mms_membership_phone)
 where table_name = 'l_mms_membership_phone'
--go
update dv_sequence_number
   set max_sequence_number = (select max(s_mms_membership_phone_id) from s_mms_membership_phone)
 where table_name = 's_mms_membership_phone'
-- p_mms_membership is updated by the pit proc
--go
---select getdate()																----commenting out as we are converting into stored procedure


/*
-- Dev/QA only code to handle bad data in staging
-- Delete any records where the dv_load_date_time for a record is less than the dv_load_date_time for the prior record in a series
--  There don't appear to be any in Dev, but try this in QA and prod too
select count(*)
  from (select MembershipAddressID,
               rank2,
               case when #stage_mms_MembershipAddress.rank2 = 1 then
                         case when #stage_mms_MembershipAddress.MMSInsertedDateTime is null then convert(datetime,'jan 1, 1980',107)
                              else #stage_mms_MembershipAddress.MMSInsertedDateTime
                          end
                    when datepart(hh, #stage_mms_MembershipAddress.MMSUpdatedDateTime) in (0, 1) then dateadd(hh, 3, convert(datetime, convert(date, #stage_mms_MembershipAddress.MMSUpdatedDateTime)))
                    else dateadd(hh, 27, convert(datetime, convert(date, #stage_mms_MembershipAddress.MMSUpdatedDateTime)))
                end dv_load_date_time
          from #stage_mms_MembershipAddress) this
  left join (select MembershipAddressID,
               rank2,
               case when #stage_mms_MembershipAddress.rank2 = 1 then
                         case when #stage_mms_MembershipAddress.MMSInsertedDateTime is null then convert(datetime,'jan 1, 1980',107)
                              else #stage_mms_MembershipAddress.MMSInsertedDateTime
                          end
                    when datepart(hh, #stage_mms_MembershipAddress.MMSUpdatedDateTime) in (0, 1) then dateadd(hh, 3, convert(datetime, convert(date, #stage_mms_MembershipAddress.MMSUpdatedDateTime)))
                    else dateadd(hh, 27, convert(datetime, convert(date, #stage_mms_MembershipAddress.MMSUpdatedDateTime)))
                end dv_load_date_time
          from #stage_mms_MembershipAddress) prior
    on this.MembershipAddressID = prior.MembershipAddressID
   and this.rank2 = prior.rank2 + 1
 where this.dv_load_date_time < isnull(prior.dv_load_date_time, 'jan 1, 1900')
*/

-- Populate the pit table
truncate table dbo.p_mms_membership_phone						---Truncating pit table as suggested by Brian,slightly doubtful on functionality if otherwise
exec proc_p_mms_membership_phone @current_dv_batch_id = -1
--go
--select getdate()													----commenting out as we are converting into stored procedure

end

