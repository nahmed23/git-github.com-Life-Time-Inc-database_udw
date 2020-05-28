CREATE PROC [dbo].[proc_etl_mms_guest] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_mms_Guest

set @insert_date_time = getdate()
insert into dbo.stage_hash_mms_Guest (
       bk_hash,
       GuestID,
       CardNumber,
       FirstName,
       MiddleName,
       LastName,
       AddressLine1,
       AddressLine2,
       City,
       State,
       ZIP,
       InsertedDateTime,
       UpdatedDateTime,
       MaskedPersonalID,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(GuestID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       GuestID,
       CardNumber,
       FirstName,
       MiddleName,
       LastName,
       AddressLine1,
       AddressLine2,
       City,
       State,
       ZIP,
       InsertedDateTime,
       UpdatedDateTime,
       MaskedPersonalID,
       isnull(cast(stage_mms_Guest.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_mms_Guest
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_mms_guest @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_mms_guest (
       bk_hash,
       guest_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_mms_Guest.bk_hash,
       stage_hash_mms_Guest.GuestID guest_id,
       isnull(cast(stage_hash_mms_Guest.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       2,
       @insert_date_time,
       @user
  from stage_hash_mms_Guest
  left join h_mms_guest
    on stage_hash_mms_Guest.bk_hash = h_mms_guest.bk_hash
 where h_mms_guest_id is null
   and stage_hash_mms_Guest.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_mms_guest
if object_id('tempdb..#l_mms_guest_inserts') is not null drop table #l_mms_guest_inserts
create table #l_mms_guest_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_Guest.bk_hash,
       stage_hash_mms_Guest.GuestID guest_id,
       stage_hash_mms_Guest.MaskedPersonalID masked_personal_id,
       stage_hash_mms_Guest.InsertedDateTime dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_Guest.GuestID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_Guest.MaskedPersonalID,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_Guest
 where stage_hash_mms_Guest.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_mms_guest records
set @insert_date_time = getdate()
insert into l_mms_guest (
       bk_hash,
       guest_id,
       masked_personal_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_mms_guest_inserts.bk_hash,
       #l_mms_guest_inserts.guest_id,
       #l_mms_guest_inserts.masked_personal_id,
       case when l_mms_guest.l_mms_guest_id is null then isnull(#l_mms_guest_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #l_mms_guest_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_mms_guest_inserts
  left join p_mms_guest
    on #l_mms_guest_inserts.bk_hash = p_mms_guest.bk_hash
   and p_mms_guest.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_mms_guest
    on p_mms_guest.bk_hash = l_mms_guest.bk_hash
   and p_mms_guest.l_mms_guest_id = l_mms_guest.l_mms_guest_id
 where l_mms_guest.l_mms_guest_id is null
    or (l_mms_guest.l_mms_guest_id is not null
        and l_mms_guest.dv_hash <> #l_mms_guest_inserts.source_hash)

--calculate hash and lookup to current s_mms_guest
if object_id('tempdb..#s_mms_guest_inserts') is not null drop table #s_mms_guest_inserts
create table #s_mms_guest_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_Guest.bk_hash,
       stage_hash_mms_Guest.GuestID guest_id,
       stage_hash_mms_Guest.CardNumber card_number,
       stage_hash_mms_Guest.FirstName first_name,
       stage_hash_mms_Guest.MiddleName middle_name,
       stage_hash_mms_Guest.LastName last_name,
       stage_hash_mms_Guest.AddressLine1 address_line1,
       stage_hash_mms_Guest.AddressLine2 address_line2,
       stage_hash_mms_Guest.City city,
       stage_hash_mms_Guest.State state,
       stage_hash_mms_Guest.ZIP zip,
       stage_hash_mms_Guest.InsertedDateTime inserted_date_time,
       stage_hash_mms_Guest.UpdatedDateTime updated_date_time,
       stage_hash_mms_Guest.InsertedDateTime dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_Guest.GuestID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_Guest.CardNumber,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_Guest.FirstName,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_Guest.MiddleName,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_Guest.LastName,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_Guest.AddressLine1,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_Guest.AddressLine2,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_Guest.City,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_Guest.State,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_Guest.ZIP,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_Guest.InsertedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_Guest.UpdatedDateTime,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_Guest
 where stage_hash_mms_Guest.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mms_guest records
set @insert_date_time = getdate()
insert into s_mms_guest (
       bk_hash,
       guest_id,
       card_number,
       first_name,
       middle_name,
       last_name,
       address_line1,
       address_line2,
       city,
       state,
       zip,
       inserted_date_time,
       updated_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mms_guest_inserts.bk_hash,
       #s_mms_guest_inserts.guest_id,
       #s_mms_guest_inserts.card_number,
       #s_mms_guest_inserts.first_name,
       #s_mms_guest_inserts.middle_name,
       #s_mms_guest_inserts.last_name,
       #s_mms_guest_inserts.address_line1,
       #s_mms_guest_inserts.address_line2,
       #s_mms_guest_inserts.city,
       #s_mms_guest_inserts.state,
       #s_mms_guest_inserts.zip,
       #s_mms_guest_inserts.inserted_date_time,
       #s_mms_guest_inserts.updated_date_time,
       case when s_mms_guest.s_mms_guest_id is null then isnull(#s_mms_guest_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #s_mms_guest_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mms_guest_inserts
  left join p_mms_guest
    on #s_mms_guest_inserts.bk_hash = p_mms_guest.bk_hash
   and p_mms_guest.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mms_guest
    on p_mms_guest.bk_hash = s_mms_guest.bk_hash
   and p_mms_guest.s_mms_guest_id = s_mms_guest.s_mms_guest_id
 where s_mms_guest.s_mms_guest_id is null
    or (s_mms_guest.s_mms_guest_id is not null
        and s_mms_guest.dv_hash <> #s_mms_guest_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_mms_guest @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_mms_guest @current_dv_batch_id

end
