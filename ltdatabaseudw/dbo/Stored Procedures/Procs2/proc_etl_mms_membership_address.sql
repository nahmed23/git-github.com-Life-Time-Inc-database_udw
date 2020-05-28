CREATE PROC [dbo].[proc_etl_mms_membership_address] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

delete from stage_hash_mms_MembershipAddress where dv_batch_id = @current_dv_batch_id

set @insert_date_time = getdate()
insert into dbo.stage_hash_mms_MembershipAddress (
       bk_hash,
       MembershipAddressID,
       MembershipID,
       AddressLine1,
       AddressLine2,
       City,
       ValAddressTypeID,
       Zip,
       InsertedDateTime,
       ValCountryID,
       ValStateID,
       UpdatedDateTime,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(MembershipAddressID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       MembershipAddressID,
       MembershipID,
       AddressLine1,
       AddressLine2,
       City,
       ValAddressTypeID,
       Zip,
       InsertedDateTime,
       ValCountryID,
       ValStateID,
       UpdatedDateTime,
       isnull(cast(stage_mms_MembershipAddress.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_mms_MembershipAddress
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_mms_membership_address @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_mms_membership_address (
       bk_hash,
       membership_address_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_mms_MembershipAddress.bk_hash,
       stage_hash_mms_MembershipAddress.MembershipAddressID membership_address_id,
       isnull(cast(stage_hash_mms_MembershipAddress.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       2,
       @insert_date_time,
       @user
  from stage_hash_mms_MembershipAddress
  left join h_mms_membership_address
    on stage_hash_mms_MembershipAddress.bk_hash = h_mms_membership_address.bk_hash
 where h_mms_membership_address_id is null
   and stage_hash_mms_MembershipAddress.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_mms_membership_address
if object_id('tempdb..#l_mms_membership_address_inserts') is not null drop table #l_mms_membership_address_inserts
create table #l_mms_membership_address_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_MembershipAddress.bk_hash,
       stage_hash_mms_MembershipAddress.MembershipAddressID membership_address_id,
       stage_hash_mms_MembershipAddress.MembershipID membership_id,
       stage_hash_mms_MembershipAddress.ValAddressTypeID val_address_type_id,
       stage_hash_mms_MembershipAddress.ValCountryID val_country_id,
       stage_hash_mms_MembershipAddress.ValStateID val_state_id,
       stage_hash_mms_MembershipAddress.InsertedDateTime dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipAddress.MembershipAddressID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipAddress.MembershipID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipAddress.ValAddressTypeID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipAddress.ValCountryID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipAddress.ValStateID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_MembershipAddress
 where stage_hash_mms_MembershipAddress.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_mms_membership_address records
set @insert_date_time = getdate()
insert into l_mms_membership_address (
       bk_hash,
       membership_address_id,
       membership_id,
       val_address_type_id,
       val_country_id,
       val_state_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_mms_membership_address_inserts.bk_hash,
       #l_mms_membership_address_inserts.membership_address_id,
       #l_mms_membership_address_inserts.membership_id,
       #l_mms_membership_address_inserts.val_address_type_id,
       #l_mms_membership_address_inserts.val_country_id,
       #l_mms_membership_address_inserts.val_state_id,
       case when l_mms_membership_address.l_mms_membership_address_id is null then isnull(#l_mms_membership_address_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #l_mms_membership_address_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_mms_membership_address_inserts
  left join p_mms_membership_address
    on #l_mms_membership_address_inserts.bk_hash = p_mms_membership_address.bk_hash
   and p_mms_membership_address.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_mms_membership_address
    on p_mms_membership_address.bk_hash = l_mms_membership_address.bk_hash
   and p_mms_membership_address.l_mms_membership_address_id = l_mms_membership_address.l_mms_membership_address_id
 where l_mms_membership_address.l_mms_membership_address_id is null
    or (l_mms_membership_address.l_mms_membership_address_id is not null
        and l_mms_membership_address.dv_hash <> #l_mms_membership_address_inserts.source_hash)

--calculate hash and lookup to current s_mms_membership_address
if object_id('tempdb..#s_mms_membership_address_inserts') is not null drop table #s_mms_membership_address_inserts
create table #s_mms_membership_address_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_MembershipAddress.bk_hash,
       stage_hash_mms_MembershipAddress.MembershipAddressID membership_address_id,
       stage_hash_mms_MembershipAddress.MembershipID membership_id,
       stage_hash_mms_MembershipAddress.AddressLine1 address_line_1,
       stage_hash_mms_MembershipAddress.AddressLine2 address_line_2,
       stage_hash_mms_MembershipAddress.City city,
       stage_hash_mms_MembershipAddress.Zip zip,
       stage_hash_mms_MembershipAddress.InsertedDateTime inserted_date_time,
       stage_hash_mms_MembershipAddress.UpdatedDateTime updated_date_time,
       stage_hash_mms_MembershipAddress.InsertedDateTime dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipAddress.MembershipAddressID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipAddress.MembershipID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_MembershipAddress.AddressLine1,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_MembershipAddress.AddressLine2,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_MembershipAddress.City,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_MembershipAddress.Zip,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_MembershipAddress.InsertedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_MembershipAddress.UpdatedDateTime,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_MembershipAddress
 where stage_hash_mms_MembershipAddress.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mms_membership_address records
set @insert_date_time = getdate()
insert into s_mms_membership_address (
       bk_hash,
       membership_address_id,
       membership_id,
       address_line_1,
       address_line_2,
       city,
       zip,
       inserted_date_time,
       updated_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mms_membership_address_inserts.bk_hash,
       #s_mms_membership_address_inserts.membership_address_id,
       #s_mms_membership_address_inserts.membership_id,
       #s_mms_membership_address_inserts.address_line_1,
       #s_mms_membership_address_inserts.address_line_2,
       #s_mms_membership_address_inserts.city,
       #s_mms_membership_address_inserts.zip,
       #s_mms_membership_address_inserts.inserted_date_time,
       #s_mms_membership_address_inserts.updated_date_time,
       case when s_mms_membership_address.s_mms_membership_address_id is null then isnull(#s_mms_membership_address_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #s_mms_membership_address_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mms_membership_address_inserts
  left join p_mms_membership_address
    on #s_mms_membership_address_inserts.bk_hash = p_mms_membership_address.bk_hash
   and p_mms_membership_address.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mms_membership_address
    on p_mms_membership_address.bk_hash = s_mms_membership_address.bk_hash
   and p_mms_membership_address.s_mms_membership_address_id = s_mms_membership_address.s_mms_membership_address_id
 where s_mms_membership_address.s_mms_membership_address_id is null
    or (s_mms_membership_address.s_mms_membership_address_id is not null
        and s_mms_membership_address.dv_hash <> #s_mms_membership_address_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_mms_membership_address @current_dv_batch_id

end
