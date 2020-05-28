CREATE PROC [dbo].[proc_etl_mms_ltf_key_owner] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_mms_LTFKeyOwner

set @insert_date_time = getdate()
insert into dbo.stage_hash_mms_LTFKeyOwner (
       bk_hash,
       LTFKeyOwnerID,
       PartyID,
       LTFKeyID,
       KeyPriority,
       FromDate,
       ThruDate,
       FromTime,
       ThruTime,
       UsageCount,
       UsageLimit,
       AcquisitionID,
       ValAcquisitionTypeID,
       ValOwnershipTypeID,
       InsertedDateTime,
       UpdatedDateTime,
       DisplayName,
       LTFKeyAcquisitionID,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(LTFKeyOwnerID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       LTFKeyOwnerID,
       PartyID,
       LTFKeyID,
       KeyPriority,
       FromDate,
       ThruDate,
       FromTime,
       ThruTime,
       UsageCount,
       UsageLimit,
       AcquisitionID,
       ValAcquisitionTypeID,
       ValOwnershipTypeID,
       InsertedDateTime,
       UpdatedDateTime,
       DisplayName,
       LTFKeyAcquisitionID,
       isnull(cast(stage_mms_LTFKeyOwner.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_mms_LTFKeyOwner
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_mms_ltf_key_owner @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_mms_ltf_key_owner (
       bk_hash,
       ltf_key_owner_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_mms_LTFKeyOwner.bk_hash,
       stage_hash_mms_LTFKeyOwner.LTFKeyOwnerID ltf_key_owner_id,
       isnull(cast(stage_hash_mms_LTFKeyOwner.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       2,
       @insert_date_time,
       @user
  from stage_hash_mms_LTFKeyOwner
  left join h_mms_ltf_key_owner
    on stage_hash_mms_LTFKeyOwner.bk_hash = h_mms_ltf_key_owner.bk_hash
 where h_mms_ltf_key_owner_id is null
   and stage_hash_mms_LTFKeyOwner.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_mms_ltf_key_owner
if object_id('tempdb..#l_mms_ltf_key_owner_inserts') is not null drop table #l_mms_ltf_key_owner_inserts
create table #l_mms_ltf_key_owner_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_LTFKeyOwner.bk_hash,
       stage_hash_mms_LTFKeyOwner.LTFKeyOwnerID ltf_key_owner_id,
       stage_hash_mms_LTFKeyOwner.PartyID party_id,
       stage_hash_mms_LTFKeyOwner.LTFKeyID ltf_key_id,
       stage_hash_mms_LTFKeyOwner.AcquisitionID acquisition_id,
       stage_hash_mms_LTFKeyOwner.ValAcquisitionTypeID val_acquisition_type_id,
       stage_hash_mms_LTFKeyOwner.ValOwnershipTypeID val_ownership_type_id,
       stage_hash_mms_LTFKeyOwner.LTFKeyAcquisitionID ltf_key_acquisition_id,
       isnull(cast(stage_hash_mms_LTFKeyOwner.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_LTFKeyOwner.LTFKeyOwnerID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_LTFKeyOwner.PartyID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_LTFKeyOwner.LTFKeyID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_LTFKeyOwner.AcquisitionID,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_LTFKeyOwner.ValAcquisitionTypeID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_LTFKeyOwner.ValOwnershipTypeID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_LTFKeyOwner.LTFKeyAcquisitionID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_LTFKeyOwner
 where stage_hash_mms_LTFKeyOwner.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_mms_ltf_key_owner records
set @insert_date_time = getdate()
insert into l_mms_ltf_key_owner (
       bk_hash,
       ltf_key_owner_id,
       party_id,
       ltf_key_id,
       acquisition_id,
       val_acquisition_type_id,
       val_ownership_type_id,
       ltf_key_acquisition_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_mms_ltf_key_owner_inserts.bk_hash,
       #l_mms_ltf_key_owner_inserts.ltf_key_owner_id,
       #l_mms_ltf_key_owner_inserts.party_id,
       #l_mms_ltf_key_owner_inserts.ltf_key_id,
       #l_mms_ltf_key_owner_inserts.acquisition_id,
       #l_mms_ltf_key_owner_inserts.val_acquisition_type_id,
       #l_mms_ltf_key_owner_inserts.val_ownership_type_id,
       #l_mms_ltf_key_owner_inserts.ltf_key_acquisition_id,
       case when l_mms_ltf_key_owner.l_mms_ltf_key_owner_id is null then isnull(#l_mms_ltf_key_owner_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #l_mms_ltf_key_owner_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_mms_ltf_key_owner_inserts
  left join p_mms_ltf_key_owner
    on #l_mms_ltf_key_owner_inserts.bk_hash = p_mms_ltf_key_owner.bk_hash
   and p_mms_ltf_key_owner.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_mms_ltf_key_owner
    on p_mms_ltf_key_owner.bk_hash = l_mms_ltf_key_owner.bk_hash
   and p_mms_ltf_key_owner.l_mms_ltf_key_owner_id = l_mms_ltf_key_owner.l_mms_ltf_key_owner_id
 where l_mms_ltf_key_owner.l_mms_ltf_key_owner_id is null
    or (l_mms_ltf_key_owner.l_mms_ltf_key_owner_id is not null
        and l_mms_ltf_key_owner.dv_hash <> #l_mms_ltf_key_owner_inserts.source_hash)

--calculate hash and lookup to current s_mms_ltf_key_owner
if object_id('tempdb..#s_mms_ltf_key_owner_inserts') is not null drop table #s_mms_ltf_key_owner_inserts
create table #s_mms_ltf_key_owner_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_LTFKeyOwner.bk_hash,
       stage_hash_mms_LTFKeyOwner.LTFKeyOwnerID ltf_key_owner_id,
       stage_hash_mms_LTFKeyOwner.KeyPriority key_priority,
       stage_hash_mms_LTFKeyOwner.FromDate from_date,
       stage_hash_mms_LTFKeyOwner.ThruDate thru_date,
       stage_hash_mms_LTFKeyOwner.FromTime from_time,
       stage_hash_mms_LTFKeyOwner.ThruTime thru_time,
       stage_hash_mms_LTFKeyOwner.UsageCount usage_count,
       stage_hash_mms_LTFKeyOwner.UsageLimit usage_limit,
       stage_hash_mms_LTFKeyOwner.InsertedDateTime inserted_date_time,
       stage_hash_mms_LTFKeyOwner.UpdatedDateTime updated_date_time,
       stage_hash_mms_LTFKeyOwner.DisplayName display_name,
       isnull(cast(stage_hash_mms_LTFKeyOwner.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_LTFKeyOwner.LTFKeyOwnerID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_LTFKeyOwner.KeyPriority as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_LTFKeyOwner.FromDate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_LTFKeyOwner.ThruDate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_LTFKeyOwner.FromTime,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_LTFKeyOwner.ThruTime,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_LTFKeyOwner.UsageCount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_LTFKeyOwner.UsageLimit as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_LTFKeyOwner.InsertedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_LTFKeyOwner.UpdatedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_LTFKeyOwner.DisplayName,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_LTFKeyOwner
 where stage_hash_mms_LTFKeyOwner.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mms_ltf_key_owner records
set @insert_date_time = getdate()
insert into s_mms_ltf_key_owner (
       bk_hash,
       ltf_key_owner_id,
       key_priority,
       from_date,
       thru_date,
       from_time,
       thru_time,
       usage_count,
       usage_limit,
       inserted_date_time,
       updated_date_time,
       display_name,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mms_ltf_key_owner_inserts.bk_hash,
       #s_mms_ltf_key_owner_inserts.ltf_key_owner_id,
       #s_mms_ltf_key_owner_inserts.key_priority,
       #s_mms_ltf_key_owner_inserts.from_date,
       #s_mms_ltf_key_owner_inserts.thru_date,
       #s_mms_ltf_key_owner_inserts.from_time,
       #s_mms_ltf_key_owner_inserts.thru_time,
       #s_mms_ltf_key_owner_inserts.usage_count,
       #s_mms_ltf_key_owner_inserts.usage_limit,
       #s_mms_ltf_key_owner_inserts.inserted_date_time,
       #s_mms_ltf_key_owner_inserts.updated_date_time,
       #s_mms_ltf_key_owner_inserts.display_name,
       case when s_mms_ltf_key_owner.s_mms_ltf_key_owner_id is null then isnull(#s_mms_ltf_key_owner_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #s_mms_ltf_key_owner_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mms_ltf_key_owner_inserts
  left join p_mms_ltf_key_owner
    on #s_mms_ltf_key_owner_inserts.bk_hash = p_mms_ltf_key_owner.bk_hash
   and p_mms_ltf_key_owner.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mms_ltf_key_owner
    on p_mms_ltf_key_owner.bk_hash = s_mms_ltf_key_owner.bk_hash
   and p_mms_ltf_key_owner.s_mms_ltf_key_owner_id = s_mms_ltf_key_owner.s_mms_ltf_key_owner_id
 where s_mms_ltf_key_owner.s_mms_ltf_key_owner_id is null
    or (s_mms_ltf_key_owner.s_mms_ltf_key_owner_id is not null
        and s_mms_ltf_key_owner.dv_hash <> #s_mms_ltf_key_owner_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_mms_ltf_key_owner @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_mms_ltf_key_owner @current_dv_batch_id

end
