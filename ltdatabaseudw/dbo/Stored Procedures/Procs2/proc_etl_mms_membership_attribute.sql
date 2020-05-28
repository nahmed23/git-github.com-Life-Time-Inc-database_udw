CREATE PROC [dbo].[proc_etl_mms_membership_attribute] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_mms_MembershipAttribute

set @insert_date_time = getdate()
insert into dbo.stage_hash_mms_MembershipAttribute (
       bk_hash,
       MembershipAttributeID,
       MembershipID,
       AttributeValue,
       ValMembershipAttributeTypeID,
       InsertedDateTime,
       UpdatedDateTime,
       EffectiveFromDateTime,
       EffectiveThruDateTime,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(MembershipAttributeID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       MembershipAttributeID,
       MembershipID,
       AttributeValue,
       ValMembershipAttributeTypeID,
       InsertedDateTime,
       UpdatedDateTime,
       EffectiveFromDateTime,
       EffectiveThruDateTime,
       isnull(cast(stage_mms_MembershipAttribute.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_mms_MembershipAttribute
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_mms_membership_attribute @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_mms_membership_attribute (
       bk_hash,
       membership_attribute_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_mms_MembershipAttribute.bk_hash,
       stage_hash_mms_MembershipAttribute.MembershipAttributeID membership_attribute_id,
       isnull(cast(stage_hash_mms_MembershipAttribute.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       2,
       @insert_date_time,
       @user
  from stage_hash_mms_MembershipAttribute
  left join h_mms_membership_attribute
    on stage_hash_mms_MembershipAttribute.bk_hash = h_mms_membership_attribute.bk_hash
 where h_mms_membership_attribute_id is null
   and stage_hash_mms_MembershipAttribute.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_mms_membership_attribute
if object_id('tempdb..#l_mms_membership_attribute_inserts') is not null drop table #l_mms_membership_attribute_inserts
create table #l_mms_membership_attribute_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_MembershipAttribute.bk_hash,
       stage_hash_mms_MembershipAttribute.MembershipAttributeID membership_attribute_id,
       stage_hash_mms_MembershipAttribute.MembershipID membership_id,
       stage_hash_mms_MembershipAttribute.ValMembershipAttributeTypeID val_membership_attribute_type_id,
       isnull(cast(stage_hash_mms_MembershipAttribute.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipAttribute.MembershipAttributeID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipAttribute.MembershipID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipAttribute.ValMembershipAttributeTypeID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_MembershipAttribute
 where stage_hash_mms_MembershipAttribute.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_mms_membership_attribute records
set @insert_date_time = getdate()
insert into l_mms_membership_attribute (
       bk_hash,
       membership_attribute_id,
       membership_id,
       val_membership_attribute_type_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_mms_membership_attribute_inserts.bk_hash,
       #l_mms_membership_attribute_inserts.membership_attribute_id,
       #l_mms_membership_attribute_inserts.membership_id,
       #l_mms_membership_attribute_inserts.val_membership_attribute_type_id,
       case when l_mms_membership_attribute.l_mms_membership_attribute_id is null then isnull(#l_mms_membership_attribute_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #l_mms_membership_attribute_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_mms_membership_attribute_inserts
  left join p_mms_membership_attribute
    on #l_mms_membership_attribute_inserts.bk_hash = p_mms_membership_attribute.bk_hash
   and p_mms_membership_attribute.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_mms_membership_attribute
    on p_mms_membership_attribute.bk_hash = l_mms_membership_attribute.bk_hash
   and p_mms_membership_attribute.l_mms_membership_attribute_id = l_mms_membership_attribute.l_mms_membership_attribute_id
 where l_mms_membership_attribute.l_mms_membership_attribute_id is null
    or (l_mms_membership_attribute.l_mms_membership_attribute_id is not null
        and l_mms_membership_attribute.dv_hash <> #l_mms_membership_attribute_inserts.source_hash)

--calculate hash and lookup to current s_mms_membership_attribute
if object_id('tempdb..#s_mms_membership_attribute_inserts') is not null drop table #s_mms_membership_attribute_inserts
create table #s_mms_membership_attribute_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_MembershipAttribute.bk_hash,
       stage_hash_mms_MembershipAttribute.MembershipAttributeID membership_attribute_id,
       stage_hash_mms_MembershipAttribute.AttributeValue attribute_value,
       stage_hash_mms_MembershipAttribute.InsertedDateTime inserted_date_time,
       stage_hash_mms_MembershipAttribute.UpdatedDateTime updated_date_time,
       stage_hash_mms_MembershipAttribute.EffectiveFromDateTime effective_from_date_time,
       stage_hash_mms_MembershipAttribute.EffectiveThruDateTime effective_thru_date_time,
       isnull(cast(stage_hash_mms_MembershipAttribute.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipAttribute.MembershipAttributeID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_MembershipAttribute.AttributeValue,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_MembershipAttribute.InsertedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_MembershipAttribute.UpdatedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_MembershipAttribute.EffectiveFromDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_MembershipAttribute.EffectiveThruDateTime,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_MembershipAttribute
 where stage_hash_mms_MembershipAttribute.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mms_membership_attribute records
set @insert_date_time = getdate()
insert into s_mms_membership_attribute (
       bk_hash,
       membership_attribute_id,
       attribute_value,
       inserted_date_time,
       updated_date_time,
       effective_from_date_time,
       effective_thru_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mms_membership_attribute_inserts.bk_hash,
       #s_mms_membership_attribute_inserts.membership_attribute_id,
       #s_mms_membership_attribute_inserts.attribute_value,
       #s_mms_membership_attribute_inserts.inserted_date_time,
       #s_mms_membership_attribute_inserts.updated_date_time,
       #s_mms_membership_attribute_inserts.effective_from_date_time,
       #s_mms_membership_attribute_inserts.effective_thru_date_time,
       case when s_mms_membership_attribute.s_mms_membership_attribute_id is null then isnull(#s_mms_membership_attribute_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #s_mms_membership_attribute_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mms_membership_attribute_inserts
  left join p_mms_membership_attribute
    on #s_mms_membership_attribute_inserts.bk_hash = p_mms_membership_attribute.bk_hash
   and p_mms_membership_attribute.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mms_membership_attribute
    on p_mms_membership_attribute.bk_hash = s_mms_membership_attribute.bk_hash
   and p_mms_membership_attribute.s_mms_membership_attribute_id = s_mms_membership_attribute.s_mms_membership_attribute_id
 where s_mms_membership_attribute.s_mms_membership_attribute_id is null
    or (s_mms_membership_attribute.s_mms_membership_attribute_id is not null
        and s_mms_membership_attribute.dv_hash <> #s_mms_membership_attribute_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_mms_membership_attribute @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_mms_membership_attribute @current_dv_batch_id

end
