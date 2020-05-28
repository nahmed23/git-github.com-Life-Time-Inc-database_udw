CREATE PROC [dbo].[proc_etl_crmcloudsync_ltf_campaign_instance] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_crmcloudsync_ltf_campaigninstance

set @insert_date_time = getdate()
insert into dbo.stage_hash_crmcloudsync_ltf_campaigninstance (
       bk_hash,
       createdby,
       createdbyname,
       createdbyyominame,
       createdon,
       createdonbehalfby,
       createdonbehalfbyname,
       createdonbehalfbyyominame,
       importsequencenumber,
       ltf_campaign,
       ltf_campaigninstanceid,
       ltf_campaignname,
       ltf_club,
       ltf_clubname,
       ltf_connectwitham,
       ltf_connectwithamname,
       ltf_expirationdate,
       ltf_initialusedate,
       ltf_issuedby,
       ltf_issuedbyname,
       ltf_issuedbyyominame,
       ltf_issueddate,
       ltf_issueddays,
       ltf_issuingcontact,
       ltf_issuingcontactname,
       ltf_issuingcontactyominame,
       ltf_issuinglead,
       ltf_issuingleadname,
       ltf_issuingleadyominame,
       ltf_issuingopportunity,
       ltf_issuingopportunityname,
       ltf_name,
       ltf_passbegindate,
       ltf_qrcode,
       ltf_remainingdays,
       modifiedby,
       modifiedbyname,
       modifiedbyyominame,
       modifiedon,
       modifiedonbehalfby,
       modifiedonbehalfbyname,
       modifiedonbehalfbyyominame,
       organizationid,
       organizationidname,
       overriddencreatedon,
       statecode,
       statecodename,
       statuscode,
       statuscodename,
       timezoneruleversionnumber,
       utcconversiontimezonecode,
       versionnumber,
       InsertedDateTime,
       InsertUser,
       UpdatedDateTime,
       UpdateUser,
       ltf_prospectid,
       ltf_referringmember,
       ltf_referringmemberid,
       ltf_sendid,
       ltf_referringcorpacctid,
       ltf_referringcorpacct,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(ltf_campaigninstanceid,'z#@$k%&P'))),2) bk_hash,
       createdby,
       createdbyname,
       createdbyyominame,
       createdon,
       createdonbehalfby,
       createdonbehalfbyname,
       createdonbehalfbyyominame,
       importsequencenumber,
       ltf_campaign,
       ltf_campaigninstanceid,
       ltf_campaignname,
       ltf_club,
       ltf_clubname,
       ltf_connectwitham,
       ltf_connectwithamname,
       ltf_expirationdate,
       ltf_initialusedate,
       ltf_issuedby,
       ltf_issuedbyname,
       ltf_issuedbyyominame,
       ltf_issueddate,
       ltf_issueddays,
       ltf_issuingcontact,
       ltf_issuingcontactname,
       ltf_issuingcontactyominame,
       ltf_issuinglead,
       ltf_issuingleadname,
       ltf_issuingleadyominame,
       ltf_issuingopportunity,
       ltf_issuingopportunityname,
       ltf_name,
       ltf_passbegindate,
       ltf_qrcode,
       ltf_remainingdays,
       modifiedby,
       modifiedbyname,
       modifiedbyyominame,
       modifiedon,
       modifiedonbehalfby,
       modifiedonbehalfbyname,
       modifiedonbehalfbyyominame,
       organizationid,
       organizationidname,
       overriddencreatedon,
       statecode,
       statecodename,
       statuscode,
       statuscodename,
       timezoneruleversionnumber,
       utcconversiontimezonecode,
       versionnumber,
       InsertedDateTime,
       InsertUser,
       UpdatedDateTime,
       UpdateUser,
       ltf_prospectid,
       ltf_referringmember,
       ltf_referringmemberid,
       ltf_sendid,
       ltf_referringcorpacctid,
       ltf_referringcorpacct,
       isnull(cast(stage_crmcloudsync_ltf_campaigninstance.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_crmcloudsync_ltf_campaigninstance
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_crmcloudsync_ltf_campaign_instance @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_crmcloudsync_ltf_campaign_instance (
       bk_hash,
       ltf_campaign_instance_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_crmcloudsync_ltf_campaigninstance.bk_hash,
       stage_hash_crmcloudsync_ltf_campaigninstance.ltf_campaigninstanceid ltf_campaign_instance_id,
       isnull(cast(stage_hash_crmcloudsync_ltf_campaigninstance.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       3,
       @insert_date_time,
       @user
  from stage_hash_crmcloudsync_ltf_campaigninstance
  left join h_crmcloudsync_ltf_campaign_instance
    on stage_hash_crmcloudsync_ltf_campaigninstance.bk_hash = h_crmcloudsync_ltf_campaign_instance.bk_hash
 where h_crmcloudsync_ltf_campaign_instance_id is null
   and stage_hash_crmcloudsync_ltf_campaigninstance.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_crmcloudsync_ltf_campaign_instance
if object_id('tempdb..#l_crmcloudsync_ltf_campaign_instance_inserts') is not null drop table #l_crmcloudsync_ltf_campaign_instance_inserts
create table #l_crmcloudsync_ltf_campaign_instance_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_crmcloudsync_ltf_campaigninstance.bk_hash,
       stage_hash_crmcloudsync_ltf_campaigninstance.createdby created_by,
       stage_hash_crmcloudsync_ltf_campaigninstance.createdonbehalfby created_on_behalf_by,
       stage_hash_crmcloudsync_ltf_campaigninstance.ltf_campaign ltf_campaign,
       stage_hash_crmcloudsync_ltf_campaigninstance.ltf_campaigninstanceid ltf_campaign_instance_id,
       stage_hash_crmcloudsync_ltf_campaigninstance.ltf_club ltf_club,
       stage_hash_crmcloudsync_ltf_campaigninstance.ltf_issuedby ltf_issued_by,
       stage_hash_crmcloudsync_ltf_campaigninstance.ltf_issuingcontact ltf_issuing_contact,
       stage_hash_crmcloudsync_ltf_campaigninstance.ltf_issuinglead ltf_issuing_lead,
       stage_hash_crmcloudsync_ltf_campaigninstance.ltf_issuingopportunity ltf_issuing_opportunity,
       stage_hash_crmcloudsync_ltf_campaigninstance.modifiedby modified_by,
       stage_hash_crmcloudsync_ltf_campaigninstance.modifiedonbehalfby modified_on_behalf_by,
       stage_hash_crmcloudsync_ltf_campaigninstance.organizationid organization_id,
       stage_hash_crmcloudsync_ltf_campaigninstance.ltf_prospectid ltf_prospect_id,
       stage_hash_crmcloudsync_ltf_campaigninstance.ltf_referringmember ltf_referring_member,
       stage_hash_crmcloudsync_ltf_campaigninstance.ltf_referringmemberid ltf_referring_member_id,
       stage_hash_crmcloudsync_ltf_campaigninstance.ltf_sendid ltf_send_id,
       stage_hash_crmcloudsync_ltf_campaigninstance.ltf_referringcorpacctid ltf_referring_corpacct_id,
       stage_hash_crmcloudsync_ltf_campaigninstance.ltf_referringcorpacct ltf_referring_corpacct,
       isnull(cast(stage_hash_crmcloudsync_ltf_campaigninstance.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_campaigninstance.createdby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_campaigninstance.createdonbehalfby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_campaigninstance.ltf_campaign,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_campaigninstance.ltf_campaigninstanceid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_campaigninstance.ltf_club,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_campaigninstance.ltf_issuedby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_campaigninstance.ltf_issuingcontact,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_campaigninstance.ltf_issuinglead,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_campaigninstance.ltf_issuingopportunity,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_campaigninstance.modifiedby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_campaigninstance.modifiedonbehalfby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_campaigninstance.organizationid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_campaigninstance.ltf_prospectid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_campaigninstance.ltf_referringmember,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_campaigninstance.ltf_referringmemberid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_campaigninstance.ltf_sendid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_campaigninstance.ltf_referringcorpacctid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_campaigninstance.ltf_referringcorpacct,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_crmcloudsync_ltf_campaigninstance
 where stage_hash_crmcloudsync_ltf_campaigninstance.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_crmcloudsync_ltf_campaign_instance records
set @insert_date_time = getdate()
insert into l_crmcloudsync_ltf_campaign_instance (
       bk_hash,
       created_by,
       created_on_behalf_by,
       ltf_campaign,
       ltf_campaign_instance_id,
       ltf_club,
       ltf_issued_by,
       ltf_issuing_contact,
       ltf_issuing_lead,
       ltf_issuing_opportunity,
       modified_by,
       modified_on_behalf_by,
       organization_id,
       ltf_prospect_id,
       ltf_referring_member,
       ltf_referring_member_id,
       ltf_send_id,
       ltf_referring_corpacct_id,
       ltf_referring_corpacct,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_crmcloudsync_ltf_campaign_instance_inserts.bk_hash,
       #l_crmcloudsync_ltf_campaign_instance_inserts.created_by,
       #l_crmcloudsync_ltf_campaign_instance_inserts.created_on_behalf_by,
       #l_crmcloudsync_ltf_campaign_instance_inserts.ltf_campaign,
       #l_crmcloudsync_ltf_campaign_instance_inserts.ltf_campaign_instance_id,
       #l_crmcloudsync_ltf_campaign_instance_inserts.ltf_club,
       #l_crmcloudsync_ltf_campaign_instance_inserts.ltf_issued_by,
       #l_crmcloudsync_ltf_campaign_instance_inserts.ltf_issuing_contact,
       #l_crmcloudsync_ltf_campaign_instance_inserts.ltf_issuing_lead,
       #l_crmcloudsync_ltf_campaign_instance_inserts.ltf_issuing_opportunity,
       #l_crmcloudsync_ltf_campaign_instance_inserts.modified_by,
       #l_crmcloudsync_ltf_campaign_instance_inserts.modified_on_behalf_by,
       #l_crmcloudsync_ltf_campaign_instance_inserts.organization_id,
       #l_crmcloudsync_ltf_campaign_instance_inserts.ltf_prospect_id,
       #l_crmcloudsync_ltf_campaign_instance_inserts.ltf_referring_member,
       #l_crmcloudsync_ltf_campaign_instance_inserts.ltf_referring_member_id,
       #l_crmcloudsync_ltf_campaign_instance_inserts.ltf_send_id,
       #l_crmcloudsync_ltf_campaign_instance_inserts.ltf_referring_corpacct_id,
       #l_crmcloudsync_ltf_campaign_instance_inserts.ltf_referring_corpacct,
       case when l_crmcloudsync_ltf_campaign_instance.l_crmcloudsync_ltf_campaign_instance_id is null then isnull(#l_crmcloudsync_ltf_campaign_instance_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       3,
       #l_crmcloudsync_ltf_campaign_instance_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_crmcloudsync_ltf_campaign_instance_inserts
  left join p_crmcloudsync_ltf_campaign_instance
    on #l_crmcloudsync_ltf_campaign_instance_inserts.bk_hash = p_crmcloudsync_ltf_campaign_instance.bk_hash
   and p_crmcloudsync_ltf_campaign_instance.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_crmcloudsync_ltf_campaign_instance
    on p_crmcloudsync_ltf_campaign_instance.bk_hash = l_crmcloudsync_ltf_campaign_instance.bk_hash
   and p_crmcloudsync_ltf_campaign_instance.l_crmcloudsync_ltf_campaign_instance_id = l_crmcloudsync_ltf_campaign_instance.l_crmcloudsync_ltf_campaign_instance_id
 where l_crmcloudsync_ltf_campaign_instance.l_crmcloudsync_ltf_campaign_instance_id is null
    or (l_crmcloudsync_ltf_campaign_instance.l_crmcloudsync_ltf_campaign_instance_id is not null
        and l_crmcloudsync_ltf_campaign_instance.dv_hash <> #l_crmcloudsync_ltf_campaign_instance_inserts.source_hash)

--calculate hash and lookup to current s_crmcloudsync_ltf_campaign_instance
if object_id('tempdb..#s_crmcloudsync_ltf_campaign_instance_inserts') is not null drop table #s_crmcloudsync_ltf_campaign_instance_inserts
create table #s_crmcloudsync_ltf_campaign_instance_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_crmcloudsync_ltf_campaigninstance.bk_hash,
       stage_hash_crmcloudsync_ltf_campaigninstance.createdbyname created_by_name,
       stage_hash_crmcloudsync_ltf_campaigninstance.createdbyyominame created_by_yomi_name,
       stage_hash_crmcloudsync_ltf_campaigninstance.createdon created_on,
       stage_hash_crmcloudsync_ltf_campaigninstance.createdonbehalfbyname created_on_behalf_by_name,
       stage_hash_crmcloudsync_ltf_campaigninstance.createdonbehalfbyyominame created_on_behalf_by_yomi_name,
       stage_hash_crmcloudsync_ltf_campaigninstance.importsequencenumber import_sequence_number,
       stage_hash_crmcloudsync_ltf_campaigninstance.ltf_campaigninstanceid ltf_campaign_instance_id,
       stage_hash_crmcloudsync_ltf_campaigninstance.ltf_campaignname ltf_campaign_name,
       stage_hash_crmcloudsync_ltf_campaigninstance.ltf_clubname ltf_club_name,
       stage_hash_crmcloudsync_ltf_campaigninstance.ltf_connectwitham ltf_connect_witham,
       stage_hash_crmcloudsync_ltf_campaigninstance.ltf_connectwithamname ltf_connect_witham_name,
       stage_hash_crmcloudsync_ltf_campaigninstance.ltf_expirationdate ltf_expiration_date,
       stage_hash_crmcloudsync_ltf_campaigninstance.ltf_initialusedate ltf_initial_use_date,
       stage_hash_crmcloudsync_ltf_campaigninstance.ltf_issuedbyname ltf_issued_by_name,
       stage_hash_crmcloudsync_ltf_campaigninstance.ltf_issuedbyyominame ltf_issued_by_yomi_name,
       stage_hash_crmcloudsync_ltf_campaigninstance.ltf_issueddate ltf_issued_date,
       stage_hash_crmcloudsync_ltf_campaigninstance.ltf_issueddays ltf_issued_days,
       stage_hash_crmcloudsync_ltf_campaigninstance.ltf_issuingcontactname ltf_issuing_contact_name,
       stage_hash_crmcloudsync_ltf_campaigninstance.ltf_issuingcontactyominame ltf_issuing_contact_yomi_name,
       stage_hash_crmcloudsync_ltf_campaigninstance.ltf_issuingleadname ltf_issuing_lead_name,
       stage_hash_crmcloudsync_ltf_campaigninstance.ltf_issuingleadyominame ltf_issuing_lead_yomi_name,
       stage_hash_crmcloudsync_ltf_campaigninstance.ltf_issuingopportunityname ltf_issuing_opportunity_name,
       stage_hash_crmcloudsync_ltf_campaigninstance.ltf_name ltf_name,
       stage_hash_crmcloudsync_ltf_campaigninstance.ltf_passbegindate ltf_pass_begin_date,
       stage_hash_crmcloudsync_ltf_campaigninstance.ltf_qrcode ltf_qr_code,
       stage_hash_crmcloudsync_ltf_campaigninstance.ltf_remainingdays ltf_remaining_days,
       stage_hash_crmcloudsync_ltf_campaigninstance.modifiedbyname modified_by_name,
       stage_hash_crmcloudsync_ltf_campaigninstance.modifiedbyyominame modified_by_yomi_name,
       stage_hash_crmcloudsync_ltf_campaigninstance.modifiedon modified_on,
       stage_hash_crmcloudsync_ltf_campaigninstance.modifiedonbehalfbyname modified_on_behalf_by_name,
       stage_hash_crmcloudsync_ltf_campaigninstance.modifiedonbehalfbyyominame modified_on_behalf_by_yomi_name,
       stage_hash_crmcloudsync_ltf_campaigninstance.organizationidname organization_id_name,
       stage_hash_crmcloudsync_ltf_campaigninstance.overriddencreatedon overridden_created_on,
       stage_hash_crmcloudsync_ltf_campaigninstance.statecode state_code,
       stage_hash_crmcloudsync_ltf_campaigninstance.statecodename state_code_name,
       stage_hash_crmcloudsync_ltf_campaigninstance.statuscode status_code,
       stage_hash_crmcloudsync_ltf_campaigninstance.statuscodename status_code_name,
       stage_hash_crmcloudsync_ltf_campaigninstance.timezoneruleversionnumber time_zone_rule_version_number,
       stage_hash_crmcloudsync_ltf_campaigninstance.utcconversiontimezonecode utc_conversion_time_zone_code,
       stage_hash_crmcloudsync_ltf_campaigninstance.versionnumber version_number,
       stage_hash_crmcloudsync_ltf_campaigninstance.InsertedDateTime inserted_date_time,
       stage_hash_crmcloudsync_ltf_campaigninstance.InsertUser insert_user,
       stage_hash_crmcloudsync_ltf_campaigninstance.UpdatedDateTime updated_date_time,
       stage_hash_crmcloudsync_ltf_campaigninstance.UpdateUser update_user,
       isnull(cast(stage_hash_crmcloudsync_ltf_campaigninstance.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_campaigninstance.createdbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_campaigninstance.createdbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_ltf_campaigninstance.createdon,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_campaigninstance.createdonbehalfbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_campaigninstance.createdonbehalfbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltf_campaigninstance.importsequencenumber as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_campaigninstance.ltf_campaigninstanceid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_campaigninstance.ltf_campaignname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_campaigninstance.ltf_clubname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltf_campaigninstance.ltf_connectwitham as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_campaigninstance.ltf_connectwithamname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_ltf_campaigninstance.ltf_expirationdate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_ltf_campaigninstance.ltf_initialusedate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_campaigninstance.ltf_issuedbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_campaigninstance.ltf_issuedbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_ltf_campaigninstance.ltf_issueddate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltf_campaigninstance.ltf_issueddays as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_campaigninstance.ltf_issuingcontactname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_campaigninstance.ltf_issuingcontactyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_campaigninstance.ltf_issuingleadname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_campaigninstance.ltf_issuingleadyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_campaigninstance.ltf_issuingopportunityname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_campaigninstance.ltf_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_ltf_campaigninstance.ltf_passbegindate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_campaigninstance.ltf_qrcode,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltf_campaigninstance.ltf_remainingdays as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_campaigninstance.modifiedbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_campaigninstance.modifiedbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_ltf_campaigninstance.modifiedon,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_campaigninstance.modifiedonbehalfbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_campaigninstance.modifiedonbehalfbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_campaigninstance.organizationidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_ltf_campaigninstance.overriddencreatedon,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltf_campaigninstance.statecode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_campaigninstance.statecodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltf_campaigninstance.statuscode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_campaigninstance.statuscodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltf_campaigninstance.timezoneruleversionnumber as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltf_campaigninstance.utcconversiontimezonecode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltf_campaigninstance.versionnumber as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_ltf_campaigninstance.InsertedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_campaigninstance.InsertUser,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_ltf_campaigninstance.UpdatedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_campaigninstance.UpdateUser,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_crmcloudsync_ltf_campaigninstance
 where stage_hash_crmcloudsync_ltf_campaigninstance.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_crmcloudsync_ltf_campaign_instance records
set @insert_date_time = getdate()
insert into s_crmcloudsync_ltf_campaign_instance (
       bk_hash,
       created_by_name,
       created_by_yomi_name,
       created_on,
       created_on_behalf_by_name,
       created_on_behalf_by_yomi_name,
       import_sequence_number,
       ltf_campaign_instance_id,
       ltf_campaign_name,
       ltf_club_name,
       ltf_connect_witham,
       ltf_connect_witham_name,
       ltf_expiration_date,
       ltf_initial_use_date,
       ltf_issued_by_name,
       ltf_issued_by_yomi_name,
       ltf_issued_date,
       ltf_issued_days,
       ltf_issuing_contact_name,
       ltf_issuing_contact_yomi_name,
       ltf_issuing_lead_name,
       ltf_issuing_lead_yomi_name,
       ltf_issuing_opportunity_name,
       ltf_name,
       ltf_pass_begin_date,
       ltf_qr_code,
       ltf_remaining_days,
       modified_by_name,
       modified_by_yomi_name,
       modified_on,
       modified_on_behalf_by_name,
       modified_on_behalf_by_yomi_name,
       organization_id_name,
       overridden_created_on,
       state_code,
       state_code_name,
       status_code,
       status_code_name,
       time_zone_rule_version_number,
       utc_conversion_time_zone_code,
       version_number,
       inserted_date_time,
       insert_user,
       updated_date_time,
       update_user,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_crmcloudsync_ltf_campaign_instance_inserts.bk_hash,
       #s_crmcloudsync_ltf_campaign_instance_inserts.created_by_name,
       #s_crmcloudsync_ltf_campaign_instance_inserts.created_by_yomi_name,
       #s_crmcloudsync_ltf_campaign_instance_inserts.created_on,
       #s_crmcloudsync_ltf_campaign_instance_inserts.created_on_behalf_by_name,
       #s_crmcloudsync_ltf_campaign_instance_inserts.created_on_behalf_by_yomi_name,
       #s_crmcloudsync_ltf_campaign_instance_inserts.import_sequence_number,
       #s_crmcloudsync_ltf_campaign_instance_inserts.ltf_campaign_instance_id,
       #s_crmcloudsync_ltf_campaign_instance_inserts.ltf_campaign_name,
       #s_crmcloudsync_ltf_campaign_instance_inserts.ltf_club_name,
       #s_crmcloudsync_ltf_campaign_instance_inserts.ltf_connect_witham,
       #s_crmcloudsync_ltf_campaign_instance_inserts.ltf_connect_witham_name,
       #s_crmcloudsync_ltf_campaign_instance_inserts.ltf_expiration_date,
       #s_crmcloudsync_ltf_campaign_instance_inserts.ltf_initial_use_date,
       #s_crmcloudsync_ltf_campaign_instance_inserts.ltf_issued_by_name,
       #s_crmcloudsync_ltf_campaign_instance_inserts.ltf_issued_by_yomi_name,
       #s_crmcloudsync_ltf_campaign_instance_inserts.ltf_issued_date,
       #s_crmcloudsync_ltf_campaign_instance_inserts.ltf_issued_days,
       #s_crmcloudsync_ltf_campaign_instance_inserts.ltf_issuing_contact_name,
       #s_crmcloudsync_ltf_campaign_instance_inserts.ltf_issuing_contact_yomi_name,
       #s_crmcloudsync_ltf_campaign_instance_inserts.ltf_issuing_lead_name,
       #s_crmcloudsync_ltf_campaign_instance_inserts.ltf_issuing_lead_yomi_name,
       #s_crmcloudsync_ltf_campaign_instance_inserts.ltf_issuing_opportunity_name,
       #s_crmcloudsync_ltf_campaign_instance_inserts.ltf_name,
       #s_crmcloudsync_ltf_campaign_instance_inserts.ltf_pass_begin_date,
       #s_crmcloudsync_ltf_campaign_instance_inserts.ltf_qr_code,
       #s_crmcloudsync_ltf_campaign_instance_inserts.ltf_remaining_days,
       #s_crmcloudsync_ltf_campaign_instance_inserts.modified_by_name,
       #s_crmcloudsync_ltf_campaign_instance_inserts.modified_by_yomi_name,
       #s_crmcloudsync_ltf_campaign_instance_inserts.modified_on,
       #s_crmcloudsync_ltf_campaign_instance_inserts.modified_on_behalf_by_name,
       #s_crmcloudsync_ltf_campaign_instance_inserts.modified_on_behalf_by_yomi_name,
       #s_crmcloudsync_ltf_campaign_instance_inserts.organization_id_name,
       #s_crmcloudsync_ltf_campaign_instance_inserts.overridden_created_on,
       #s_crmcloudsync_ltf_campaign_instance_inserts.state_code,
       #s_crmcloudsync_ltf_campaign_instance_inserts.state_code_name,
       #s_crmcloudsync_ltf_campaign_instance_inserts.status_code,
       #s_crmcloudsync_ltf_campaign_instance_inserts.status_code_name,
       #s_crmcloudsync_ltf_campaign_instance_inserts.time_zone_rule_version_number,
       #s_crmcloudsync_ltf_campaign_instance_inserts.utc_conversion_time_zone_code,
       #s_crmcloudsync_ltf_campaign_instance_inserts.version_number,
       #s_crmcloudsync_ltf_campaign_instance_inserts.inserted_date_time,
       #s_crmcloudsync_ltf_campaign_instance_inserts.insert_user,
       #s_crmcloudsync_ltf_campaign_instance_inserts.updated_date_time,
       #s_crmcloudsync_ltf_campaign_instance_inserts.update_user,
       case when s_crmcloudsync_ltf_campaign_instance.s_crmcloudsync_ltf_campaign_instance_id is null then isnull(#s_crmcloudsync_ltf_campaign_instance_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       3,
       #s_crmcloudsync_ltf_campaign_instance_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_crmcloudsync_ltf_campaign_instance_inserts
  left join p_crmcloudsync_ltf_campaign_instance
    on #s_crmcloudsync_ltf_campaign_instance_inserts.bk_hash = p_crmcloudsync_ltf_campaign_instance.bk_hash
   and p_crmcloudsync_ltf_campaign_instance.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_crmcloudsync_ltf_campaign_instance
    on p_crmcloudsync_ltf_campaign_instance.bk_hash = s_crmcloudsync_ltf_campaign_instance.bk_hash
   and p_crmcloudsync_ltf_campaign_instance.s_crmcloudsync_ltf_campaign_instance_id = s_crmcloudsync_ltf_campaign_instance.s_crmcloudsync_ltf_campaign_instance_id
 where s_crmcloudsync_ltf_campaign_instance.s_crmcloudsync_ltf_campaign_instance_id is null
    or (s_crmcloudsync_ltf_campaign_instance.s_crmcloudsync_ltf_campaign_instance_id is not null
        and s_crmcloudsync_ltf_campaign_instance.dv_hash <> #s_crmcloudsync_ltf_campaign_instance_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_crmcloudsync_ltf_campaign_instance @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_crmcloudsync_ltf_campaign_instance @current_dv_batch_id

end
