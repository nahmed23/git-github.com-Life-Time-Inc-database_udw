CREATE PROC [dbo].[proc_etl_crmcloudsync_ltf_do_not_contact_store] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_crmcloudsync_ltf_do_not_contact_store

set @insert_date_time = getdate()
insert into dbo.stage_hash_crmcloudsync_ltf_do_not_contact_store (
       bk_hash,
       createdby,
       createdbyname,
       createdbyyominame,
       createdon,
       createdonbehalfby,
       createdonbehalfbyname,
       createdonbehalfbyyominame,
       importsequencenumber,
       ltf_address1_city,
       ltf_address1_county,
       ltf_address1_line1,
       ltf_address1_line1_country,
       ltf_address1_line2,
       ltf_address1_line3,
       ltf_address1_postalcode,
       ltf_address1_stateorprovince,
       ltf_contactlookup,
       ltf_contactlookupname,
       ltf_contactlookupyominame,
       ltf_do_not_contact_store_id,
       ltf_emailaddress1,
       ltf_emailaddress1_dncsource,
       ltf_emailaddress1_dncsourcename,
       ltf_emailaddress2,
       ltf_emailaddress2_dncsource,
       ltf_emailaddress2_dncsourcename,
       ltf_leadlookup,
       ltf_leadlookupname,
       ltf_leadlookupyominame,
       ltf_mobilephone,
       ltf_mobilephone_dncsource,
       ltf_mobilephone_dncsourcename,
       ltf_name,
       ltf_telephone1,
       ltf_telephone1_dncsource,
       ltf_telephone1_dncsourcename,
       ltf_telephone2,
       ltf_telephone2_dncsource,
       ltf_telephone2_dncsourcename,
       ltf_telephone3,
       ltf_telephone3_dncsource,
       ltf_telephone3_dncsourcename,
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
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(ltf_do_not_contact_store_id,'z#@$k%&P'))),2) bk_hash,
       createdby,
       createdbyname,
       createdbyyominame,
       createdon,
       createdonbehalfby,
       createdonbehalfbyname,
       createdonbehalfbyyominame,
       importsequencenumber,
       ltf_address1_city,
       ltf_address1_county,
       ltf_address1_line1,
       ltf_address1_line1_country,
       ltf_address1_line2,
       ltf_address1_line3,
       ltf_address1_postalcode,
       ltf_address1_stateorprovince,
       ltf_contactlookup,
       ltf_contactlookupname,
       ltf_contactlookupyominame,
       ltf_do_not_contact_store_id,
       ltf_emailaddress1,
       ltf_emailaddress1_dncsource,
       ltf_emailaddress1_dncsourcename,
       ltf_emailaddress2,
       ltf_emailaddress2_dncsource,
       ltf_emailaddress2_dncsourcename,
       ltf_leadlookup,
       ltf_leadlookupname,
       ltf_leadlookupyominame,
       ltf_mobilephone,
       ltf_mobilephone_dncsource,
       ltf_mobilephone_dncsourcename,
       ltf_name,
       ltf_telephone1,
       ltf_telephone1_dncsource,
       ltf_telephone1_dncsourcename,
       ltf_telephone2,
       ltf_telephone2_dncsource,
       ltf_telephone2_dncsourcename,
       ltf_telephone3,
       ltf_telephone3_dncsource,
       ltf_telephone3_dncsourcename,
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
       isnull(cast(stage_crmcloudsync_ltf_do_not_contact_store.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_crmcloudsync_ltf_do_not_contact_store
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_crmcloudsync_ltf_do_not_contact_store @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_crmcloudsync_ltf_do_not_contact_store (
       bk_hash,
       ltf_do_not_contact_store_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_crmcloudsync_ltf_do_not_contact_store.bk_hash,
       stage_hash_crmcloudsync_ltf_do_not_contact_store.ltf_do_not_contact_store_id ltf_do_not_contact_store_id,
       isnull(cast(stage_hash_crmcloudsync_ltf_do_not_contact_store.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       3,
       @insert_date_time,
       @user
  from stage_hash_crmcloudsync_ltf_do_not_contact_store
  left join h_crmcloudsync_ltf_do_not_contact_store
    on stage_hash_crmcloudsync_ltf_do_not_contact_store.bk_hash = h_crmcloudsync_ltf_do_not_contact_store.bk_hash
 where h_crmcloudsync_ltf_do_not_contact_store_id is null
   and stage_hash_crmcloudsync_ltf_do_not_contact_store.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_crmcloudsync_ltf_do_not_contact_store
if object_id('tempdb..#l_crmcloudsync_ltf_do_not_contact_store_inserts') is not null drop table #l_crmcloudsync_ltf_do_not_contact_store_inserts
create table #l_crmcloudsync_ltf_do_not_contact_store_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_crmcloudsync_ltf_do_not_contact_store.bk_hash,
       stage_hash_crmcloudsync_ltf_do_not_contact_store.createdby created_by,
       stage_hash_crmcloudsync_ltf_do_not_contact_store.importsequencenumber import_sequence_number,
       stage_hash_crmcloudsync_ltf_do_not_contact_store.ltf_do_not_contact_store_id ltf_do_not_contact_store_id,
       stage_hash_crmcloudsync_ltf_do_not_contact_store.modifiedby modifiedby,
       stage_hash_crmcloudsync_ltf_do_not_contact_store.organizationid organization_id,
       isnull(cast(stage_hash_crmcloudsync_ltf_do_not_contact_store.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_do_not_contact_store.createdby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltf_do_not_contact_store.importsequencenumber as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_do_not_contact_store.ltf_do_not_contact_store_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_do_not_contact_store.modifiedby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_do_not_contact_store.organizationid,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_crmcloudsync_ltf_do_not_contact_store
 where stage_hash_crmcloudsync_ltf_do_not_contact_store.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_crmcloudsync_ltf_do_not_contact_store records
set @insert_date_time = getdate()
insert into l_crmcloudsync_ltf_do_not_contact_store (
       bk_hash,
       created_by,
       import_sequence_number,
       ltf_do_not_contact_store_id,
       modifiedby,
       organization_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_crmcloudsync_ltf_do_not_contact_store_inserts.bk_hash,
       #l_crmcloudsync_ltf_do_not_contact_store_inserts.created_by,
       #l_crmcloudsync_ltf_do_not_contact_store_inserts.import_sequence_number,
       #l_crmcloudsync_ltf_do_not_contact_store_inserts.ltf_do_not_contact_store_id,
       #l_crmcloudsync_ltf_do_not_contact_store_inserts.modifiedby,
       #l_crmcloudsync_ltf_do_not_contact_store_inserts.organization_id,
       case when l_crmcloudsync_ltf_do_not_contact_store.l_crmcloudsync_ltf_do_not_contact_store_id is null then isnull(#l_crmcloudsync_ltf_do_not_contact_store_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       3,
       #l_crmcloudsync_ltf_do_not_contact_store_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_crmcloudsync_ltf_do_not_contact_store_inserts
  left join p_crmcloudsync_ltf_do_not_contact_store
    on #l_crmcloudsync_ltf_do_not_contact_store_inserts.bk_hash = p_crmcloudsync_ltf_do_not_contact_store.bk_hash
   and p_crmcloudsync_ltf_do_not_contact_store.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_crmcloudsync_ltf_do_not_contact_store
    on p_crmcloudsync_ltf_do_not_contact_store.bk_hash = l_crmcloudsync_ltf_do_not_contact_store.bk_hash
   and p_crmcloudsync_ltf_do_not_contact_store.l_crmcloudsync_ltf_do_not_contact_store_id = l_crmcloudsync_ltf_do_not_contact_store.l_crmcloudsync_ltf_do_not_contact_store_id
 where l_crmcloudsync_ltf_do_not_contact_store.l_crmcloudsync_ltf_do_not_contact_store_id is null
    or (l_crmcloudsync_ltf_do_not_contact_store.l_crmcloudsync_ltf_do_not_contact_store_id is not null
        and l_crmcloudsync_ltf_do_not_contact_store.dv_hash <> #l_crmcloudsync_ltf_do_not_contact_store_inserts.source_hash)

--calculate hash and lookup to current s_crmcloudsync_ltf_do_not_contact_store
if object_id('tempdb..#s_crmcloudsync_ltf_do_not_contact_store_inserts') is not null drop table #s_crmcloudsync_ltf_do_not_contact_store_inserts
create table #s_crmcloudsync_ltf_do_not_contact_store_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_crmcloudsync_ltf_do_not_contact_store.bk_hash,
       stage_hash_crmcloudsync_ltf_do_not_contact_store.createdbyname created_by_name,
       stage_hash_crmcloudsync_ltf_do_not_contact_store.createdbyyominame created_by_yomi_name,
       stage_hash_crmcloudsync_ltf_do_not_contact_store.createdon createdon,
       stage_hash_crmcloudsync_ltf_do_not_contact_store.createdonbehalfby created_on_behalf_by,
       stage_hash_crmcloudsync_ltf_do_not_contact_store.createdonbehalfbyname created_on_behalf_by_name,
       stage_hash_crmcloudsync_ltf_do_not_contact_store.createdonbehalfbyyominame created_on_behalf_by_yomi_name,
       stage_hash_crmcloudsync_ltf_do_not_contact_store.ltf_address1_city ltf_address1_city,
       stage_hash_crmcloudsync_ltf_do_not_contact_store.ltf_address1_county ltf_address1_county,
       stage_hash_crmcloudsync_ltf_do_not_contact_store.ltf_address1_line1 ltf_address1_line1,
       stage_hash_crmcloudsync_ltf_do_not_contact_store.ltf_address1_line1_country ltf_address1_line1_country,
       stage_hash_crmcloudsync_ltf_do_not_contact_store.ltf_address1_line2 ltf_address1_line2,
       stage_hash_crmcloudsync_ltf_do_not_contact_store.ltf_address1_line3 ltf_address1_line3,
       stage_hash_crmcloudsync_ltf_do_not_contact_store.ltf_address1_postalcode ltf_address1_postal_code,
       stage_hash_crmcloudsync_ltf_do_not_contact_store.ltf_address1_stateorprovince ltf_address1_state_or_province,
       stage_hash_crmcloudsync_ltf_do_not_contact_store.ltf_contactlookup ltf_contact_lookup,
       stage_hash_crmcloudsync_ltf_do_not_contact_store.ltf_contactlookupname ltf_contact_lookup_name,
       stage_hash_crmcloudsync_ltf_do_not_contact_store.ltf_contactlookupyominame ltf_contact_lookup_yomi_name,
       stage_hash_crmcloudsync_ltf_do_not_contact_store.ltf_do_not_contact_store_id ltf_do_not_contact_store_id,
       stage_hash_crmcloudsync_ltf_do_not_contact_store.ltf_emailaddress1 ltf_email_address1,
       stage_hash_crmcloudsync_ltf_do_not_contact_store.ltf_emailaddress1_dncsource ltf_email_address1_dncsource,
       stage_hash_crmcloudsync_ltf_do_not_contact_store.ltf_emailaddress1_dncsourcename ltf_email_address1_dncsourcename,
       stage_hash_crmcloudsync_ltf_do_not_contact_store.ltf_emailaddress2 ltf_email_address2,
       stage_hash_crmcloudsync_ltf_do_not_contact_store.ltf_emailaddress2_dncsource ltf_email_address2_dncsource,
       stage_hash_crmcloudsync_ltf_do_not_contact_store.ltf_emailaddress2_dncsourcename ltf_email_address2_dncsourcename,
       stage_hash_crmcloudsync_ltf_do_not_contact_store.ltf_leadlookup ltf_lead_lookup,
       stage_hash_crmcloudsync_ltf_do_not_contact_store.ltf_leadlookupname ltf_lead_lookup_name,
       stage_hash_crmcloudsync_ltf_do_not_contact_store.ltf_leadlookupyominame ltf_lead_lookup_yomi_name,
       stage_hash_crmcloudsync_ltf_do_not_contact_store.ltf_mobilephone ltf_mobile_phone,
       stage_hash_crmcloudsync_ltf_do_not_contact_store.ltf_mobilephone_dncsource ltf_mobile_phone_dnc_source,
       stage_hash_crmcloudsync_ltf_do_not_contact_store.ltf_mobilephone_dncsourcename ltf_mobile_phone_dnc_source_name,
       stage_hash_crmcloudsync_ltf_do_not_contact_store.ltf_name ltf_name,
       stage_hash_crmcloudsync_ltf_do_not_contact_store.ltf_telephone1 ltf_telephone1,
       stage_hash_crmcloudsync_ltf_do_not_contact_store.ltf_telephone1_dncsource ltf_telephone1_dnc_source,
       stage_hash_crmcloudsync_ltf_do_not_contact_store.ltf_telephone1_dncsourcename ltf_telephone1_dnc_source_name,
       stage_hash_crmcloudsync_ltf_do_not_contact_store.ltf_telephone2 ltf_telephone2,
       stage_hash_crmcloudsync_ltf_do_not_contact_store.ltf_telephone2_dncsource ltf_telephone2_dnc_source,
       stage_hash_crmcloudsync_ltf_do_not_contact_store.ltf_telephone2_dncsourcename ltf_telephone2_dnc_source_name,
       stage_hash_crmcloudsync_ltf_do_not_contact_store.ltf_telephone3 ltf_telephone3,
       stage_hash_crmcloudsync_ltf_do_not_contact_store.ltf_telephone3_dncsource ltf_telephone3_dncsource,
       stage_hash_crmcloudsync_ltf_do_not_contact_store.ltf_telephone3_dncsourcename ltf_telephone3_dncsourcename,
       stage_hash_crmcloudsync_ltf_do_not_contact_store.modifiedbyname modified_by_name,
       stage_hash_crmcloudsync_ltf_do_not_contact_store.modifiedbyyominame modified_by_yomi_name,
       stage_hash_crmcloudsync_ltf_do_not_contact_store.modifiedon modifiedon,
       stage_hash_crmcloudsync_ltf_do_not_contact_store.modifiedonbehalfby modified_on_behalf_by,
       stage_hash_crmcloudsync_ltf_do_not_contact_store.modifiedonbehalfbyname modified_on_behalf_by_name,
       stage_hash_crmcloudsync_ltf_do_not_contact_store.modifiedonbehalfbyyominame modified_on_behalf_by_yomi_name,
       stage_hash_crmcloudsync_ltf_do_not_contact_store.organizationidname organization_id_name,
       stage_hash_crmcloudsync_ltf_do_not_contact_store.overriddencreatedon overridden_created_on,
       stage_hash_crmcloudsync_ltf_do_not_contact_store.statecode state_code,
       stage_hash_crmcloudsync_ltf_do_not_contact_store.statecodename state_code_name,
       stage_hash_crmcloudsync_ltf_do_not_contact_store.statuscode status_code,
       stage_hash_crmcloudsync_ltf_do_not_contact_store.statuscodename status_code_name,
       stage_hash_crmcloudsync_ltf_do_not_contact_store.timezoneruleversionnumber time_zone_rule_version_number,
       stage_hash_crmcloudsync_ltf_do_not_contact_store.utcconversiontimezonecode utc_conversion_time_zone_code,
       stage_hash_crmcloudsync_ltf_do_not_contact_store.versionnumber version_number,
       stage_hash_crmcloudsync_ltf_do_not_contact_store.InsertedDateTime inserted_date_time,
       stage_hash_crmcloudsync_ltf_do_not_contact_store.InsertUser insert_user,
       stage_hash_crmcloudsync_ltf_do_not_contact_store.UpdatedDateTime updated_date_time,
       stage_hash_crmcloudsync_ltf_do_not_contact_store.UpdateUser update_user,
       isnull(cast(stage_hash_crmcloudsync_ltf_do_not_contact_store.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_do_not_contact_store.createdbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_do_not_contact_store.createdbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_ltf_do_not_contact_store.createdon,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_do_not_contact_store.createdonbehalfby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_do_not_contact_store.createdonbehalfbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_do_not_contact_store.createdonbehalfbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_do_not_contact_store.ltf_address1_city,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_do_not_contact_store.ltf_address1_county,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_do_not_contact_store.ltf_address1_line1,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_do_not_contact_store.ltf_address1_line1_country,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_do_not_contact_store.ltf_address1_line2,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_do_not_contact_store.ltf_address1_line3,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_do_not_contact_store.ltf_address1_postalcode,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_do_not_contact_store.ltf_address1_stateorprovince,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_do_not_contact_store.ltf_contactlookup,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_do_not_contact_store.ltf_contactlookupname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_do_not_contact_store.ltf_contactlookupyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_do_not_contact_store.ltf_do_not_contact_store_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_do_not_contact_store.ltf_emailaddress1,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltf_do_not_contact_store.ltf_emailaddress1_dncsource as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_do_not_contact_store.ltf_emailaddress1_dncsourcename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_do_not_contact_store.ltf_emailaddress2,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltf_do_not_contact_store.ltf_emailaddress2_dncsource as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_do_not_contact_store.ltf_emailaddress2_dncsourcename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_do_not_contact_store.ltf_leadlookup,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_do_not_contact_store.ltf_leadlookupname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_do_not_contact_store.ltf_leadlookupyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_do_not_contact_store.ltf_mobilephone,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltf_do_not_contact_store.ltf_mobilephone_dncsource as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_do_not_contact_store.ltf_mobilephone_dncsourcename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_do_not_contact_store.ltf_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_do_not_contact_store.ltf_telephone1,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltf_do_not_contact_store.ltf_telephone1_dncsource as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_do_not_contact_store.ltf_telephone1_dncsourcename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_do_not_contact_store.ltf_telephone2,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltf_do_not_contact_store.ltf_telephone2_dncsource as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_do_not_contact_store.ltf_telephone2_dncsourcename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_do_not_contact_store.ltf_telephone3,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltf_do_not_contact_store.ltf_telephone3_dncsource as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_do_not_contact_store.ltf_telephone3_dncsourcename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_do_not_contact_store.modifiedbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_do_not_contact_store.modifiedbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_ltf_do_not_contact_store.modifiedon,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_do_not_contact_store.modifiedonbehalfby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_do_not_contact_store.modifiedonbehalfbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_do_not_contact_store.modifiedonbehalfbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_do_not_contact_store.organizationidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_ltf_do_not_contact_store.overriddencreatedon,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltf_do_not_contact_store.statecode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_do_not_contact_store.statecodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltf_do_not_contact_store.statuscode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_do_not_contact_store.statuscodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltf_do_not_contact_store.timezoneruleversionnumber as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltf_do_not_contact_store.utcconversiontimezonecode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltf_do_not_contact_store.versionnumber as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_ltf_do_not_contact_store.InsertedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_do_not_contact_store.InsertUser,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_ltf_do_not_contact_store.UpdatedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_do_not_contact_store.UpdateUser,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_crmcloudsync_ltf_do_not_contact_store
 where stage_hash_crmcloudsync_ltf_do_not_contact_store.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_crmcloudsync_ltf_do_not_contact_store records
set @insert_date_time = getdate()
insert into s_crmcloudsync_ltf_do_not_contact_store (
       bk_hash,
       created_by_name,
       created_by_yomi_name,
       createdon,
       created_on_behalf_by,
       created_on_behalf_by_name,
       created_on_behalf_by_yomi_name,
       ltf_address1_city,
       ltf_address1_county,
       ltf_address1_line1,
       ltf_address1_line1_country,
       ltf_address1_line2,
       ltf_address1_line3,
       ltf_address1_postal_code,
       ltf_address1_state_or_province,
       ltf_contact_lookup,
       ltf_contact_lookup_name,
       ltf_contact_lookup_yomi_name,
       ltf_do_not_contact_store_id,
       ltf_email_address1,
       ltf_email_address1_dncsource,
       ltf_email_address1_dncsourcename,
       ltf_email_address2,
       ltf_email_address2_dncsource,
       ltf_email_address2_dncsourcename,
       ltf_lead_lookup,
       ltf_lead_lookup_name,
       ltf_lead_lookup_yomi_name,
       ltf_mobile_phone,
       ltf_mobile_phone_dnc_source,
       ltf_mobile_phone_dnc_source_name,
       ltf_name,
       ltf_telephone1,
       ltf_telephone1_dnc_source,
       ltf_telephone1_dnc_source_name,
       ltf_telephone2,
       ltf_telephone2_dnc_source,
       ltf_telephone2_dnc_source_name,
       ltf_telephone3,
       ltf_telephone3_dncsource,
       ltf_telephone3_dncsourcename,
       modified_by_name,
       modified_by_yomi_name,
       modifiedon,
       modified_on_behalf_by,
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
select #s_crmcloudsync_ltf_do_not_contact_store_inserts.bk_hash,
       #s_crmcloudsync_ltf_do_not_contact_store_inserts.created_by_name,
       #s_crmcloudsync_ltf_do_not_contact_store_inserts.created_by_yomi_name,
       #s_crmcloudsync_ltf_do_not_contact_store_inserts.createdon,
       #s_crmcloudsync_ltf_do_not_contact_store_inserts.created_on_behalf_by,
       #s_crmcloudsync_ltf_do_not_contact_store_inserts.created_on_behalf_by_name,
       #s_crmcloudsync_ltf_do_not_contact_store_inserts.created_on_behalf_by_yomi_name,
       #s_crmcloudsync_ltf_do_not_contact_store_inserts.ltf_address1_city,
       #s_crmcloudsync_ltf_do_not_contact_store_inserts.ltf_address1_county,
       #s_crmcloudsync_ltf_do_not_contact_store_inserts.ltf_address1_line1,
       #s_crmcloudsync_ltf_do_not_contact_store_inserts.ltf_address1_line1_country,
       #s_crmcloudsync_ltf_do_not_contact_store_inserts.ltf_address1_line2,
       #s_crmcloudsync_ltf_do_not_contact_store_inserts.ltf_address1_line3,
       #s_crmcloudsync_ltf_do_not_contact_store_inserts.ltf_address1_postal_code,
       #s_crmcloudsync_ltf_do_not_contact_store_inserts.ltf_address1_state_or_province,
       #s_crmcloudsync_ltf_do_not_contact_store_inserts.ltf_contact_lookup,
       #s_crmcloudsync_ltf_do_not_contact_store_inserts.ltf_contact_lookup_name,
       #s_crmcloudsync_ltf_do_not_contact_store_inserts.ltf_contact_lookup_yomi_name,
       #s_crmcloudsync_ltf_do_not_contact_store_inserts.ltf_do_not_contact_store_id,
       #s_crmcloudsync_ltf_do_not_contact_store_inserts.ltf_email_address1,
       #s_crmcloudsync_ltf_do_not_contact_store_inserts.ltf_email_address1_dncsource,
       #s_crmcloudsync_ltf_do_not_contact_store_inserts.ltf_email_address1_dncsourcename,
       #s_crmcloudsync_ltf_do_not_contact_store_inserts.ltf_email_address2,
       #s_crmcloudsync_ltf_do_not_contact_store_inserts.ltf_email_address2_dncsource,
       #s_crmcloudsync_ltf_do_not_contact_store_inserts.ltf_email_address2_dncsourcename,
       #s_crmcloudsync_ltf_do_not_contact_store_inserts.ltf_lead_lookup,
       #s_crmcloudsync_ltf_do_not_contact_store_inserts.ltf_lead_lookup_name,
       #s_crmcloudsync_ltf_do_not_contact_store_inserts.ltf_lead_lookup_yomi_name,
       #s_crmcloudsync_ltf_do_not_contact_store_inserts.ltf_mobile_phone,
       #s_crmcloudsync_ltf_do_not_contact_store_inserts.ltf_mobile_phone_dnc_source,
       #s_crmcloudsync_ltf_do_not_contact_store_inserts.ltf_mobile_phone_dnc_source_name,
       #s_crmcloudsync_ltf_do_not_contact_store_inserts.ltf_name,
       #s_crmcloudsync_ltf_do_not_contact_store_inserts.ltf_telephone1,
       #s_crmcloudsync_ltf_do_not_contact_store_inserts.ltf_telephone1_dnc_source,
       #s_crmcloudsync_ltf_do_not_contact_store_inserts.ltf_telephone1_dnc_source_name,
       #s_crmcloudsync_ltf_do_not_contact_store_inserts.ltf_telephone2,
       #s_crmcloudsync_ltf_do_not_contact_store_inserts.ltf_telephone2_dnc_source,
       #s_crmcloudsync_ltf_do_not_contact_store_inserts.ltf_telephone2_dnc_source_name,
       #s_crmcloudsync_ltf_do_not_contact_store_inserts.ltf_telephone3,
       #s_crmcloudsync_ltf_do_not_contact_store_inserts.ltf_telephone3_dncsource,
       #s_crmcloudsync_ltf_do_not_contact_store_inserts.ltf_telephone3_dncsourcename,
       #s_crmcloudsync_ltf_do_not_contact_store_inserts.modified_by_name,
       #s_crmcloudsync_ltf_do_not_contact_store_inserts.modified_by_yomi_name,
       #s_crmcloudsync_ltf_do_not_contact_store_inserts.modifiedon,
       #s_crmcloudsync_ltf_do_not_contact_store_inserts.modified_on_behalf_by,
       #s_crmcloudsync_ltf_do_not_contact_store_inserts.modified_on_behalf_by_name,
       #s_crmcloudsync_ltf_do_not_contact_store_inserts.modified_on_behalf_by_yomi_name,
       #s_crmcloudsync_ltf_do_not_contact_store_inserts.organization_id_name,
       #s_crmcloudsync_ltf_do_not_contact_store_inserts.overridden_created_on,
       #s_crmcloudsync_ltf_do_not_contact_store_inserts.state_code,
       #s_crmcloudsync_ltf_do_not_contact_store_inserts.state_code_name,
       #s_crmcloudsync_ltf_do_not_contact_store_inserts.status_code,
       #s_crmcloudsync_ltf_do_not_contact_store_inserts.status_code_name,
       #s_crmcloudsync_ltf_do_not_contact_store_inserts.time_zone_rule_version_number,
       #s_crmcloudsync_ltf_do_not_contact_store_inserts.utc_conversion_time_zone_code,
       #s_crmcloudsync_ltf_do_not_contact_store_inserts.version_number,
       #s_crmcloudsync_ltf_do_not_contact_store_inserts.inserted_date_time,
       #s_crmcloudsync_ltf_do_not_contact_store_inserts.insert_user,
       #s_crmcloudsync_ltf_do_not_contact_store_inserts.updated_date_time,
       #s_crmcloudsync_ltf_do_not_contact_store_inserts.update_user,
       case when s_crmcloudsync_ltf_do_not_contact_store.s_crmcloudsync_ltf_do_not_contact_store_id is null then isnull(#s_crmcloudsync_ltf_do_not_contact_store_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       3,
       #s_crmcloudsync_ltf_do_not_contact_store_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_crmcloudsync_ltf_do_not_contact_store_inserts
  left join p_crmcloudsync_ltf_do_not_contact_store
    on #s_crmcloudsync_ltf_do_not_contact_store_inserts.bk_hash = p_crmcloudsync_ltf_do_not_contact_store.bk_hash
   and p_crmcloudsync_ltf_do_not_contact_store.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_crmcloudsync_ltf_do_not_contact_store
    on p_crmcloudsync_ltf_do_not_contact_store.bk_hash = s_crmcloudsync_ltf_do_not_contact_store.bk_hash
   and p_crmcloudsync_ltf_do_not_contact_store.s_crmcloudsync_ltf_do_not_contact_store_id = s_crmcloudsync_ltf_do_not_contact_store.s_crmcloudsync_ltf_do_not_contact_store_id
 where s_crmcloudsync_ltf_do_not_contact_store.s_crmcloudsync_ltf_do_not_contact_store_id is null
    or (s_crmcloudsync_ltf_do_not_contact_store.s_crmcloudsync_ltf_do_not_contact_store_id is not null
        and s_crmcloudsync_ltf_do_not_contact_store.dv_hash <> #s_crmcloudsync_ltf_do_not_contact_store_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_crmcloudsync_ltf_do_not_contact_store @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_crmcloudsync_ltf_do_not_contact_store @current_dv_batch_id

end
