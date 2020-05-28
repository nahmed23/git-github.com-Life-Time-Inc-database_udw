CREATE PROC [dbo].[proc_etl_crmcloudsync_team] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_crmcloudsync_Team

set @insert_date_time = getdate()
insert into dbo.stage_hash_crmcloudsync_Team (
       bk_hash,
       administratorid,
       administratoridname,
       administratoridyominame,
       businessunitid,
       businessunitidname,
       createdby,
       createdbyname,
       createdbyyominame,
       createdon,
       createdonbehalfby,
       createdonbehalfbyname,
       createdonbehalfbyyominame,
       description,
       emailaddress,
       exchangerate,
       importsequencenumber,
       isdefault,
       isdefaultname,
       ltf_telephone1,
       modifiedby,
       modifiedbyname,
       modifiedbyyominame,
       modifiedon,
       modifiedonbehalfby,
       modifiedonbehalfbyname,
       modifiedonbehalfbyyominame,
       name,
       organizationid,
       organizationidname,
       overriddencreatedon,
       processid,
       queueid,
       queueidname,
       regardingobjectid,
       regardingobjecttypecode,
       stageid,
       systemmanaged,
       systemmanagedname,
       teamid,
       teamtemplateid,
       teamtype,
       teamtypename,
       transactioncurrencyid,
       transactioncurrencyidname,
       versionnumber,
       yominame,
       InsertedDateTime,
       InsertUser,
       UpdatedDateTime,
       UpdateUser,
       ltf_teamtype,
       ltf_teamtypename,
       traversedpath,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(teamid,'z#@$k%&P'))),2) bk_hash,
       administratorid,
       administratoridname,
       administratoridyominame,
       businessunitid,
       businessunitidname,
       createdby,
       createdbyname,
       createdbyyominame,
       createdon,
       createdonbehalfby,
       createdonbehalfbyname,
       createdonbehalfbyyominame,
       description,
       emailaddress,
       exchangerate,
       importsequencenumber,
       isdefault,
       isdefaultname,
       ltf_telephone1,
       modifiedby,
       modifiedbyname,
       modifiedbyyominame,
       modifiedon,
       modifiedonbehalfby,
       modifiedonbehalfbyname,
       modifiedonbehalfbyyominame,
       name,
       organizationid,
       organizationidname,
       overriddencreatedon,
       processid,
       queueid,
       queueidname,
       regardingobjectid,
       regardingobjecttypecode,
       stageid,
       systemmanaged,
       systemmanagedname,
       teamid,
       teamtemplateid,
       teamtype,
       teamtypename,
       transactioncurrencyid,
       transactioncurrencyidname,
       versionnumber,
       yominame,
       InsertedDateTime,
       InsertUser,
       UpdatedDateTime,
       UpdateUser,
       ltf_teamtype,
       ltf_teamtypename,
       traversedpath,
       isnull(cast(stage_crmcloudsync_Team.createdon as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_crmcloudsync_Team
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_crmcloudsync_team @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_crmcloudsync_team (
       bk_hash,
       team_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_crmcloudsync_Team.bk_hash,
       stage_hash_crmcloudsync_Team.teamid team_id,
       isnull(cast(stage_hash_crmcloudsync_Team.createdon as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       3,
       @insert_date_time,
       @user
  from stage_hash_crmcloudsync_Team
  left join h_crmcloudsync_team
    on stage_hash_crmcloudsync_Team.bk_hash = h_crmcloudsync_team.bk_hash
 where h_crmcloudsync_team_id is null
   and stage_hash_crmcloudsync_Team.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_crmcloudsync_team
if object_id('tempdb..#l_crmcloudsync_team_inserts') is not null drop table #l_crmcloudsync_team_inserts
create table #l_crmcloudsync_team_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_crmcloudsync_Team.bk_hash,
       stage_hash_crmcloudsync_Team.administratorid administrator_id,
       stage_hash_crmcloudsync_Team.businessunitid business_unit_id,
       stage_hash_crmcloudsync_Team.createdby created_by,
       stage_hash_crmcloudsync_Team.createdonbehalfby created_on_behalf_by,
       stage_hash_crmcloudsync_Team.modifiedby modified_by,
       stage_hash_crmcloudsync_Team.modifiedonbehalfby modified_on_behalf_by,
       stage_hash_crmcloudsync_Team.organizationid organization_id,
       stage_hash_crmcloudsync_Team.processid process_id,
       stage_hash_crmcloudsync_Team.queueid queue_id,
       stage_hash_crmcloudsync_Team.regardingobjectid regarding_object_id,
       stage_hash_crmcloudsync_Team.stageid stage_id,
       stage_hash_crmcloudsync_Team.teamid team_id,
       stage_hash_crmcloudsync_Team.teamtemplateid team_template_id,
       stage_hash_crmcloudsync_Team.transactioncurrencyid transaction_currency_id,
       isnull(cast(stage_hash_crmcloudsync_Team.createdon as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_crmcloudsync_Team.administratorid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Team.businessunitid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Team.createdby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Team.createdonbehalfby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Team.modifiedby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Team.modifiedonbehalfby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Team.organizationid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Team.processid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Team.queueid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Team.regardingobjectid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Team.stageid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Team.teamid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Team.teamtemplateid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Team.transactioncurrencyid,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_crmcloudsync_Team
 where stage_hash_crmcloudsync_Team.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_crmcloudsync_team records
set @insert_date_time = getdate()
insert into l_crmcloudsync_team (
       bk_hash,
       administrator_id,
       business_unit_id,
       created_by,
       created_on_behalf_by,
       modified_by,
       modified_on_behalf_by,
       organization_id,
       process_id,
       queue_id,
       regarding_object_id,
       stage_id,
       team_id,
       team_template_id,
       transaction_currency_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_crmcloudsync_team_inserts.bk_hash,
       #l_crmcloudsync_team_inserts.administrator_id,
       #l_crmcloudsync_team_inserts.business_unit_id,
       #l_crmcloudsync_team_inserts.created_by,
       #l_crmcloudsync_team_inserts.created_on_behalf_by,
       #l_crmcloudsync_team_inserts.modified_by,
       #l_crmcloudsync_team_inserts.modified_on_behalf_by,
       #l_crmcloudsync_team_inserts.organization_id,
       #l_crmcloudsync_team_inserts.process_id,
       #l_crmcloudsync_team_inserts.queue_id,
       #l_crmcloudsync_team_inserts.regarding_object_id,
       #l_crmcloudsync_team_inserts.stage_id,
       #l_crmcloudsync_team_inserts.team_id,
       #l_crmcloudsync_team_inserts.team_template_id,
       #l_crmcloudsync_team_inserts.transaction_currency_id,
       case when l_crmcloudsync_team.l_crmcloudsync_team_id is null then isnull(#l_crmcloudsync_team_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       3,
       #l_crmcloudsync_team_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_crmcloudsync_team_inserts
  left join p_crmcloudsync_team
    on #l_crmcloudsync_team_inserts.bk_hash = p_crmcloudsync_team.bk_hash
   and p_crmcloudsync_team.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_crmcloudsync_team
    on p_crmcloudsync_team.bk_hash = l_crmcloudsync_team.bk_hash
   and p_crmcloudsync_team.l_crmcloudsync_team_id = l_crmcloudsync_team.l_crmcloudsync_team_id
 where l_crmcloudsync_team.l_crmcloudsync_team_id is null
    or (l_crmcloudsync_team.l_crmcloudsync_team_id is not null
        and l_crmcloudsync_team.dv_hash <> #l_crmcloudsync_team_inserts.source_hash)

--calculate hash and lookup to current s_crmcloudsync_team
if object_id('tempdb..#s_crmcloudsync_team_inserts') is not null drop table #s_crmcloudsync_team_inserts
create table #s_crmcloudsync_team_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_crmcloudsync_Team.bk_hash,
       stage_hash_crmcloudsync_Team.administratoridname administrator_id_name,
       stage_hash_crmcloudsync_Team.administratoridyominame administrator_id_yomi_name,
       stage_hash_crmcloudsync_Team.businessunitidname business_unit_id_name,
       stage_hash_crmcloudsync_Team.createdbyname created_by_name,
       stage_hash_crmcloudsync_Team.createdbyyominame created_by_yomi_name,
       stage_hash_crmcloudsync_Team.createdon created_on,
       stage_hash_crmcloudsync_Team.createdonbehalfbyname created_on_behalf_by_name,
       stage_hash_crmcloudsync_Team.createdonbehalfbyyominame created_on_behalf_by_yomi_name,
       stage_hash_crmcloudsync_Team.description description,
       stage_hash_crmcloudsync_Team.emailaddress email_address,
       stage_hash_crmcloudsync_Team.exchangerate exchange_rate,
       stage_hash_crmcloudsync_Team.importsequencenumber import_sequence_number,
       stage_hash_crmcloudsync_Team.isdefault is_default,
       stage_hash_crmcloudsync_Team.isdefaultname is_default_name,
       stage_hash_crmcloudsync_Team.ltf_telephone1 ltf_telephone_1,
       stage_hash_crmcloudsync_Team.modifiedbyname modified_by_name,
       stage_hash_crmcloudsync_Team.modifiedbyyominame modified_by_yomi_name,
       stage_hash_crmcloudsync_Team.modifiedon modified_on,
       stage_hash_crmcloudsync_Team.modifiedonbehalfbyname modified_on_behalf_by_name,
       stage_hash_crmcloudsync_Team.modifiedonbehalfbyyominame modified_on_behalf_by_yomi_name,
       stage_hash_crmcloudsync_Team.name name,
       stage_hash_crmcloudsync_Team.organizationidname organization_id_name,
       stage_hash_crmcloudsync_Team.overriddencreatedon overridden_created_on,
       stage_hash_crmcloudsync_Team.queueidname queue_id_name,
       stage_hash_crmcloudsync_Team.regardingobjecttypecode regarding_object_type_code,
       stage_hash_crmcloudsync_Team.systemmanaged system_managed,
       stage_hash_crmcloudsync_Team.systemmanagedname system_managed_name,
       stage_hash_crmcloudsync_Team.teamid team_id,
       stage_hash_crmcloudsync_Team.teamtype team_type,
       stage_hash_crmcloudsync_Team.teamtypename team_type_name,
       stage_hash_crmcloudsync_Team.transactioncurrencyidname transaction_currency_id_name,
       stage_hash_crmcloudsync_Team.versionnumber version_number,
       stage_hash_crmcloudsync_Team.yominame yomi_name,
       stage_hash_crmcloudsync_Team.InsertedDateTime inserted_date_time,
       stage_hash_crmcloudsync_Team.InsertUser insert_user,
       stage_hash_crmcloudsync_Team.UpdatedDateTime updated_date_time,
       stage_hash_crmcloudsync_Team.UpdateUser update_user,
       stage_hash_crmcloudsync_Team.ltf_teamtype ltf_team_type,
       stage_hash_crmcloudsync_Team.ltf_teamtypename ltf_team_type_name,
       stage_hash_crmcloudsync_Team.traversedpath traversed_path,
       isnull(cast(stage_hash_crmcloudsync_Team.createdon as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_crmcloudsync_Team.administratoridname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Team.administratoridyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Team.businessunitidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Team.createdbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Team.createdbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_Team.createdon,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Team.createdonbehalfbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Team.createdonbehalfbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Team.description,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Team.emailaddress,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Team.exchangerate as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Team.importsequencenumber as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Team.isdefault as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Team.isdefaultname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Team.ltf_telephone1,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Team.modifiedbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Team.modifiedbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_Team.modifiedon,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Team.modifiedonbehalfbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Team.modifiedonbehalfbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Team.name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Team.organizationidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_Team.overriddencreatedon,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Team.queueidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Team.regardingobjecttypecode,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Team.systemmanaged as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Team.systemmanagedname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Team.teamid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Team.teamtype as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Team.teamtypename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Team.transactioncurrencyidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Team.versionnumber as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Team.yominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_Team.InsertedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Team.InsertUser,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_Team.UpdatedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Team.UpdateUser,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Team.ltf_teamtype as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Team.ltf_teamtypename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Team.traversedpath,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_crmcloudsync_Team
 where stage_hash_crmcloudsync_Team.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_crmcloudsync_team records
set @insert_date_time = getdate()
insert into s_crmcloudsync_team (
       bk_hash,
       administrator_id_name,
       administrator_id_yomi_name,
       business_unit_id_name,
       created_by_name,
       created_by_yomi_name,
       created_on,
       created_on_behalf_by_name,
       created_on_behalf_by_yomi_name,
       description,
       email_address,
       exchange_rate,
       import_sequence_number,
       is_default,
       is_default_name,
       ltf_telephone_1,
       modified_by_name,
       modified_by_yomi_name,
       modified_on,
       modified_on_behalf_by_name,
       modified_on_behalf_by_yomi_name,
       name,
       organization_id_name,
       overridden_created_on,
       queue_id_name,
       regarding_object_type_code,
       system_managed,
       system_managed_name,
       team_id,
       team_type,
       team_type_name,
       transaction_currency_id_name,
       version_number,
       yomi_name,
       inserted_date_time,
       insert_user,
       updated_date_time,
       update_user,
       ltf_team_type,
       ltf_team_type_name,
       traversed_path,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_crmcloudsync_team_inserts.bk_hash,
       #s_crmcloudsync_team_inserts.administrator_id_name,
       #s_crmcloudsync_team_inserts.administrator_id_yomi_name,
       #s_crmcloudsync_team_inserts.business_unit_id_name,
       #s_crmcloudsync_team_inserts.created_by_name,
       #s_crmcloudsync_team_inserts.created_by_yomi_name,
       #s_crmcloudsync_team_inserts.created_on,
       #s_crmcloudsync_team_inserts.created_on_behalf_by_name,
       #s_crmcloudsync_team_inserts.created_on_behalf_by_yomi_name,
       #s_crmcloudsync_team_inserts.description,
       #s_crmcloudsync_team_inserts.email_address,
       #s_crmcloudsync_team_inserts.exchange_rate,
       #s_crmcloudsync_team_inserts.import_sequence_number,
       #s_crmcloudsync_team_inserts.is_default,
       #s_crmcloudsync_team_inserts.is_default_name,
       #s_crmcloudsync_team_inserts.ltf_telephone_1,
       #s_crmcloudsync_team_inserts.modified_by_name,
       #s_crmcloudsync_team_inserts.modified_by_yomi_name,
       #s_crmcloudsync_team_inserts.modified_on,
       #s_crmcloudsync_team_inserts.modified_on_behalf_by_name,
       #s_crmcloudsync_team_inserts.modified_on_behalf_by_yomi_name,
       #s_crmcloudsync_team_inserts.name,
       #s_crmcloudsync_team_inserts.organization_id_name,
       #s_crmcloudsync_team_inserts.overridden_created_on,
       #s_crmcloudsync_team_inserts.queue_id_name,
       #s_crmcloudsync_team_inserts.regarding_object_type_code,
       #s_crmcloudsync_team_inserts.system_managed,
       #s_crmcloudsync_team_inserts.system_managed_name,
       #s_crmcloudsync_team_inserts.team_id,
       #s_crmcloudsync_team_inserts.team_type,
       #s_crmcloudsync_team_inserts.team_type_name,
       #s_crmcloudsync_team_inserts.transaction_currency_id_name,
       #s_crmcloudsync_team_inserts.version_number,
       #s_crmcloudsync_team_inserts.yomi_name,
       #s_crmcloudsync_team_inserts.inserted_date_time,
       #s_crmcloudsync_team_inserts.insert_user,
       #s_crmcloudsync_team_inserts.updated_date_time,
       #s_crmcloudsync_team_inserts.update_user,
       #s_crmcloudsync_team_inserts.ltf_team_type,
       #s_crmcloudsync_team_inserts.ltf_team_type_name,
       #s_crmcloudsync_team_inserts.traversed_path,
       case when s_crmcloudsync_team.s_crmcloudsync_team_id is null then isnull(#s_crmcloudsync_team_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       3,
       #s_crmcloudsync_team_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_crmcloudsync_team_inserts
  left join p_crmcloudsync_team
    on #s_crmcloudsync_team_inserts.bk_hash = p_crmcloudsync_team.bk_hash
   and p_crmcloudsync_team.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_crmcloudsync_team
    on p_crmcloudsync_team.bk_hash = s_crmcloudsync_team.bk_hash
   and p_crmcloudsync_team.s_crmcloudsync_team_id = s_crmcloudsync_team.s_crmcloudsync_team_id
 where s_crmcloudsync_team.s_crmcloudsync_team_id is null
    or (s_crmcloudsync_team.s_crmcloudsync_team_id is not null
        and s_crmcloudsync_team.dv_hash <> #s_crmcloudsync_team_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_crmcloudsync_team @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_crmcloudsync_team @current_dv_batch_id

end
