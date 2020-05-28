CREATE PROC [dbo].[proc_etl_crmcloudsync_annotation] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_crmcloudsync_Annotation

set @insert_date_time = getdate()
insert into dbo.stage_hash_crmcloudsync_Annotation (
       bk_hash,
       annotationid,
       createdby,
       createdbyname,
       createdbyyominame,
       createdon,
       createdonbehalfby,
       createdonbehalfbyname,
       createdonbehalfbyyominame,
       documentbody,
       filename,
       filesize,
       importsequencenumber,
       isdocument,
       isdocumentname,
       isprivatename,
       langid,
       mimetype,
       modifiedby,
       modifiedbyname,
       modifiedbyyominame,
       modifiedon,
       modifiedonbehalfby,
       modifiedonbehalfbyname,
       modifiedonbehalfbyyominame,
       notetext,
       objectid,
       objectidtypecode,
       objecttypecode,
       objecttypecodename,
       overriddencreatedon,
       ownerid,
       owneridname,
       owneridtype,
       owneridyominame,
       owningbusinessunit,
       owningteam,
       owninguser,
       stepid,
       subject,
       versionnumber,
       InsertedDateTime,
       InsertUser,
       UpdatedDateTime,
       UpdateUser,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(annotationid,'z#@$k%&P'))),2) bk_hash,
       annotationid,
       createdby,
       createdbyname,
       createdbyyominame,
       createdon,
       createdonbehalfby,
       createdonbehalfbyname,
       createdonbehalfbyyominame,
       documentbody,
       filename,
       filesize,
       importsequencenumber,
       isdocument,
       isdocumentname,
       isprivatename,
       langid,
       mimetype,
       modifiedby,
       modifiedbyname,
       modifiedbyyominame,
       modifiedon,
       modifiedonbehalfby,
       modifiedonbehalfbyname,
       modifiedonbehalfbyyominame,
       notetext,
       objectid,
       objectidtypecode,
       objecttypecode,
       objecttypecodename,
       overriddencreatedon,
       ownerid,
       owneridname,
       owneridtype,
       owneridyominame,
       owningbusinessunit,
       owningteam,
       owninguser,
       stepid,
       subject,
       versionnumber,
       InsertedDateTime,
       InsertUser,
       UpdatedDateTime,
       UpdateUser,
       isnull(cast(stage_crmcloudsync_Annotation.createdon as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_crmcloudsync_Annotation
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_crmcloudsync_annotation @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_crmcloudsync_annotation (
       bk_hash,
       annotation_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_crmcloudsync_Annotation.bk_hash,
       stage_hash_crmcloudsync_Annotation.annotationid annotation_id,
       isnull(cast(stage_hash_crmcloudsync_Annotation.createdon as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       3,
       @insert_date_time,
       @user
  from stage_hash_crmcloudsync_Annotation
  left join h_crmcloudsync_annotation
    on stage_hash_crmcloudsync_Annotation.bk_hash = h_crmcloudsync_annotation.bk_hash
 where h_crmcloudsync_annotation_id is null
   and stage_hash_crmcloudsync_Annotation.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_crmcloudsync_annotation
if object_id('tempdb..#l_crmcloudsync_annotation_inserts') is not null drop table #l_crmcloudsync_annotation_inserts
create table #l_crmcloudsync_annotation_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_crmcloudsync_Annotation.bk_hash,
       stage_hash_crmcloudsync_Annotation.annotationid annotation_id,
       stage_hash_crmcloudsync_Annotation.createdby created_by,
       stage_hash_crmcloudsync_Annotation.createdonbehalfby created_on_behalf_by,
       stage_hash_crmcloudsync_Annotation.modifiedby modified_by,
       stage_hash_crmcloudsync_Annotation.modifiedonbehalfby modified_on_behalf_by,
       stage_hash_crmcloudsync_Annotation.objectid object_id,
       stage_hash_crmcloudsync_Annotation.ownerid owner_id,
       stage_hash_crmcloudsync_Annotation.owningbusinessunit owning_business_unit,
       stage_hash_crmcloudsync_Annotation.owningteam owning_team,
       stage_hash_crmcloudsync_Annotation.owninguser owning_user,
       isnull(cast(stage_hash_crmcloudsync_Annotation.createdon as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_crmcloudsync_Annotation.annotationid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Annotation.createdby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Annotation.createdonbehalfby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Annotation.modifiedby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Annotation.modifiedonbehalfby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Annotation.objectid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Annotation.ownerid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Annotation.owningbusinessunit,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Annotation.owningteam,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Annotation.owninguser,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_crmcloudsync_Annotation
 where stage_hash_crmcloudsync_Annotation.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_crmcloudsync_annotation records
set @insert_date_time = getdate()
insert into l_crmcloudsync_annotation (
       bk_hash,
       annotation_id,
       created_by,
       created_on_behalf_by,
       modified_by,
       modified_on_behalf_by,
       object_id,
       owner_id,
       owning_business_unit,
       owning_team,
       owning_user,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_crmcloudsync_annotation_inserts.bk_hash,
       #l_crmcloudsync_annotation_inserts.annotation_id,
       #l_crmcloudsync_annotation_inserts.created_by,
       #l_crmcloudsync_annotation_inserts.created_on_behalf_by,
       #l_crmcloudsync_annotation_inserts.modified_by,
       #l_crmcloudsync_annotation_inserts.modified_on_behalf_by,
       #l_crmcloudsync_annotation_inserts.object_id,
       #l_crmcloudsync_annotation_inserts.owner_id,
       #l_crmcloudsync_annotation_inserts.owning_business_unit,
       #l_crmcloudsync_annotation_inserts.owning_team,
       #l_crmcloudsync_annotation_inserts.owning_user,
       case when l_crmcloudsync_annotation.l_crmcloudsync_annotation_id is null then isnull(#l_crmcloudsync_annotation_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       3,
       #l_crmcloudsync_annotation_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_crmcloudsync_annotation_inserts
  left join p_crmcloudsync_annotation
    on #l_crmcloudsync_annotation_inserts.bk_hash = p_crmcloudsync_annotation.bk_hash
   and p_crmcloudsync_annotation.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_crmcloudsync_annotation
    on p_crmcloudsync_annotation.bk_hash = l_crmcloudsync_annotation.bk_hash
   and p_crmcloudsync_annotation.l_crmcloudsync_annotation_id = l_crmcloudsync_annotation.l_crmcloudsync_annotation_id
 where l_crmcloudsync_annotation.l_crmcloudsync_annotation_id is null
    or (l_crmcloudsync_annotation.l_crmcloudsync_annotation_id is not null
        and l_crmcloudsync_annotation.dv_hash <> #l_crmcloudsync_annotation_inserts.source_hash)

--calculate hash and lookup to current s_crmcloudsync_annotation
if object_id('tempdb..#s_crmcloudsync_annotation_inserts') is not null drop table #s_crmcloudsync_annotation_inserts
create table #s_crmcloudsync_annotation_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_crmcloudsync_Annotation.bk_hash,
       stage_hash_crmcloudsync_Annotation.annotationid annotation_id,
       stage_hash_crmcloudsync_Annotation.createdbyname created_by_name,
       stage_hash_crmcloudsync_Annotation.createdbyyominame created_by_yomi_name,
       stage_hash_crmcloudsync_Annotation.createdon created_on,
       stage_hash_crmcloudsync_Annotation.createdonbehalfbyname created_on_behalf_by_name,
       stage_hash_crmcloudsync_Annotation.createdonbehalfbyyominame created_on_behalf_by_yomi_name,
       stage_hash_crmcloudsync_Annotation.documentbody document_body,
       stage_hash_crmcloudsync_Annotation.filename file_name,
       stage_hash_crmcloudsync_Annotation.filesize file_size,
       stage_hash_crmcloudsync_Annotation.importsequencenumber import_sequence_number,
       stage_hash_crmcloudsync_Annotation.isdocument is_document,
       stage_hash_crmcloudsync_Annotation.isdocumentname is_document_name,
       stage_hash_crmcloudsync_Annotation.isprivatename is_private_name,
       stage_hash_crmcloudsync_Annotation.langid lang_id,
       stage_hash_crmcloudsync_Annotation.mimetype mime_type,
       stage_hash_crmcloudsync_Annotation.modifiedbyname modified_by_name,
       stage_hash_crmcloudsync_Annotation.modifiedbyyominame modified_by_yomi_name,
       stage_hash_crmcloudsync_Annotation.modifiedon modified_on,
       stage_hash_crmcloudsync_Annotation.modifiedonbehalfbyname modified_on_behalf_by_name,
       stage_hash_crmcloudsync_Annotation.modifiedonbehalfbyyominame modified_on_behalf_by_yomi_name,
       stage_hash_crmcloudsync_Annotation.notetext note_text,
       stage_hash_crmcloudsync_Annotation.objectidtypecode object_id_type_code,
       stage_hash_crmcloudsync_Annotation.objecttypecode object_type_code,
       stage_hash_crmcloudsync_Annotation.objecttypecodename object_type_code_name,
       stage_hash_crmcloudsync_Annotation.overriddencreatedon over_ridden_created_on,
       stage_hash_crmcloudsync_Annotation.owneridname owner_id_name,
       stage_hash_crmcloudsync_Annotation.owneridtype owner_id_type,
       stage_hash_crmcloudsync_Annotation.owneridyominame owner_id_yomi_name,
       stage_hash_crmcloudsync_Annotation.stepid step_id,
       stage_hash_crmcloudsync_Annotation.subject subject,
       stage_hash_crmcloudsync_Annotation.versionnumber version_number,
       stage_hash_crmcloudsync_Annotation.InsertedDateTime inserted_date_time,
       stage_hash_crmcloudsync_Annotation.InsertUser insert_user,
       stage_hash_crmcloudsync_Annotation.UpdatedDateTime updated_date_time,
       stage_hash_crmcloudsync_Annotation.UpdateUser update_user,
       isnull(cast(stage_hash_crmcloudsync_Annotation.createdon as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_crmcloudsync_Annotation.annotationid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Annotation.createdbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Annotation.createdbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_Annotation.createdon,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Annotation.createdonbehalfbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Annotation.createdonbehalfbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Annotation.documentbody,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Annotation.filename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Annotation.filesize as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Annotation.importsequencenumber as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Annotation.isdocument as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Annotation.isdocumentname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Annotation.isprivatename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Annotation.langid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Annotation.mimetype,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Annotation.modifiedbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Annotation.modifiedbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_Annotation.modifiedon,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Annotation.modifiedonbehalfbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Annotation.modifiedonbehalfbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Annotation.notetext,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Annotation.objectidtypecode,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Annotation.objecttypecode,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Annotation.objecttypecodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_Annotation.overriddencreatedon,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Annotation.owneridname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Annotation.owneridtype,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Annotation.owneridyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Annotation.stepid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Annotation.subject,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Annotation.versionnumber as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_Annotation.InsertedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Annotation.InsertUser,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_Annotation.UpdatedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Annotation.UpdateUser,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_crmcloudsync_Annotation
 where stage_hash_crmcloudsync_Annotation.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_crmcloudsync_annotation records
set @insert_date_time = getdate()
insert into s_crmcloudsync_annotation (
       bk_hash,
       annotation_id,
       created_by_name,
       created_by_yomi_name,
       created_on,
       created_on_behalf_by_name,
       created_on_behalf_by_yomi_name,
       document_body,
       file_name,
       file_size,
       import_sequence_number,
       is_document,
       is_document_name,
       is_private_name,
       lang_id,
       mime_type,
       modified_by_name,
       modified_by_yomi_name,
       modified_on,
       modified_on_behalf_by_name,
       modified_on_behalf_by_yomi_name,
       note_text,
       object_id_type_code,
       object_type_code,
       object_type_code_name,
       over_ridden_created_on,
       owner_id_name,
       owner_id_type,
       owner_id_yomi_name,
       step_id,
       subject,
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
select #s_crmcloudsync_annotation_inserts.bk_hash,
       #s_crmcloudsync_annotation_inserts.annotation_id,
       #s_crmcloudsync_annotation_inserts.created_by_name,
       #s_crmcloudsync_annotation_inserts.created_by_yomi_name,
       #s_crmcloudsync_annotation_inserts.created_on,
       #s_crmcloudsync_annotation_inserts.created_on_behalf_by_name,
       #s_crmcloudsync_annotation_inserts.created_on_behalf_by_yomi_name,
       #s_crmcloudsync_annotation_inserts.document_body,
       #s_crmcloudsync_annotation_inserts.file_name,
       #s_crmcloudsync_annotation_inserts.file_size,
       #s_crmcloudsync_annotation_inserts.import_sequence_number,
       #s_crmcloudsync_annotation_inserts.is_document,
       #s_crmcloudsync_annotation_inserts.is_document_name,
       #s_crmcloudsync_annotation_inserts.is_private_name,
       #s_crmcloudsync_annotation_inserts.lang_id,
       #s_crmcloudsync_annotation_inserts.mime_type,
       #s_crmcloudsync_annotation_inserts.modified_by_name,
       #s_crmcloudsync_annotation_inserts.modified_by_yomi_name,
       #s_crmcloudsync_annotation_inserts.modified_on,
       #s_crmcloudsync_annotation_inserts.modified_on_behalf_by_name,
       #s_crmcloudsync_annotation_inserts.modified_on_behalf_by_yomi_name,
       #s_crmcloudsync_annotation_inserts.note_text,
       #s_crmcloudsync_annotation_inserts.object_id_type_code,
       #s_crmcloudsync_annotation_inserts.object_type_code,
       #s_crmcloudsync_annotation_inserts.object_type_code_name,
       #s_crmcloudsync_annotation_inserts.over_ridden_created_on,
       #s_crmcloudsync_annotation_inserts.owner_id_name,
       #s_crmcloudsync_annotation_inserts.owner_id_type,
       #s_crmcloudsync_annotation_inserts.owner_id_yomi_name,
       #s_crmcloudsync_annotation_inserts.step_id,
       #s_crmcloudsync_annotation_inserts.subject,
       #s_crmcloudsync_annotation_inserts.version_number,
       #s_crmcloudsync_annotation_inserts.inserted_date_time,
       #s_crmcloudsync_annotation_inserts.insert_user,
       #s_crmcloudsync_annotation_inserts.updated_date_time,
       #s_crmcloudsync_annotation_inserts.update_user,
       case when s_crmcloudsync_annotation.s_crmcloudsync_annotation_id is null then isnull(#s_crmcloudsync_annotation_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       3,
       #s_crmcloudsync_annotation_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_crmcloudsync_annotation_inserts
  left join p_crmcloudsync_annotation
    on #s_crmcloudsync_annotation_inserts.bk_hash = p_crmcloudsync_annotation.bk_hash
   and p_crmcloudsync_annotation.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_crmcloudsync_annotation
    on p_crmcloudsync_annotation.bk_hash = s_crmcloudsync_annotation.bk_hash
   and p_crmcloudsync_annotation.s_crmcloudsync_annotation_id = s_crmcloudsync_annotation.s_crmcloudsync_annotation_id
 where s_crmcloudsync_annotation.s_crmcloudsync_annotation_id is null
    or (s_crmcloudsync_annotation.s_crmcloudsync_annotation_id is not null
        and s_crmcloudsync_annotation.dv_hash <> #s_crmcloudsync_annotation_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_crmcloudsync_annotation @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_crmcloudsync_annotation @current_dv_batch_id

end
