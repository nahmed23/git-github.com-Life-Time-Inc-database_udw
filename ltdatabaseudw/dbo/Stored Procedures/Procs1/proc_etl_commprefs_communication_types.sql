CREATE PROC [dbo].[proc_etl_commprefs_communication_types] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_commprefs_CommunicationTypes

set @insert_date_time = getdate()
insert into dbo.stage_hash_commprefs_CommunicationTypes (
       bk_hash,
       [Id],
       Slug,
       Name,
       Description,
       Sequence,
       ActiveOn,
       ActiveUntil,
       CreatedTime,
       UpdatedTime,
       CommunicationCategoryId,
       OptInRequired,
       SampleImageUrl,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast([Id] as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       [Id],
       Slug,
       Name,
       Description,
       Sequence,
       ActiveOn,
       ActiveUntil,
       CreatedTime,
       UpdatedTime,
       CommunicationCategoryId,
       OptInRequired,
       SampleImageUrl,
       isnull(cast(stage_commprefs_CommunicationTypes.updatedtime as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_commprefs_CommunicationTypes
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_commprefs_communication_types @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_commprefs_communication_types (
       bk_hash,
       communication_types_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_commprefs_CommunicationTypes.bk_hash,
       stage_hash_commprefs_CommunicationTypes.[Id] communication_types_id,
       isnull(cast(stage_hash_commprefs_CommunicationTypes.updatedtime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       28,
       @insert_date_time,
       @user
  from stage_hash_commprefs_CommunicationTypes
  left join h_commprefs_communication_types
    on stage_hash_commprefs_CommunicationTypes.bk_hash = h_commprefs_communication_types.bk_hash
 where h_commprefs_communication_types_id is null
   and stage_hash_commprefs_CommunicationTypes.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_commprefs_communication_types
if object_id('tempdb..#l_commprefs_communication_types_inserts') is not null drop table #l_commprefs_communication_types_inserts
create table #l_commprefs_communication_types_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_commprefs_CommunicationTypes.bk_hash,
       stage_hash_commprefs_CommunicationTypes.[Id] communication_types_id,
       stage_hash_commprefs_CommunicationTypes.CommunicationCategoryId communication_category_id,
       isnull(cast(stage_hash_commprefs_CommunicationTypes.updatedtime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_commprefs_CommunicationTypes.[Id] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_commprefs_CommunicationTypes.CommunicationCategoryId as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_commprefs_CommunicationTypes
 where stage_hash_commprefs_CommunicationTypes.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_commprefs_communication_types records
set @insert_date_time = getdate()
insert into l_commprefs_communication_types (
       bk_hash,
       communication_types_id,
       communication_category_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_commprefs_communication_types_inserts.bk_hash,
       #l_commprefs_communication_types_inserts.communication_types_id,
       #l_commprefs_communication_types_inserts.communication_category_id,
       case when l_commprefs_communication_types.l_commprefs_communication_types_id is null then isnull(#l_commprefs_communication_types_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       28,
       #l_commprefs_communication_types_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_commprefs_communication_types_inserts
  left join p_commprefs_communication_types
    on #l_commprefs_communication_types_inserts.bk_hash = p_commprefs_communication_types.bk_hash
   and p_commprefs_communication_types.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_commprefs_communication_types
    on p_commprefs_communication_types.bk_hash = l_commprefs_communication_types.bk_hash
   and p_commprefs_communication_types.l_commprefs_communication_types_id = l_commprefs_communication_types.l_commprefs_communication_types_id
 where l_commprefs_communication_types.l_commprefs_communication_types_id is null
    or (l_commprefs_communication_types.l_commprefs_communication_types_id is not null
        and l_commprefs_communication_types.dv_hash <> #l_commprefs_communication_types_inserts.source_hash)

--calculate hash and lookup to current s_commprefs_communication_types
if object_id('tempdb..#s_commprefs_communication_types_inserts') is not null drop table #s_commprefs_communication_types_inserts
create table #s_commprefs_communication_types_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_commprefs_CommunicationTypes.bk_hash,
       stage_hash_commprefs_CommunicationTypes.[Id] communication_types_id,
       stage_hash_commprefs_CommunicationTypes.Slug slug,
       stage_hash_commprefs_CommunicationTypes.Name name,
       stage_hash_commprefs_CommunicationTypes.Description description,
       stage_hash_commprefs_CommunicationTypes.Sequence sequence,
       stage_hash_commprefs_CommunicationTypes.ActiveOn active_on,
       stage_hash_commprefs_CommunicationTypes.ActiveUntil active_until,
       stage_hash_commprefs_CommunicationTypes.CreatedTime created_time,
       stage_hash_commprefs_CommunicationTypes.UpdatedTime updated_time,
       stage_hash_commprefs_CommunicationTypes.OptInRequired opt_in_required,
       stage_hash_commprefs_CommunicationTypes.SampleImageUrl sample_image_url,
       isnull(cast(stage_hash_commprefs_CommunicationTypes.updatedtime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_commprefs_CommunicationTypes.[Id] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_commprefs_CommunicationTypes.Slug,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_commprefs_CommunicationTypes.Name,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_commprefs_CommunicationTypes.Description,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_commprefs_CommunicationTypes.Sequence as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_commprefs_CommunicationTypes.ActiveOn,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_commprefs_CommunicationTypes.ActiveUntil,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_commprefs_CommunicationTypes.CreatedTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_commprefs_CommunicationTypes.UpdatedTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_commprefs_CommunicationTypes.OptInRequired as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_commprefs_CommunicationTypes.SampleImageUrl,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_commprefs_CommunicationTypes
 where stage_hash_commprefs_CommunicationTypes.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_commprefs_communication_types records
set @insert_date_time = getdate()
insert into s_commprefs_communication_types (
       bk_hash,
       communication_types_id,
       slug,
       name,
       description,
       sequence,
       active_on,
       active_until,
       created_time,
       updated_time,
       opt_in_required,
       sample_image_url,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_commprefs_communication_types_inserts.bk_hash,
       #s_commprefs_communication_types_inserts.communication_types_id,
       #s_commprefs_communication_types_inserts.slug,
       #s_commprefs_communication_types_inserts.name,
       #s_commprefs_communication_types_inserts.description,
       #s_commprefs_communication_types_inserts.sequence,
       #s_commprefs_communication_types_inserts.active_on,
       #s_commprefs_communication_types_inserts.active_until,
       #s_commprefs_communication_types_inserts.created_time,
       #s_commprefs_communication_types_inserts.updated_time,
       #s_commprefs_communication_types_inserts.opt_in_required,
       #s_commprefs_communication_types_inserts.sample_image_url,
       case when s_commprefs_communication_types.s_commprefs_communication_types_id is null then isnull(#s_commprefs_communication_types_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       28,
       #s_commprefs_communication_types_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_commprefs_communication_types_inserts
  left join p_commprefs_communication_types
    on #s_commprefs_communication_types_inserts.bk_hash = p_commprefs_communication_types.bk_hash
   and p_commprefs_communication_types.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_commprefs_communication_types
    on p_commprefs_communication_types.bk_hash = s_commprefs_communication_types.bk_hash
   and p_commprefs_communication_types.s_commprefs_communication_types_id = s_commprefs_communication_types.s_commprefs_communication_types_id
 where s_commprefs_communication_types.s_commprefs_communication_types_id is null
    or (s_commprefs_communication_types.s_commprefs_communication_types_id is not null
        and s_commprefs_communication_types.dv_hash <> #s_commprefs_communication_types_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_commprefs_communication_types @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_commprefs_communication_types @current_dv_batch_id

end
