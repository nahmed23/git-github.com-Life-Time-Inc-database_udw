CREATE PROC [dbo].[proc_etl_exerp_activity_group] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_exerp_activity_group

set @insert_date_time = getdate()
insert into dbo.stage_hash_exerp_activity_group (
       bk_hash,
       id,
       name,
       state,
       book_kiosk,
       book_web,
       book_api,
       book_mobile_api,
       book_client,
       parent_activity_group_id,
       external_id,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       id,
       name,
       state,
       book_kiosk,
       book_web,
       book_api,
       book_mobile_api,
       book_client,
       parent_activity_group_id,
       external_id,
       dummy_modified_date_time,
       isnull(cast(stage_exerp_activity_group.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_exerp_activity_group
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_exerp_activity_group @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_exerp_activity_group (
       bk_hash,
       activity_group_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_exerp_activity_group.bk_hash,
       stage_hash_exerp_activity_group.id activity_group_id,
       isnull(cast(stage_hash_exerp_activity_group.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       33,
       @insert_date_time,
       @user
  from stage_hash_exerp_activity_group
  left join h_exerp_activity_group
    on stage_hash_exerp_activity_group.bk_hash = h_exerp_activity_group.bk_hash
 where h_exerp_activity_group_id is null
   and stage_hash_exerp_activity_group.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_exerp_activity_group
if object_id('tempdb..#l_exerp_activity_group_inserts') is not null drop table #l_exerp_activity_group_inserts
create table #l_exerp_activity_group_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_exerp_activity_group.bk_hash,
       stage_hash_exerp_activity_group.id activity_group_id,
       stage_hash_exerp_activity_group.parent_activity_group_id parent_activity_group_id,
       isnull(cast(stage_hash_exerp_activity_group.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_exerp_activity_group.id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_activity_group.parent_activity_group_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_exerp_activity_group
 where stage_hash_exerp_activity_group.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_exerp_activity_group records
set @insert_date_time = getdate()
insert into l_exerp_activity_group (
       bk_hash,
       activity_group_id,
       parent_activity_group_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_exerp_activity_group_inserts.bk_hash,
       #l_exerp_activity_group_inserts.activity_group_id,
       #l_exerp_activity_group_inserts.parent_activity_group_id,
       case when l_exerp_activity_group.l_exerp_activity_group_id is null then isnull(#l_exerp_activity_group_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       33,
       #l_exerp_activity_group_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_exerp_activity_group_inserts
  left join p_exerp_activity_group
    on #l_exerp_activity_group_inserts.bk_hash = p_exerp_activity_group.bk_hash
   and p_exerp_activity_group.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_exerp_activity_group
    on p_exerp_activity_group.bk_hash = l_exerp_activity_group.bk_hash
   and p_exerp_activity_group.l_exerp_activity_group_id = l_exerp_activity_group.l_exerp_activity_group_id
 where l_exerp_activity_group.l_exerp_activity_group_id is null
    or (l_exerp_activity_group.l_exerp_activity_group_id is not null
        and l_exerp_activity_group.dv_hash <> #l_exerp_activity_group_inserts.source_hash)

--calculate hash and lookup to current s_exerp_activity_group
if object_id('tempdb..#s_exerp_activity_group_inserts') is not null drop table #s_exerp_activity_group_inserts
create table #s_exerp_activity_group_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_exerp_activity_group.bk_hash,
       stage_hash_exerp_activity_group.id activity_group_id,
       stage_hash_exerp_activity_group.name name,
       stage_hash_exerp_activity_group.state state,
       stage_hash_exerp_activity_group.book_kiosk book_kiosk,
       stage_hash_exerp_activity_group.book_web book_web,
       stage_hash_exerp_activity_group.book_api book_api,
       stage_hash_exerp_activity_group.book_mobile_api book_mobile_api,
       stage_hash_exerp_activity_group.book_client book_client,
       stage_hash_exerp_activity_group.dummy_modified_date_time dummy_modified_date_time,
       isnull(cast(stage_hash_exerp_activity_group.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_exerp_activity_group.id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_activity_group.name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_activity_group.state,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_activity_group.book_kiosk as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_activity_group.book_web as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_activity_group.book_api as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_activity_group.book_mobile_api as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_activity_group.book_client as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_exerp_activity_group
 where stage_hash_exerp_activity_group.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_exerp_activity_group records
set @insert_date_time = getdate()
insert into s_exerp_activity_group (
       bk_hash,
       activity_group_id,
       name,
       state,
       book_kiosk,
       book_web,
       book_api,
       book_mobile_api,
       book_client,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_exerp_activity_group_inserts.bk_hash,
       #s_exerp_activity_group_inserts.activity_group_id,
       #s_exerp_activity_group_inserts.name,
       #s_exerp_activity_group_inserts.state,
       #s_exerp_activity_group_inserts.book_kiosk,
       #s_exerp_activity_group_inserts.book_web,
       #s_exerp_activity_group_inserts.book_api,
       #s_exerp_activity_group_inserts.book_mobile_api,
       #s_exerp_activity_group_inserts.book_client,
       #s_exerp_activity_group_inserts.dummy_modified_date_time,
       case when s_exerp_activity_group.s_exerp_activity_group_id is null then isnull(#s_exerp_activity_group_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       33,
       #s_exerp_activity_group_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_exerp_activity_group_inserts
  left join p_exerp_activity_group
    on #s_exerp_activity_group_inserts.bk_hash = p_exerp_activity_group.bk_hash
   and p_exerp_activity_group.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_exerp_activity_group
    on p_exerp_activity_group.bk_hash = s_exerp_activity_group.bk_hash
   and p_exerp_activity_group.s_exerp_activity_group_id = s_exerp_activity_group.s_exerp_activity_group_id
 where s_exerp_activity_group.s_exerp_activity_group_id is null
    or (s_exerp_activity_group.s_exerp_activity_group_id is not null
        and s_exerp_activity_group.dv_hash <> #s_exerp_activity_group_inserts.source_hash)

--calculate hash and lookup to current s_exerp_activity_group_1
if object_id('tempdb..#s_exerp_activity_group_1_inserts') is not null drop table #s_exerp_activity_group_1_inserts
create table #s_exerp_activity_group_1_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_exerp_activity_group.bk_hash,
       stage_hash_exerp_activity_group.id activity_group_id,
       stage_hash_exerp_activity_group.external_id external_id,
       stage_hash_exerp_activity_group.dummy_modified_date_time dummy_modified_date_time,
       isnull(cast(stage_hash_exerp_activity_group.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_exerp_activity_group.id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_activity_group.external_id,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_exerp_activity_group
 where stage_hash_exerp_activity_group.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_exerp_activity_group_1 records
set @insert_date_time = getdate()
insert into s_exerp_activity_group_1 (
       bk_hash,
       activity_group_id,
       external_id,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_exerp_activity_group_1_inserts.bk_hash,
       #s_exerp_activity_group_1_inserts.activity_group_id,
       #s_exerp_activity_group_1_inserts.external_id,
       #s_exerp_activity_group_1_inserts.dummy_modified_date_time,
       case when s_exerp_activity_group_1.s_exerp_activity_group_1_id is null then isnull(#s_exerp_activity_group_1_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       33,
       #s_exerp_activity_group_1_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_exerp_activity_group_1_inserts
  left join p_exerp_activity_group
    on #s_exerp_activity_group_1_inserts.bk_hash = p_exerp_activity_group.bk_hash
   and p_exerp_activity_group.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_exerp_activity_group_1
    on p_exerp_activity_group.bk_hash = s_exerp_activity_group_1.bk_hash
   and p_exerp_activity_group.s_exerp_activity_group_1_id = s_exerp_activity_group_1.s_exerp_activity_group_1_id
 where s_exerp_activity_group_1.s_exerp_activity_group_1_id is null
    or (s_exerp_activity_group_1.s_exerp_activity_group_1_id is not null
        and s_exerp_activity_group_1.dv_hash <> #s_exerp_activity_group_1_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_exerp_activity_group @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_exerp_activity_group @current_dv_batch_id

end
