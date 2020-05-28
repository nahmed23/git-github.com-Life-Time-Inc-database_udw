CREATE PROC [dbo].[proc_etl_exerp_area] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_exerp_area

set @insert_date_time = getdate()
insert into dbo.stage_hash_exerp_area (
       bk_hash,
       id,
       parent_area_id,
       name,
       tree_name,
       blocked,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       id,
       parent_area_id,
       name,
       tree_name,
       blocked,
       dummy_modified_date_time,
       isnull(cast(stage_exerp_area.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_exerp_area
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_exerp_area @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_exerp_area (
       bk_hash,
       area_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_exerp_area.bk_hash,
       stage_hash_exerp_area.id area_id,
       isnull(cast(stage_hash_exerp_area.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       33,
       @insert_date_time,
       @user
  from stage_hash_exerp_area
  left join h_exerp_area
    on stage_hash_exerp_area.bk_hash = h_exerp_area.bk_hash
 where h_exerp_area_id is null
   and stage_hash_exerp_area.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_exerp_area
if object_id('tempdb..#l_exerp_area_inserts') is not null drop table #l_exerp_area_inserts
create table #l_exerp_area_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_exerp_area.bk_hash,
       stage_hash_exerp_area.id area_id,
       stage_hash_exerp_area.parent_area_id parent_area_id,
       isnull(cast(stage_hash_exerp_area.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_exerp_area.id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_area.parent_area_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_exerp_area
 where stage_hash_exerp_area.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_exerp_area records
set @insert_date_time = getdate()
insert into l_exerp_area (
       bk_hash,
       area_id,
       parent_area_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_exerp_area_inserts.bk_hash,
       #l_exerp_area_inserts.area_id,
       #l_exerp_area_inserts.parent_area_id,
       case when l_exerp_area.l_exerp_area_id is null then isnull(#l_exerp_area_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       33,
       #l_exerp_area_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_exerp_area_inserts
  left join p_exerp_area
    on #l_exerp_area_inserts.bk_hash = p_exerp_area.bk_hash
   and p_exerp_area.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_exerp_area
    on p_exerp_area.bk_hash = l_exerp_area.bk_hash
   and p_exerp_area.l_exerp_area_id = l_exerp_area.l_exerp_area_id
 where l_exerp_area.l_exerp_area_id is null
    or (l_exerp_area.l_exerp_area_id is not null
        and l_exerp_area.dv_hash <> #l_exerp_area_inserts.source_hash)

--calculate hash and lookup to current s_exerp_area
if object_id('tempdb..#s_exerp_area_inserts') is not null drop table #s_exerp_area_inserts
create table #s_exerp_area_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_exerp_area.bk_hash,
       stage_hash_exerp_area.id area_id,
       stage_hash_exerp_area.name name,
       stage_hash_exerp_area.tree_name tree_name,
       stage_hash_exerp_area.blocked blocked,
       stage_hash_exerp_area.dummy_modified_date_time dummy_modified_date_time,
       isnull(cast(stage_hash_exerp_area.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_exerp_area.id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_area.name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_area.tree_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_area.blocked as varchar(42)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_exerp_area
 where stage_hash_exerp_area.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_exerp_area records
set @insert_date_time = getdate()
insert into s_exerp_area (
       bk_hash,
       area_id,
       name,
       tree_name,
       blocked,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_exerp_area_inserts.bk_hash,
       #s_exerp_area_inserts.area_id,
       #s_exerp_area_inserts.name,
       #s_exerp_area_inserts.tree_name,
       #s_exerp_area_inserts.blocked,
       #s_exerp_area_inserts.dummy_modified_date_time,
       case when s_exerp_area.s_exerp_area_id is null then isnull(#s_exerp_area_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       33,
       #s_exerp_area_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_exerp_area_inserts
  left join p_exerp_area
    on #s_exerp_area_inserts.bk_hash = p_exerp_area.bk_hash
   and p_exerp_area.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_exerp_area
    on p_exerp_area.bk_hash = s_exerp_area.bk_hash
   and p_exerp_area.s_exerp_area_id = s_exerp_area.s_exerp_area_id
 where s_exerp_area.s_exerp_area_id is null
    or (s_exerp_area.s_exerp_area_id is not null
        and s_exerp_area.dv_hash <> #s_exerp_area_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_exerp_area @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_exerp_area @current_dv_batch_id

end
