CREATE PROC [dbo].[proc_etl_boss_interest] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_boss_interest

set @insert_date_time = getdate()
insert into dbo.stage_hash_boss_interest (
       bk_hash,
       [id],
       short_desc,
       long_desc,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast([id] as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       [id],
       short_desc,
       long_desc,
       dummy_modified_date_time,
       isnull(cast(stage_boss_interest.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_boss_interest
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_boss_interest @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_boss_interest (
       bk_hash,
       interest_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_boss_interest.bk_hash,
       stage_hash_boss_interest.[id] interest_id,
       isnull(cast(stage_hash_boss_interest.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       26,
       @insert_date_time,
       @user
  from stage_hash_boss_interest
  left join h_boss_interest
    on stage_hash_boss_interest.bk_hash = h_boss_interest.bk_hash
 where h_boss_interest_id is null
   and stage_hash_boss_interest.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current s_boss_interest
if object_id('tempdb..#s_boss_interest_inserts') is not null drop table #s_boss_interest_inserts
create table #s_boss_interest_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_boss_interest.bk_hash,
       stage_hash_boss_interest.[id] interest_id,
       stage_hash_boss_interest.short_desc short_desc,
       stage_hash_boss_interest.long_desc long_desc,
       stage_hash_boss_interest.dummy_modified_date_time dummy_modified_date_time,
       isnull(cast(stage_hash_boss_interest.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_boss_interest.[id] as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_interest.short_desc,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_interest.long_desc,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_boss_interest
 where stage_hash_boss_interest.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_boss_interest records
set @insert_date_time = getdate()
insert into s_boss_interest (
       bk_hash,
       interest_id,
       short_desc,
       long_desc,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_boss_interest_inserts.bk_hash,
       #s_boss_interest_inserts.interest_id,
       #s_boss_interest_inserts.short_desc,
       #s_boss_interest_inserts.long_desc,
       #s_boss_interest_inserts.dummy_modified_date_time,
       case when s_boss_interest.s_boss_interest_id is null then isnull(#s_boss_interest_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       26,
       #s_boss_interest_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_boss_interest_inserts
  left join p_boss_interest
    on #s_boss_interest_inserts.bk_hash = p_boss_interest.bk_hash
   and p_boss_interest.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_boss_interest
    on p_boss_interest.bk_hash = s_boss_interest.bk_hash
   and p_boss_interest.s_boss_interest_id = s_boss_interest.s_boss_interest_id
 where s_boss_interest.s_boss_interest_id is null
    or (s_boss_interest.s_boss_interest_id is not null
        and s_boss_interest.dv_hash <> #s_boss_interest_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_boss_interest @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_boss_interest @current_dv_batch_id

end
