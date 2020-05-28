CREATE PROC [dbo].[proc_etl_boss_mbr_authorized_pickups] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_boss_mbr_authorized_pickups

set @insert_date_time = getdate()
insert into dbo.stage_hash_boss_mbr_authorized_pickups (
       bk_hash,
       [id],
       cust_code,
       mbr_code,
       created_at,
       updated_at,
       notes,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast([id] as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       [id],
       cust_code,
       mbr_code,
       created_at,
       updated_at,
       notes,
       isnull(cast(stage_boss_mbr_authorized_pickups.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_boss_mbr_authorized_pickups
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_boss_mbr_authorized_pickups @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_boss_mbr_authorized_pickups (
       bk_hash,
       mbr_authorized_pickups_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_boss_mbr_authorized_pickups.bk_hash,
       stage_hash_boss_mbr_authorized_pickups.[id] mbr_authorized_pickups_id,
       isnull(cast(stage_hash_boss_mbr_authorized_pickups.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       26,
       @insert_date_time,
       @user
  from stage_hash_boss_mbr_authorized_pickups
  left join h_boss_mbr_authorized_pickups
    on stage_hash_boss_mbr_authorized_pickups.bk_hash = h_boss_mbr_authorized_pickups.bk_hash
 where h_boss_mbr_authorized_pickups_id is null
   and stage_hash_boss_mbr_authorized_pickups.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current s_boss_mbr_authorized_pickups
if object_id('tempdb..#s_boss_mbr_authorized_pickups_inserts') is not null drop table #s_boss_mbr_authorized_pickups_inserts
create table #s_boss_mbr_authorized_pickups_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_boss_mbr_authorized_pickups.bk_hash,
       stage_hash_boss_mbr_authorized_pickups.[id] mbr_authorized_pickups_id,
       stage_hash_boss_mbr_authorized_pickups.cust_code cust_code,
       stage_hash_boss_mbr_authorized_pickups.mbr_code mbr_code,
       stage_hash_boss_mbr_authorized_pickups.created_at created_at,
       stage_hash_boss_mbr_authorized_pickups.updated_at updated_at,
       stage_hash_boss_mbr_authorized_pickups.notes notes,
       isnull(cast(stage_hash_boss_mbr_authorized_pickups.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_boss_mbr_authorized_pickups.[id] as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_mbr_authorized_pickups.cust_code,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_mbr_authorized_pickups.mbr_code,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_boss_mbr_authorized_pickups.created_at,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_boss_mbr_authorized_pickups.updated_at,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_mbr_authorized_pickups.notes,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_boss_mbr_authorized_pickups
 where stage_hash_boss_mbr_authorized_pickups.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_boss_mbr_authorized_pickups records
set @insert_date_time = getdate()
insert into s_boss_mbr_authorized_pickups (
       bk_hash,
       mbr_authorized_pickups_id,
       cust_code,
       mbr_code,
       created_at,
       updated_at,
       notes,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_boss_mbr_authorized_pickups_inserts.bk_hash,
       #s_boss_mbr_authorized_pickups_inserts.mbr_authorized_pickups_id,
       #s_boss_mbr_authorized_pickups_inserts.cust_code,
       #s_boss_mbr_authorized_pickups_inserts.mbr_code,
       #s_boss_mbr_authorized_pickups_inserts.created_at,
       #s_boss_mbr_authorized_pickups_inserts.updated_at,
       #s_boss_mbr_authorized_pickups_inserts.notes,
       case when s_boss_mbr_authorized_pickups.s_boss_mbr_authorized_pickups_id is null then isnull(#s_boss_mbr_authorized_pickups_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       26,
       #s_boss_mbr_authorized_pickups_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_boss_mbr_authorized_pickups_inserts
  left join p_boss_mbr_authorized_pickups
    on #s_boss_mbr_authorized_pickups_inserts.bk_hash = p_boss_mbr_authorized_pickups.bk_hash
   and p_boss_mbr_authorized_pickups.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_boss_mbr_authorized_pickups
    on p_boss_mbr_authorized_pickups.bk_hash = s_boss_mbr_authorized_pickups.bk_hash
   and p_boss_mbr_authorized_pickups.s_boss_mbr_authorized_pickups_id = s_boss_mbr_authorized_pickups.s_boss_mbr_authorized_pickups_id
 where s_boss_mbr_authorized_pickups.s_boss_mbr_authorized_pickups_id is null
    or (s_boss_mbr_authorized_pickups.s_boss_mbr_authorized_pickups_id is not null
        and s_boss_mbr_authorized_pickups.dv_hash <> #s_boss_mbr_authorized_pickups_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_boss_mbr_authorized_pickups @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_boss_mbr_authorized_pickups @current_dv_batch_id

end
