CREATE PROC [dbo].[proc_etl_boss_mbr_reg_status_values] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_boss_mbr_reg_status_values

set @insert_date_time = getdate()
insert into dbo.stage_hash_boss_mbr_reg_status_values (
       bk_hash,
       [id],
       cust_code,
       mbr_code,
       start_date,
       end_date,
       reg_status_type_id,
       created_at,
       updated_at,
       notes,
       value,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast([id] as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       [id],
       cust_code,
       mbr_code,
       start_date,
       end_date,
       reg_status_type_id,
       created_at,
       updated_at,
       notes,
       value,
       isnull(cast(stage_boss_mbr_reg_status_values.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_boss_mbr_reg_status_values
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_boss_mbr_reg_status_values @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_boss_mbr_reg_status_values (
       bk_hash,
       mbr_reg_status_values_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_boss_mbr_reg_status_values.bk_hash,
       stage_hash_boss_mbr_reg_status_values.[id] mbr_reg_status_values_id,
       isnull(cast(stage_hash_boss_mbr_reg_status_values.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       26,
       @insert_date_time,
       @user
  from stage_hash_boss_mbr_reg_status_values
  left join h_boss_mbr_reg_status_values
    on stage_hash_boss_mbr_reg_status_values.bk_hash = h_boss_mbr_reg_status_values.bk_hash
 where h_boss_mbr_reg_status_values_id is null
   and stage_hash_boss_mbr_reg_status_values.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_boss_mbr_reg_status_values
if object_id('tempdb..#l_boss_mbr_reg_status_values_inserts') is not null drop table #l_boss_mbr_reg_status_values_inserts
create table #l_boss_mbr_reg_status_values_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_boss_mbr_reg_status_values.bk_hash,
       stage_hash_boss_mbr_reg_status_values.[id] mbr_reg_status_values_id,
       stage_hash_boss_mbr_reg_status_values.reg_status_type_id reg_status_type_id,
       isnull(cast(stage_hash_boss_mbr_reg_status_values.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_boss_mbr_reg_status_values.[id] as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_mbr_reg_status_values.reg_status_type_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_boss_mbr_reg_status_values
 where stage_hash_boss_mbr_reg_status_values.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_boss_mbr_reg_status_values records
set @insert_date_time = getdate()
insert into l_boss_mbr_reg_status_values (
       bk_hash,
       mbr_reg_status_values_id,
       reg_status_type_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_boss_mbr_reg_status_values_inserts.bk_hash,
       #l_boss_mbr_reg_status_values_inserts.mbr_reg_status_values_id,
       #l_boss_mbr_reg_status_values_inserts.reg_status_type_id,
       case when l_boss_mbr_reg_status_values.l_boss_mbr_reg_status_values_id is null then isnull(#l_boss_mbr_reg_status_values_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       26,
       #l_boss_mbr_reg_status_values_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_boss_mbr_reg_status_values_inserts
  left join p_boss_mbr_reg_status_values
    on #l_boss_mbr_reg_status_values_inserts.bk_hash = p_boss_mbr_reg_status_values.bk_hash
   and p_boss_mbr_reg_status_values.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_boss_mbr_reg_status_values
    on p_boss_mbr_reg_status_values.bk_hash = l_boss_mbr_reg_status_values.bk_hash
   and p_boss_mbr_reg_status_values.l_boss_mbr_reg_status_values_id = l_boss_mbr_reg_status_values.l_boss_mbr_reg_status_values_id
 where l_boss_mbr_reg_status_values.l_boss_mbr_reg_status_values_id is null
    or (l_boss_mbr_reg_status_values.l_boss_mbr_reg_status_values_id is not null
        and l_boss_mbr_reg_status_values.dv_hash <> #l_boss_mbr_reg_status_values_inserts.source_hash)

--calculate hash and lookup to current s_boss_mbr_reg_status_values
if object_id('tempdb..#s_boss_mbr_reg_status_values_inserts') is not null drop table #s_boss_mbr_reg_status_values_inserts
create table #s_boss_mbr_reg_status_values_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_boss_mbr_reg_status_values.bk_hash,
       stage_hash_boss_mbr_reg_status_values.[id] mbr_reg_status_values_id,
       stage_hash_boss_mbr_reg_status_values.cust_code cust_code,
       stage_hash_boss_mbr_reg_status_values.mbr_code mbr_code,
       stage_hash_boss_mbr_reg_status_values.start_date start_date,
       stage_hash_boss_mbr_reg_status_values.end_date end_date,
       stage_hash_boss_mbr_reg_status_values.reg_status_type_id reg_status_type_id,
       stage_hash_boss_mbr_reg_status_values.created_at created_at,
       stage_hash_boss_mbr_reg_status_values.updated_at updated_at,
       stage_hash_boss_mbr_reg_status_values.notes notes,
       stage_hash_boss_mbr_reg_status_values.value value,
       isnull(cast(stage_hash_boss_mbr_reg_status_values.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_boss_mbr_reg_status_values.[id] as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_mbr_reg_status_values.cust_code,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_mbr_reg_status_values.mbr_code,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_boss_mbr_reg_status_values.start_date,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_boss_mbr_reg_status_values.end_date,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_mbr_reg_status_values.reg_status_type_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_boss_mbr_reg_status_values.created_at,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_boss_mbr_reg_status_values.updated_at,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_mbr_reg_status_values.notes,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_mbr_reg_status_values.value,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_boss_mbr_reg_status_values
 where stage_hash_boss_mbr_reg_status_values.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_boss_mbr_reg_status_values records
set @insert_date_time = getdate()
insert into s_boss_mbr_reg_status_values (
       bk_hash,
       mbr_reg_status_values_id,
       cust_code,
       mbr_code,
       start_date,
       end_date,
       reg_status_type_id,
       created_at,
       updated_at,
       notes,
       value,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_boss_mbr_reg_status_values_inserts.bk_hash,
       #s_boss_mbr_reg_status_values_inserts.mbr_reg_status_values_id,
       #s_boss_mbr_reg_status_values_inserts.cust_code,
       #s_boss_mbr_reg_status_values_inserts.mbr_code,
       #s_boss_mbr_reg_status_values_inserts.start_date,
       #s_boss_mbr_reg_status_values_inserts.end_date,
       #s_boss_mbr_reg_status_values_inserts.reg_status_type_id,
       #s_boss_mbr_reg_status_values_inserts.created_at,
       #s_boss_mbr_reg_status_values_inserts.updated_at,
       #s_boss_mbr_reg_status_values_inserts.notes,
       #s_boss_mbr_reg_status_values_inserts.value,
       case when s_boss_mbr_reg_status_values.s_boss_mbr_reg_status_values_id is null then isnull(#s_boss_mbr_reg_status_values_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       26,
       #s_boss_mbr_reg_status_values_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_boss_mbr_reg_status_values_inserts
  left join p_boss_mbr_reg_status_values
    on #s_boss_mbr_reg_status_values_inserts.bk_hash = p_boss_mbr_reg_status_values.bk_hash
   and p_boss_mbr_reg_status_values.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_boss_mbr_reg_status_values
    on p_boss_mbr_reg_status_values.bk_hash = s_boss_mbr_reg_status_values.bk_hash
   and p_boss_mbr_reg_status_values.s_boss_mbr_reg_status_values_id = s_boss_mbr_reg_status_values.s_boss_mbr_reg_status_values_id
 where s_boss_mbr_reg_status_values.s_boss_mbr_reg_status_values_id is null
    or (s_boss_mbr_reg_status_values.s_boss_mbr_reg_status_values_id is not null
        and s_boss_mbr_reg_status_values.dv_hash <> #s_boss_mbr_reg_status_values_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_boss_mbr_reg_status_values @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_boss_mbr_reg_status_values @current_dv_batch_id

end
