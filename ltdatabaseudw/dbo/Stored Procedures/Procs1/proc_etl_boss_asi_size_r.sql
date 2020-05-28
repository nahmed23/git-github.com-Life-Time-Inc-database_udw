CREATE PROC [dbo].[proc_etl_boss_asi_size_r] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_boss_asisizer

set @insert_date_time = getdate()
insert into dbo.stage_hash_boss_asisizer (
       bk_hash,
       sizer_dept,
       sizer_class,
       sizer_code,
       sizer_desc,
       sizer_seq,
       sizer_class_id,
       [id],
       jan_one,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(sizer_dept as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(sizer_class as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(sizer_code,'z#@$k%&P'))),2) bk_hash,
       sizer_dept,
       sizer_class,
       sizer_code,
       sizer_desc,
       sizer_seq,
       sizer_class_id,
       [id],
       jan_one,
       isnull(cast(stage_boss_asisizer.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_boss_asisizer
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_boss_asi_size_r @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_boss_asi_size_r (
       bk_hash,
       size_r_dept,
       size_r_class,
       size_r_code,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_boss_asisizer.bk_hash,
       stage_hash_boss_asisizer.sizer_dept size_r_dept,
       stage_hash_boss_asisizer.sizer_class size_r_class,
       stage_hash_boss_asisizer.sizer_code size_r_code,
       isnull(cast(stage_hash_boss_asisizer.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       26,
       @insert_date_time,
       @user
  from stage_hash_boss_asisizer
  left join h_boss_asi_size_r
    on stage_hash_boss_asisizer.bk_hash = h_boss_asi_size_r.bk_hash
 where h_boss_asi_size_r_id is null
   and stage_hash_boss_asisizer.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_boss_asi_size_r
if object_id('tempdb..#l_boss_asi_size_r_inserts') is not null drop table #l_boss_asi_size_r_inserts
create table #l_boss_asi_size_r_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_boss_asisizer.bk_hash,
       stage_hash_boss_asisizer.sizer_dept size_r_dept,
       stage_hash_boss_asisizer.sizer_class size_r_class,
       stage_hash_boss_asisizer.sizer_code size_r_code,
       stage_hash_boss_asisizer.sizer_class_id size_r_class_id,
       stage_hash_boss_asisizer.[id] asi_size_r_id,
       isnull(cast(stage_hash_boss_asisizer.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_boss_asisizer.sizer_dept as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_boss_asisizer.sizer_class as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_boss_asisizer.sizer_code,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_boss_asisizer.sizer_class_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_boss_asisizer.[id] as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_boss_asisizer
 where stage_hash_boss_asisizer.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_boss_asi_size_r records
set @insert_date_time = getdate()
insert into l_boss_asi_size_r (
       bk_hash,
       size_r_dept,
       size_r_class,
       size_r_code,
       size_r_class_id,
       asi_size_r_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_boss_asi_size_r_inserts.bk_hash,
       #l_boss_asi_size_r_inserts.size_r_dept,
       #l_boss_asi_size_r_inserts.size_r_class,
       #l_boss_asi_size_r_inserts.size_r_code,
       #l_boss_asi_size_r_inserts.size_r_class_id,
       #l_boss_asi_size_r_inserts.asi_size_r_id,
       case when l_boss_asi_size_r.l_boss_asi_size_r_id is null then isnull(#l_boss_asi_size_r_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       26,
       #l_boss_asi_size_r_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_boss_asi_size_r_inserts
  left join p_boss_asi_size_r
    on #l_boss_asi_size_r_inserts.bk_hash = p_boss_asi_size_r.bk_hash
   and p_boss_asi_size_r.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_boss_asi_size_r
    on p_boss_asi_size_r.bk_hash = l_boss_asi_size_r.bk_hash
   and p_boss_asi_size_r.l_boss_asi_size_r_id = l_boss_asi_size_r.l_boss_asi_size_r_id
 where l_boss_asi_size_r.l_boss_asi_size_r_id is null
    or (l_boss_asi_size_r.l_boss_asi_size_r_id is not null
        and l_boss_asi_size_r.dv_hash <> #l_boss_asi_size_r_inserts.source_hash)

--calculate hash and lookup to current s_boss_asi_size_r
if object_id('tempdb..#s_boss_asi_size_r_inserts') is not null drop table #s_boss_asi_size_r_inserts
create table #s_boss_asi_size_r_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_boss_asisizer.bk_hash,
       stage_hash_boss_asisizer.sizer_dept size_r_dept,
       stage_hash_boss_asisizer.sizer_class size_r_class,
       stage_hash_boss_asisizer.sizer_code size_r_code,
       stage_hash_boss_asisizer.sizer_desc size_r_desc,
       stage_hash_boss_asisizer.sizer_seq size_r_seq,
       stage_hash_boss_asisizer.jan_one jan_one,
       isnull(cast(stage_hash_boss_asisizer.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_boss_asisizer.sizer_dept as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_boss_asisizer.sizer_class as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_boss_asisizer.sizer_code,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_boss_asisizer.sizer_desc,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_boss_asisizer.sizer_seq as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_boss_asisizer.jan_one,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_boss_asisizer
 where stage_hash_boss_asisizer.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_boss_asi_size_r records
set @insert_date_time = getdate()
insert into s_boss_asi_size_r (
       bk_hash,
       size_r_dept,
       size_r_class,
       size_r_code,
       size_r_desc,
       size_r_seq,
       jan_one,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_boss_asi_size_r_inserts.bk_hash,
       #s_boss_asi_size_r_inserts.size_r_dept,
       #s_boss_asi_size_r_inserts.size_r_class,
       #s_boss_asi_size_r_inserts.size_r_code,
       #s_boss_asi_size_r_inserts.size_r_desc,
       #s_boss_asi_size_r_inserts.size_r_seq,
       #s_boss_asi_size_r_inserts.jan_one,
       case when s_boss_asi_size_r.s_boss_asi_size_r_id is null then isnull(#s_boss_asi_size_r_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       26,
       #s_boss_asi_size_r_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_boss_asi_size_r_inserts
  left join p_boss_asi_size_r
    on #s_boss_asi_size_r_inserts.bk_hash = p_boss_asi_size_r.bk_hash
   and p_boss_asi_size_r.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_boss_asi_size_r
    on p_boss_asi_size_r.bk_hash = s_boss_asi_size_r.bk_hash
   and p_boss_asi_size_r.s_boss_asi_size_r_id = s_boss_asi_size_r.s_boss_asi_size_r_id
 where s_boss_asi_size_r.s_boss_asi_size_r_id is null
    or (s_boss_asi_size_r.s_boss_asi_size_r_id is not null
        and s_boss_asi_size_r.dv_hash <> #s_boss_asi_size_r_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_boss_asi_size_r @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_boss_asi_size_r @current_dv_batch_id

end
