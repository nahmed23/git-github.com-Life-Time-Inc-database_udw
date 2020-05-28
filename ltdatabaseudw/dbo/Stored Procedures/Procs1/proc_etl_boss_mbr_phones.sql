CREATE PROC [dbo].[proc_etl_boss_mbr_phones] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_boss_mbr_phones

set @insert_date_time = getdate()
insert into dbo.stage_hash_boss_mbr_phones (
       bk_hash,
       [id],
       area_code,
       number,
       ext,
       ph_type,
       contact_id,
       created_at,
       updated_at,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast([id] as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       [id],
       area_code,
       number,
       ext,
       ph_type,
       contact_id,
       created_at,
       updated_at,
       isnull(cast(stage_boss_mbr_phones.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_boss_mbr_phones
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_boss_mbr_phones @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_boss_mbr_phones (
       bk_hash,
       mbr_phones_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_boss_mbr_phones.bk_hash,
       stage_hash_boss_mbr_phones.[id] mbr_phones_id,
       isnull(cast(stage_hash_boss_mbr_phones.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       26,
       @insert_date_time,
       @user
  from stage_hash_boss_mbr_phones
  left join h_boss_mbr_phones
    on stage_hash_boss_mbr_phones.bk_hash = h_boss_mbr_phones.bk_hash
 where h_boss_mbr_phones_id is null
   and stage_hash_boss_mbr_phones.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_boss_mbr_phones
if object_id('tempdb..#l_boss_mbr_phones_inserts') is not null drop table #l_boss_mbr_phones_inserts
create table #l_boss_mbr_phones_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_boss_mbr_phones.bk_hash,
       stage_hash_boss_mbr_phones.[id] mbr_phones_id,
       stage_hash_boss_mbr_phones.contact_id contact_id,
       isnull(cast(stage_hash_boss_mbr_phones.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_boss_mbr_phones.[id] as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_mbr_phones.contact_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_boss_mbr_phones
 where stage_hash_boss_mbr_phones.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_boss_mbr_phones records
set @insert_date_time = getdate()
insert into l_boss_mbr_phones (
       bk_hash,
       mbr_phones_id,
       contact_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_boss_mbr_phones_inserts.bk_hash,
       #l_boss_mbr_phones_inserts.mbr_phones_id,
       #l_boss_mbr_phones_inserts.contact_id,
       case when l_boss_mbr_phones.l_boss_mbr_phones_id is null then isnull(#l_boss_mbr_phones_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       26,
       #l_boss_mbr_phones_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_boss_mbr_phones_inserts
  left join p_boss_mbr_phones
    on #l_boss_mbr_phones_inserts.bk_hash = p_boss_mbr_phones.bk_hash
   and p_boss_mbr_phones.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_boss_mbr_phones
    on p_boss_mbr_phones.bk_hash = l_boss_mbr_phones.bk_hash
   and p_boss_mbr_phones.l_boss_mbr_phones_id = l_boss_mbr_phones.l_boss_mbr_phones_id
 where l_boss_mbr_phones.l_boss_mbr_phones_id is null
    or (l_boss_mbr_phones.l_boss_mbr_phones_id is not null
        and l_boss_mbr_phones.dv_hash <> #l_boss_mbr_phones_inserts.source_hash)

--calculate hash and lookup to current s_boss_mbr_phones
if object_id('tempdb..#s_boss_mbr_phones_inserts') is not null drop table #s_boss_mbr_phones_inserts
create table #s_boss_mbr_phones_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_boss_mbr_phones.bk_hash,
       stage_hash_boss_mbr_phones.[id] mbr_phones_id,
       stage_hash_boss_mbr_phones.area_code area_code,
       stage_hash_boss_mbr_phones.number number,
       stage_hash_boss_mbr_phones.ext ext,
       stage_hash_boss_mbr_phones.ph_type ph_type,
       stage_hash_boss_mbr_phones.created_at created_at,
       stage_hash_boss_mbr_phones.updated_at updated_at,
       isnull(cast(stage_hash_boss_mbr_phones.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_boss_mbr_phones.[id] as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_mbr_phones.area_code,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_mbr_phones.number,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_mbr_phones.ext,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_mbr_phones.ph_type,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_boss_mbr_phones.created_at,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_boss_mbr_phones.updated_at,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_boss_mbr_phones
 where stage_hash_boss_mbr_phones.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_boss_mbr_phones records
set @insert_date_time = getdate()
insert into s_boss_mbr_phones (
       bk_hash,
       mbr_phones_id,
       area_code,
       number,
       ext,
       ph_type,
       created_at,
       updated_at,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_boss_mbr_phones_inserts.bk_hash,
       #s_boss_mbr_phones_inserts.mbr_phones_id,
       #s_boss_mbr_phones_inserts.area_code,
       #s_boss_mbr_phones_inserts.number,
       #s_boss_mbr_phones_inserts.ext,
       #s_boss_mbr_phones_inserts.ph_type,
       #s_boss_mbr_phones_inserts.created_at,
       #s_boss_mbr_phones_inserts.updated_at,
       case when s_boss_mbr_phones.s_boss_mbr_phones_id is null then isnull(#s_boss_mbr_phones_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       26,
       #s_boss_mbr_phones_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_boss_mbr_phones_inserts
  left join p_boss_mbr_phones
    on #s_boss_mbr_phones_inserts.bk_hash = p_boss_mbr_phones.bk_hash
   and p_boss_mbr_phones.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_boss_mbr_phones
    on p_boss_mbr_phones.bk_hash = s_boss_mbr_phones.bk_hash
   and p_boss_mbr_phones.s_boss_mbr_phones_id = s_boss_mbr_phones.s_boss_mbr_phones_id
 where s_boss_mbr_phones.s_boss_mbr_phones_id is null
    or (s_boss_mbr_phones.s_boss_mbr_phones_id is not null
        and s_boss_mbr_phones.dv_hash <> #s_boss_mbr_phones_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_boss_mbr_phones @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_boss_mbr_phones @current_dv_batch_id

end
