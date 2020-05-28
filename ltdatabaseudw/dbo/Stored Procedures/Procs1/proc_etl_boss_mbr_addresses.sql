CREATE PROC [dbo].[proc_etl_boss_mbr_addresses] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_boss_mbr_addresses

set @insert_date_time = getdate()
insert into dbo.stage_hash_boss_mbr_addresses (
       bk_hash,
       [id],
       line_1,
       line_2,
       city,
       zip,
       zip_four,
       state_code,
       addr_type,
       contact_id,
       created_at,
       updated_at,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast([id] as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       [id],
       line_1,
       line_2,
       city,
       zip,
       zip_four,
       state_code,
       addr_type,
       contact_id,
       created_at,
       updated_at,
       isnull(cast(stage_boss_mbr_addresses.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_boss_mbr_addresses
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_boss_mbr_addresses @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_boss_mbr_addresses (
       bk_hash,
       mbr_addresses_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_boss_mbr_addresses.bk_hash,
       stage_hash_boss_mbr_addresses.[id] mbr_addresses_id,
       isnull(cast(stage_hash_boss_mbr_addresses.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       26,
       @insert_date_time,
       @user
  from stage_hash_boss_mbr_addresses
  left join h_boss_mbr_addresses
    on stage_hash_boss_mbr_addresses.bk_hash = h_boss_mbr_addresses.bk_hash
 where h_boss_mbr_addresses_id is null
   and stage_hash_boss_mbr_addresses.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_boss_mbr_addresses
if object_id('tempdb..#l_boss_mbr_addresses_inserts') is not null drop table #l_boss_mbr_addresses_inserts
create table #l_boss_mbr_addresses_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_boss_mbr_addresses.bk_hash,
       stage_hash_boss_mbr_addresses.[id] mbr_addresses_id,
       stage_hash_boss_mbr_addresses.contact_id contact_id,
       isnull(cast(stage_hash_boss_mbr_addresses.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_boss_mbr_addresses.[id] as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_mbr_addresses.contact_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_boss_mbr_addresses
 where stage_hash_boss_mbr_addresses.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_boss_mbr_addresses records
set @insert_date_time = getdate()
insert into l_boss_mbr_addresses (
       bk_hash,
       mbr_addresses_id,
       contact_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_boss_mbr_addresses_inserts.bk_hash,
       #l_boss_mbr_addresses_inserts.mbr_addresses_id,
       #l_boss_mbr_addresses_inserts.contact_id,
       case when l_boss_mbr_addresses.l_boss_mbr_addresses_id is null then isnull(#l_boss_mbr_addresses_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       26,
       #l_boss_mbr_addresses_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_boss_mbr_addresses_inserts
  left join p_boss_mbr_addresses
    on #l_boss_mbr_addresses_inserts.bk_hash = p_boss_mbr_addresses.bk_hash
   and p_boss_mbr_addresses.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_boss_mbr_addresses
    on p_boss_mbr_addresses.bk_hash = l_boss_mbr_addresses.bk_hash
   and p_boss_mbr_addresses.l_boss_mbr_addresses_id = l_boss_mbr_addresses.l_boss_mbr_addresses_id
 where l_boss_mbr_addresses.l_boss_mbr_addresses_id is null
    or (l_boss_mbr_addresses.l_boss_mbr_addresses_id is not null
        and l_boss_mbr_addresses.dv_hash <> #l_boss_mbr_addresses_inserts.source_hash)

--calculate hash and lookup to current s_boss_mbr_addresses
if object_id('tempdb..#s_boss_mbr_addresses_inserts') is not null drop table #s_boss_mbr_addresses_inserts
create table #s_boss_mbr_addresses_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_boss_mbr_addresses.bk_hash,
       stage_hash_boss_mbr_addresses.[id] mbr_addresses_id,
       stage_hash_boss_mbr_addresses.line_1 line_1,
       stage_hash_boss_mbr_addresses.line_2 line_2,
       stage_hash_boss_mbr_addresses.city city,
       stage_hash_boss_mbr_addresses.zip zip,
       stage_hash_boss_mbr_addresses.zip_four zip_four,
       stage_hash_boss_mbr_addresses.state_code state_code,
       stage_hash_boss_mbr_addresses.addr_type addr_type,
       stage_hash_boss_mbr_addresses.created_at created_at,
       stage_hash_boss_mbr_addresses.updated_at updated_at,
       isnull(cast(stage_hash_boss_mbr_addresses.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_boss_mbr_addresses.[id] as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_mbr_addresses.line_1,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_mbr_addresses.line_2,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_mbr_addresses.city,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_mbr_addresses.zip,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_mbr_addresses.zip_four,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_mbr_addresses.state_code,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_mbr_addresses.addr_type,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_boss_mbr_addresses.created_at,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_boss_mbr_addresses.updated_at,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_boss_mbr_addresses
 where stage_hash_boss_mbr_addresses.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_boss_mbr_addresses records
set @insert_date_time = getdate()
insert into s_boss_mbr_addresses (
       bk_hash,
       mbr_addresses_id,
       line_1,
       line_2,
       city,
       zip,
       zip_four,
       state_code,
       addr_type,
       created_at,
       updated_at,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_boss_mbr_addresses_inserts.bk_hash,
       #s_boss_mbr_addresses_inserts.mbr_addresses_id,
       #s_boss_mbr_addresses_inserts.line_1,
       #s_boss_mbr_addresses_inserts.line_2,
       #s_boss_mbr_addresses_inserts.city,
       #s_boss_mbr_addresses_inserts.zip,
       #s_boss_mbr_addresses_inserts.zip_four,
       #s_boss_mbr_addresses_inserts.state_code,
       #s_boss_mbr_addresses_inserts.addr_type,
       #s_boss_mbr_addresses_inserts.created_at,
       #s_boss_mbr_addresses_inserts.updated_at,
       case when s_boss_mbr_addresses.s_boss_mbr_addresses_id is null then isnull(#s_boss_mbr_addresses_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       26,
       #s_boss_mbr_addresses_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_boss_mbr_addresses_inserts
  left join p_boss_mbr_addresses
    on #s_boss_mbr_addresses_inserts.bk_hash = p_boss_mbr_addresses.bk_hash
   and p_boss_mbr_addresses.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_boss_mbr_addresses
    on p_boss_mbr_addresses.bk_hash = s_boss_mbr_addresses.bk_hash
   and p_boss_mbr_addresses.s_boss_mbr_addresses_id = s_boss_mbr_addresses.s_boss_mbr_addresses_id
 where s_boss_mbr_addresses.s_boss_mbr_addresses_id is null
    or (s_boss_mbr_addresses.s_boss_mbr_addresses_id is not null
        and s_boss_mbr_addresses.dv_hash <> #s_boss_mbr_addresses_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_boss_mbr_addresses @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_boss_mbr_addresses @current_dv_batch_id

end
