CREATE PROC [dbo].[proc_etl_boss_mbr_ooa_contacts] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_boss_mbr_ooa_contacts

set @insert_date_time = getdate()
insert into dbo.stage_hash_boss_mbr_ooa_contacts (
       bk_hash,
       [id],
       cust_code,
       mbr_code,
       created_at,
       updated_at,
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
       isnull(cast(stage_boss_mbr_ooa_contacts.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_boss_mbr_ooa_contacts
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_boss_mbr_ooa_contacts @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_boss_mbr_ooa_contacts (
       bk_hash,
       mbr_ooa_contacts_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_boss_mbr_ooa_contacts.bk_hash,
       stage_hash_boss_mbr_ooa_contacts.[id] mbr_ooa_contacts_id,
       isnull(cast(stage_hash_boss_mbr_ooa_contacts.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       26,
       @insert_date_time,
       @user
  from stage_hash_boss_mbr_ooa_contacts
  left join h_boss_mbr_ooa_contacts
    on stage_hash_boss_mbr_ooa_contacts.bk_hash = h_boss_mbr_ooa_contacts.bk_hash
 where h_boss_mbr_ooa_contacts_id is null
   and stage_hash_boss_mbr_ooa_contacts.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current s_boss_mbr_ooa_contacts
if object_id('tempdb..#s_boss_mbr_ooa_contacts_inserts') is not null drop table #s_boss_mbr_ooa_contacts_inserts
create table #s_boss_mbr_ooa_contacts_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_boss_mbr_ooa_contacts.bk_hash,
       stage_hash_boss_mbr_ooa_contacts.[id] mbr_ooa_contacts_id,
       stage_hash_boss_mbr_ooa_contacts.cust_code cust_code,
       stage_hash_boss_mbr_ooa_contacts.mbr_code mbr_code,
       stage_hash_boss_mbr_ooa_contacts.created_at created_at,
       stage_hash_boss_mbr_ooa_contacts.updated_at updated_at,
       isnull(cast(stage_hash_boss_mbr_ooa_contacts.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_boss_mbr_ooa_contacts.[id] as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_mbr_ooa_contacts.cust_code,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_mbr_ooa_contacts.mbr_code,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_boss_mbr_ooa_contacts.created_at,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_boss_mbr_ooa_contacts.updated_at,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_boss_mbr_ooa_contacts
 where stage_hash_boss_mbr_ooa_contacts.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_boss_mbr_ooa_contacts records
set @insert_date_time = getdate()
insert into s_boss_mbr_ooa_contacts (
       bk_hash,
       mbr_ooa_contacts_id,
       cust_code,
       mbr_code,
       created_at,
       updated_at,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_boss_mbr_ooa_contacts_inserts.bk_hash,
       #s_boss_mbr_ooa_contacts_inserts.mbr_ooa_contacts_id,
       #s_boss_mbr_ooa_contacts_inserts.cust_code,
       #s_boss_mbr_ooa_contacts_inserts.mbr_code,
       #s_boss_mbr_ooa_contacts_inserts.created_at,
       #s_boss_mbr_ooa_contacts_inserts.updated_at,
       case when s_boss_mbr_ooa_contacts.s_boss_mbr_ooa_contacts_id is null then isnull(#s_boss_mbr_ooa_contacts_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       26,
       #s_boss_mbr_ooa_contacts_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_boss_mbr_ooa_contacts_inserts
  left join p_boss_mbr_ooa_contacts
    on #s_boss_mbr_ooa_contacts_inserts.bk_hash = p_boss_mbr_ooa_contacts.bk_hash
   and p_boss_mbr_ooa_contacts.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_boss_mbr_ooa_contacts
    on p_boss_mbr_ooa_contacts.bk_hash = s_boss_mbr_ooa_contacts.bk_hash
   and p_boss_mbr_ooa_contacts.s_boss_mbr_ooa_contacts_id = s_boss_mbr_ooa_contacts.s_boss_mbr_ooa_contacts_id
 where s_boss_mbr_ooa_contacts.s_boss_mbr_ooa_contacts_id is null
    or (s_boss_mbr_ooa_contacts.s_boss_mbr_ooa_contacts_id is not null
        and s_boss_mbr_ooa_contacts.dv_hash <> #s_boss_mbr_ooa_contacts_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_boss_mbr_ooa_contacts @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_boss_mbr_ooa_contacts @current_dv_batch_id

end
