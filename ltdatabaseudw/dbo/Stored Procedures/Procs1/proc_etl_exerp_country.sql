CREATE PROC [dbo].[proc_etl_exerp_country] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_exerp_country

set @insert_date_time = getdate()
insert into dbo.stage_hash_exerp_country (
       bk_hash,
       id,
       name,
       timezone,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(id,'z#@$k%&P'))),2) bk_hash,
       id,
       name,
       timezone,
       dummy_modified_date_time,
       isnull(cast(stage_exerp_country.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_exerp_country
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_exerp_country @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_exerp_country (
       bk_hash,
       country_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_exerp_country.bk_hash,
       stage_hash_exerp_country.id country_id,
       isnull(cast(stage_hash_exerp_country.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       33,
       @insert_date_time,
       @user
  from stage_hash_exerp_country
  left join h_exerp_country
    on stage_hash_exerp_country.bk_hash = h_exerp_country.bk_hash
 where h_exerp_country_id is null
   and stage_hash_exerp_country.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current s_exerp_country
if object_id('tempdb..#s_exerp_country_inserts') is not null drop table #s_exerp_country_inserts
create table #s_exerp_country_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_exerp_country.bk_hash,
       stage_hash_exerp_country.id country_id,
       stage_hash_exerp_country.name name,
       stage_hash_exerp_country.timezone timezone,
       stage_hash_exerp_country.dummy_modified_date_time dummy_modified_date_time,
       isnull(cast(stage_hash_exerp_country.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_exerp_country.id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_country.name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_country.timezone,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_exerp_country
 where stage_hash_exerp_country.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_exerp_country records
set @insert_date_time = getdate()
insert into s_exerp_country (
       bk_hash,
       country_id,
       name,
       timezone,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_exerp_country_inserts.bk_hash,
       #s_exerp_country_inserts.country_id,
       #s_exerp_country_inserts.name,
       #s_exerp_country_inserts.timezone,
       #s_exerp_country_inserts.dummy_modified_date_time,
       case when s_exerp_country.s_exerp_country_id is null then isnull(#s_exerp_country_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       33,
       #s_exerp_country_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_exerp_country_inserts
  left join p_exerp_country
    on #s_exerp_country_inserts.bk_hash = p_exerp_country.bk_hash
   and p_exerp_country.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_exerp_country
    on p_exerp_country.bk_hash = s_exerp_country.bk_hash
   and p_exerp_country.s_exerp_country_id = s_exerp_country.s_exerp_country_id
 where s_exerp_country.s_exerp_country_id is null
    or (s_exerp_country.s_exerp_country_id is not null
        and s_exerp_country.dv_hash <> #s_exerp_country_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_exerp_country @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_exerp_country @current_dv_batch_id

end
