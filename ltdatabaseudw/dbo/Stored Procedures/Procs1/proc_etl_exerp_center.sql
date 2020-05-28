CREATE PROC [dbo].[proc_etl_exerp_center] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_exerp_center

set @insert_date_time = getdate()
insert into dbo.stage_hash_exerp_center (
       bk_hash,
       id,
       state,
       county,
       manager_person_id,
       time_zone,
       city,
       phone_number,
       address3,
       address2,
       address1,
       postal_code,
       country_code,
       shortname,
       name,
       migration_date,
       startup_date,
       longitude,
       latitude,
       external_id,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       id,
       state,
       county,
       manager_person_id,
       time_zone,
       city,
       phone_number,
       address3,
       address2,
       address1,
       postal_code,
       country_code,
       shortname,
       name,
       migration_date,
       startup_date,
       longitude,
       latitude,
       external_id,
       dummy_modified_date_time,
       isnull(cast(stage_exerp_center.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_exerp_center
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_exerp_center @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_exerp_center (
       bk_hash,
       center_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_exerp_center.bk_hash,
       stage_hash_exerp_center.id center_id,
       isnull(cast(stage_hash_exerp_center.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       33,
       @insert_date_time,
       @user
  from stage_hash_exerp_center
  left join h_exerp_center
    on stage_hash_exerp_center.bk_hash = h_exerp_center.bk_hash
 where h_exerp_center_id is null
   and stage_hash_exerp_center.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_exerp_center
if object_id('tempdb..#l_exerp_center_inserts') is not null drop table #l_exerp_center_inserts
create table #l_exerp_center_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_exerp_center.bk_hash,
       stage_hash_exerp_center.id center_id,
       stage_hash_exerp_center.manager_person_id manager_person_id,
       stage_hash_exerp_center.external_id external_id,
       isnull(cast(stage_hash_exerp_center.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_exerp_center.id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_center.manager_person_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_center.external_id,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_exerp_center
 where stage_hash_exerp_center.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_exerp_center records
set @insert_date_time = getdate()
insert into l_exerp_center (
       bk_hash,
       center_id,
       manager_person_id,
       external_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_exerp_center_inserts.bk_hash,
       #l_exerp_center_inserts.center_id,
       #l_exerp_center_inserts.manager_person_id,
       #l_exerp_center_inserts.external_id,
       case when l_exerp_center.l_exerp_center_id is null then isnull(#l_exerp_center_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       33,
       #l_exerp_center_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_exerp_center_inserts
  left join p_exerp_center
    on #l_exerp_center_inserts.bk_hash = p_exerp_center.bk_hash
   and p_exerp_center.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_exerp_center
    on p_exerp_center.bk_hash = l_exerp_center.bk_hash
   and p_exerp_center.l_exerp_center_id = l_exerp_center.l_exerp_center_id
 where l_exerp_center.l_exerp_center_id is null
    or (l_exerp_center.l_exerp_center_id is not null
        and l_exerp_center.dv_hash <> #l_exerp_center_inserts.source_hash)

--calculate hash and lookup to current s_exerp_center
if object_id('tempdb..#s_exerp_center_inserts') is not null drop table #s_exerp_center_inserts
create table #s_exerp_center_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_exerp_center.bk_hash,
       stage_hash_exerp_center.id center_id,
       stage_hash_exerp_center.state state,
       stage_hash_exerp_center.county county,
       stage_hash_exerp_center.time_zone time_zone,
       stage_hash_exerp_center.city city,
       stage_hash_exerp_center.phone_number phone_number,
       stage_hash_exerp_center.address3 address_3,
       stage_hash_exerp_center.address2 address_2,
       stage_hash_exerp_center.address1 address_1,
       stage_hash_exerp_center.postal_code postal_code,
       stage_hash_exerp_center.country_code country_code,
       stage_hash_exerp_center.shortname short_name,
       stage_hash_exerp_center.name name,
       stage_hash_exerp_center.migration_date migration_date,
       stage_hash_exerp_center.startup_date startup_date,
       stage_hash_exerp_center.longitude longitude,
       stage_hash_exerp_center.latitude latitude,
       stage_hash_exerp_center.dummy_modified_date_time dummy_modified_date_time,
       isnull(cast(stage_hash_exerp_center.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_exerp_center.id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_center.state,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_center.county,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_center.time_zone,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_center.city,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_center.phone_number,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_center.address3,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_center.address2,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_center.address1,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_center.postal_code,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_center.country_code,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_center.shortname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_center.name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_exerp_center.migration_date,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_exerp_center.startup_date,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_center.longitude as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_center.latitude as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_exerp_center
 where stage_hash_exerp_center.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_exerp_center records
set @insert_date_time = getdate()
insert into s_exerp_center (
       bk_hash,
       center_id,
       state,
       county,
       time_zone,
       city,
       phone_number,
       address_3,
       address_2,
       address_1,
       postal_code,
       country_code,
       short_name,
       name,
       migration_date,
       startup_date,
       longitude,
       latitude,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_exerp_center_inserts.bk_hash,
       #s_exerp_center_inserts.center_id,
       #s_exerp_center_inserts.state,
       #s_exerp_center_inserts.county,
       #s_exerp_center_inserts.time_zone,
       #s_exerp_center_inserts.city,
       #s_exerp_center_inserts.phone_number,
       #s_exerp_center_inserts.address_3,
       #s_exerp_center_inserts.address_2,
       #s_exerp_center_inserts.address_1,
       #s_exerp_center_inserts.postal_code,
       #s_exerp_center_inserts.country_code,
       #s_exerp_center_inserts.short_name,
       #s_exerp_center_inserts.name,
       #s_exerp_center_inserts.migration_date,
       #s_exerp_center_inserts.startup_date,
       #s_exerp_center_inserts.longitude,
       #s_exerp_center_inserts.latitude,
       #s_exerp_center_inserts.dummy_modified_date_time,
       case when s_exerp_center.s_exerp_center_id is null then isnull(#s_exerp_center_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       33,
       #s_exerp_center_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_exerp_center_inserts
  left join p_exerp_center
    on #s_exerp_center_inserts.bk_hash = p_exerp_center.bk_hash
   and p_exerp_center.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_exerp_center
    on p_exerp_center.bk_hash = s_exerp_center.bk_hash
   and p_exerp_center.s_exerp_center_id = s_exerp_center.s_exerp_center_id
 where s_exerp_center.s_exerp_center_id is null
    or (s_exerp_center.s_exerp_center_id is not null
        and s_exerp_center.dv_hash <> #s_exerp_center_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_exerp_center @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_exerp_center @current_dv_batch_id

end
