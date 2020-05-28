CREATE PROC [dbo].[proc_etl_chronotrack_location] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_chronotrack_location

set @insert_date_time = getdate()
insert into dbo.stage_hash_chronotrack_location (
       bk_hash,
       id,
       name,
       latitude,
       longitude,
       time_zone,
       street,
       street2,
       city,
       region_id,
       county,
       postal_code,
       ctime,
       mtime,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       id,
       name,
       latitude,
       longitude,
       time_zone,
       street,
       street2,
       city,
       region_id,
       county,
       postal_code,
       ctime,
       mtime,
       dummy_modified_date_time,
       isnull(cast(stage_chronotrack_location.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_chronotrack_location
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_chronotrack_location @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_chronotrack_location (
       bk_hash,
       location_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_chronotrack_location.bk_hash,
       stage_hash_chronotrack_location.id location_id,
       isnull(cast(stage_hash_chronotrack_location.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       46,
       @insert_date_time,
       @user
  from stage_hash_chronotrack_location
  left join h_chronotrack_location
    on stage_hash_chronotrack_location.bk_hash = h_chronotrack_location.bk_hash
 where h_chronotrack_location_id is null
   and stage_hash_chronotrack_location.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_chronotrack_location
if object_id('tempdb..#l_chronotrack_location_inserts') is not null drop table #l_chronotrack_location_inserts
create table #l_chronotrack_location_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_chronotrack_location.bk_hash,
       stage_hash_chronotrack_location.id location_id,
       stage_hash_chronotrack_location.region_id region_id,
       isnull(cast(stage_hash_chronotrack_location.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_chronotrack_location.id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_location.region_id,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_chronotrack_location
 where stage_hash_chronotrack_location.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_chronotrack_location records
set @insert_date_time = getdate()
insert into l_chronotrack_location (
       bk_hash,
       location_id,
       region_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_chronotrack_location_inserts.bk_hash,
       #l_chronotrack_location_inserts.location_id,
       #l_chronotrack_location_inserts.region_id,
       case when l_chronotrack_location.l_chronotrack_location_id is null then isnull(#l_chronotrack_location_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       46,
       #l_chronotrack_location_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_chronotrack_location_inserts
  left join p_chronotrack_location
    on #l_chronotrack_location_inserts.bk_hash = p_chronotrack_location.bk_hash
   and p_chronotrack_location.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_chronotrack_location
    on p_chronotrack_location.bk_hash = l_chronotrack_location.bk_hash
   and p_chronotrack_location.l_chronotrack_location_id = l_chronotrack_location.l_chronotrack_location_id
 where l_chronotrack_location.l_chronotrack_location_id is null
    or (l_chronotrack_location.l_chronotrack_location_id is not null
        and l_chronotrack_location.dv_hash <> #l_chronotrack_location_inserts.source_hash)

--calculate hash and lookup to current s_chronotrack_location
if object_id('tempdb..#s_chronotrack_location_inserts') is not null drop table #s_chronotrack_location_inserts
create table #s_chronotrack_location_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_chronotrack_location.bk_hash,
       stage_hash_chronotrack_location.id location_id,
       stage_hash_chronotrack_location.name name,
       stage_hash_chronotrack_location.latitude latitude,
       stage_hash_chronotrack_location.longitude longitude,
       stage_hash_chronotrack_location.time_zone time_zone,
       stage_hash_chronotrack_location.street street,
       stage_hash_chronotrack_location.street2 street_2,
       stage_hash_chronotrack_location.city city,
       stage_hash_chronotrack_location.county county,
       stage_hash_chronotrack_location.postal_code postal_code,
       stage_hash_chronotrack_location.ctime create_time,
       stage_hash_chronotrack_location.mtime modified_time,
       stage_hash_chronotrack_location.dummy_modified_date_time dummy_modified_date_time,
       isnull(cast(stage_hash_chronotrack_location.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_chronotrack_location.id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_location.name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_location.latitude as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_location.longitude as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_location.time_zone,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_location.street,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_location.street2,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_location.city,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_location.county,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_location.postal_code,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_location.ctime as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_location.mtime as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_chronotrack_location
 where stage_hash_chronotrack_location.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_chronotrack_location records
set @insert_date_time = getdate()
insert into s_chronotrack_location (
       bk_hash,
       location_id,
       name,
       latitude,
       longitude,
       time_zone,
       street,
       street_2,
       city,
       county,
       postal_code,
       create_time,
       modified_time,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_chronotrack_location_inserts.bk_hash,
       #s_chronotrack_location_inserts.location_id,
       #s_chronotrack_location_inserts.name,
       #s_chronotrack_location_inserts.latitude,
       #s_chronotrack_location_inserts.longitude,
       #s_chronotrack_location_inserts.time_zone,
       #s_chronotrack_location_inserts.street,
       #s_chronotrack_location_inserts.street_2,
       #s_chronotrack_location_inserts.city,
       #s_chronotrack_location_inserts.county,
       #s_chronotrack_location_inserts.postal_code,
       #s_chronotrack_location_inserts.create_time,
       #s_chronotrack_location_inserts.modified_time,
       #s_chronotrack_location_inserts.dummy_modified_date_time,
       case when s_chronotrack_location.s_chronotrack_location_id is null then isnull(#s_chronotrack_location_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       46,
       #s_chronotrack_location_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_chronotrack_location_inserts
  left join p_chronotrack_location
    on #s_chronotrack_location_inserts.bk_hash = p_chronotrack_location.bk_hash
   and p_chronotrack_location.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_chronotrack_location
    on p_chronotrack_location.bk_hash = s_chronotrack_location.bk_hash
   and p_chronotrack_location.s_chronotrack_location_id = s_chronotrack_location.s_chronotrack_location_id
 where s_chronotrack_location.s_chronotrack_location_id is null
    or (s_chronotrack_location.s_chronotrack_location_id is not null
        and s_chronotrack_location.dv_hash <> #s_chronotrack_location_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_chronotrack_location @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_chronotrack_location @current_dv_batch_id

end
