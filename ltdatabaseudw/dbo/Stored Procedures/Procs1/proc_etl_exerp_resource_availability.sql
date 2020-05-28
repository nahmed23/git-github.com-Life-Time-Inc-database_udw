CREATE PROC [dbo].[proc_etl_exerp_resource_availability] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_exerp_resource_availability

set @insert_date_time = getdate()
insert into dbo.stage_hash_exerp_resource_availability (
       bk_hash,
       resource_id,
       resource_group_id,
       availability_type,
       value,
       from_time,
       to_time,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(resource_id,'z#@$k%&P')+'P%#&z$@k'+isnull(cast(resource_group_id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       resource_id,
       resource_group_id,
       availability_type,
       value,
       from_time,
       to_time,
       dummy_modified_date_time,
       isnull(cast(stage_exerp_resource_availability.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_exerp_resource_availability
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_exerp_resource_availability @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_exerp_resource_availability (
       bk_hash,
       resource_id,
       resource_group_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_exerp_resource_availability.bk_hash,
       stage_hash_exerp_resource_availability.resource_id resource_id,
       stage_hash_exerp_resource_availability.resource_group_id resource_group_id,
       isnull(cast(stage_hash_exerp_resource_availability.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       33,
       @insert_date_time,
       @user
  from stage_hash_exerp_resource_availability
  left join h_exerp_resource_availability
    on stage_hash_exerp_resource_availability.bk_hash = h_exerp_resource_availability.bk_hash
 where h_exerp_resource_availability_id is null
   and stage_hash_exerp_resource_availability.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current s_exerp_resource_availability
if object_id('tempdb..#s_exerp_resource_availability_inserts') is not null drop table #s_exerp_resource_availability_inserts
create table #s_exerp_resource_availability_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_exerp_resource_availability.bk_hash,
       stage_hash_exerp_resource_availability.resource_id resource_id,
       stage_hash_exerp_resource_availability.resource_group_id resource_group_id,
       stage_hash_exerp_resource_availability.availability_type availability_type,
       stage_hash_exerp_resource_availability.value value,
       stage_hash_exerp_resource_availability.from_time from_time,
       stage_hash_exerp_resource_availability.to_time to_time,
       stage_hash_exerp_resource_availability.dummy_modified_date_time dummy_modified_date_time,
       isnull(cast(stage_hash_exerp_resource_availability.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_exerp_resource_availability.resource_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_resource_availability.resource_group_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_resource_availability.availability_type,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_resource_availability.value,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_resource_availability.from_time,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_resource_availability.to_time,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_exerp_resource_availability
 where stage_hash_exerp_resource_availability.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_exerp_resource_availability records
set @insert_date_time = getdate()
insert into s_exerp_resource_availability (
       bk_hash,
       resource_id,
       resource_group_id,
       availability_type,
       value,
       from_time,
       to_time,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_exerp_resource_availability_inserts.bk_hash,
       #s_exerp_resource_availability_inserts.resource_id,
       #s_exerp_resource_availability_inserts.resource_group_id,
       #s_exerp_resource_availability_inserts.availability_type,
       #s_exerp_resource_availability_inserts.value,
       #s_exerp_resource_availability_inserts.from_time,
       #s_exerp_resource_availability_inserts.to_time,
       #s_exerp_resource_availability_inserts.dummy_modified_date_time,
       case when s_exerp_resource_availability.s_exerp_resource_availability_id is null then isnull(#s_exerp_resource_availability_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       33,
       #s_exerp_resource_availability_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_exerp_resource_availability_inserts
  left join p_exerp_resource_availability
    on #s_exerp_resource_availability_inserts.bk_hash = p_exerp_resource_availability.bk_hash
   and p_exerp_resource_availability.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_exerp_resource_availability
    on p_exerp_resource_availability.bk_hash = s_exerp_resource_availability.bk_hash
   and p_exerp_resource_availability.s_exerp_resource_availability_id = s_exerp_resource_availability.s_exerp_resource_availability_id
 where s_exerp_resource_availability.s_exerp_resource_availability_id is null
    or (s_exerp_resource_availability.s_exerp_resource_availability_id is not null
        and s_exerp_resource_availability.dv_hash <> #s_exerp_resource_availability_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_exerp_resource_availability @current_dv_batch_id

end
