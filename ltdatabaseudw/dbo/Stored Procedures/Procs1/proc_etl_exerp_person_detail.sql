CREATE PROC [dbo].[proc_etl_exerp_person_detail] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_exerp_person_detail

set @insert_date_time = getdate()
insert into dbo.stage_hash_exerp_person_detail (
       bk_hash,
       person_id,
       address1,
       address2,
       address3,
       work_phone,
       mobile_phone,
       home_phone,
       email,
       full_name,
       firstname,
       lastname,
       center_id,
       ets,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(person_id,'z#@$k%&P'))),2) bk_hash,
       person_id,
       address1,
       address2,
       address3,
       work_phone,
       mobile_phone,
       home_phone,
       email,
       full_name,
       firstname,
       lastname,
       center_id,
       ets,
       dummy_modified_date_time,
       isnull(cast(stage_exerp_person_detail.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_exerp_person_detail
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_exerp_person_detail @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_exerp_person_detail (
       bk_hash,
       person_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_exerp_person_detail.bk_hash,
       stage_hash_exerp_person_detail.person_id person_id,
       isnull(cast(stage_hash_exerp_person_detail.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       33,
       @insert_date_time,
       @user
  from stage_hash_exerp_person_detail
  left join h_exerp_person_detail
    on stage_hash_exerp_person_detail.bk_hash = h_exerp_person_detail.bk_hash
 where h_exerp_person_detail_id is null
   and stage_hash_exerp_person_detail.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_exerp_person_detail
if object_id('tempdb..#l_exerp_person_detail_inserts') is not null drop table #l_exerp_person_detail_inserts
create table #l_exerp_person_detail_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_exerp_person_detail.bk_hash,
       stage_hash_exerp_person_detail.person_id person_id,
       stage_hash_exerp_person_detail.center_id center_id,
       isnull(cast(stage_hash_exerp_person_detail.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_exerp_person_detail.person_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_person_detail.center_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_exerp_person_detail
 where stage_hash_exerp_person_detail.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_exerp_person_detail records
set @insert_date_time = getdate()
insert into l_exerp_person_detail (
       bk_hash,
       person_id,
       center_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_exerp_person_detail_inserts.bk_hash,
       #l_exerp_person_detail_inserts.person_id,
       #l_exerp_person_detail_inserts.center_id,
       case when l_exerp_person_detail.l_exerp_person_detail_id is null then isnull(#l_exerp_person_detail_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       33,
       #l_exerp_person_detail_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_exerp_person_detail_inserts
  left join p_exerp_person_detail
    on #l_exerp_person_detail_inserts.bk_hash = p_exerp_person_detail.bk_hash
   and p_exerp_person_detail.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_exerp_person_detail
    on p_exerp_person_detail.bk_hash = l_exerp_person_detail.bk_hash
   and p_exerp_person_detail.l_exerp_person_detail_id = l_exerp_person_detail.l_exerp_person_detail_id
 where l_exerp_person_detail.l_exerp_person_detail_id is null
    or (l_exerp_person_detail.l_exerp_person_detail_id is not null
        and l_exerp_person_detail.dv_hash <> #l_exerp_person_detail_inserts.source_hash)

--calculate hash and lookup to current s_exerp_person_detail
if object_id('tempdb..#s_exerp_person_detail_inserts') is not null drop table #s_exerp_person_detail_inserts
create table #s_exerp_person_detail_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_exerp_person_detail.bk_hash,
       stage_hash_exerp_person_detail.person_id person_id,
       stage_hash_exerp_person_detail.address1 address_1,
       stage_hash_exerp_person_detail.address2 address_2,
       stage_hash_exerp_person_detail.address3 address_3,
       stage_hash_exerp_person_detail.work_phone work_phone,
       stage_hash_exerp_person_detail.mobile_phone mobile_phone,
       stage_hash_exerp_person_detail.home_phone home_phone,
       stage_hash_exerp_person_detail.email email,
       stage_hash_exerp_person_detail.full_name full_name,
       stage_hash_exerp_person_detail.firstname first_name,
       stage_hash_exerp_person_detail.lastname last_name,
       stage_hash_exerp_person_detail.ets ets,
       stage_hash_exerp_person_detail.dummy_modified_date_time dummy_modified_date_time,
       isnull(cast(stage_hash_exerp_person_detail.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_exerp_person_detail.person_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_person_detail.address1,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_person_detail.address2,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_person_detail.address3,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_person_detail.work_phone,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_person_detail.mobile_phone,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_person_detail.home_phone,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_person_detail.email,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_person_detail.full_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_person_detail.firstname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_person_detail.lastname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_person_detail.ets as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_exerp_person_detail
 where stage_hash_exerp_person_detail.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_exerp_person_detail records
set @insert_date_time = getdate()
insert into s_exerp_person_detail (
       bk_hash,
       person_id,
       address_1,
       address_2,
       address_3,
       work_phone,
       mobile_phone,
       home_phone,
       email,
       full_name,
       first_name,
       last_name,
       ets,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_exerp_person_detail_inserts.bk_hash,
       #s_exerp_person_detail_inserts.person_id,
       #s_exerp_person_detail_inserts.address_1,
       #s_exerp_person_detail_inserts.address_2,
       #s_exerp_person_detail_inserts.address_3,
       #s_exerp_person_detail_inserts.work_phone,
       #s_exerp_person_detail_inserts.mobile_phone,
       #s_exerp_person_detail_inserts.home_phone,
       #s_exerp_person_detail_inserts.email,
       #s_exerp_person_detail_inserts.full_name,
       #s_exerp_person_detail_inserts.first_name,
       #s_exerp_person_detail_inserts.last_name,
       #s_exerp_person_detail_inserts.ets,
       #s_exerp_person_detail_inserts.dummy_modified_date_time,
       case when s_exerp_person_detail.s_exerp_person_detail_id is null then isnull(#s_exerp_person_detail_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       33,
       #s_exerp_person_detail_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_exerp_person_detail_inserts
  left join p_exerp_person_detail
    on #s_exerp_person_detail_inserts.bk_hash = p_exerp_person_detail.bk_hash
   and p_exerp_person_detail.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_exerp_person_detail
    on p_exerp_person_detail.bk_hash = s_exerp_person_detail.bk_hash
   and p_exerp_person_detail.s_exerp_person_detail_id = s_exerp_person_detail.s_exerp_person_detail_id
 where s_exerp_person_detail.s_exerp_person_detail_id is null
    or (s_exerp_person_detail.s_exerp_person_detail_id is not null
        and s_exerp_person_detail.dv_hash <> #s_exerp_person_detail_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_exerp_person_detail @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_exerp_person_detail @current_dv_batch_id

end
