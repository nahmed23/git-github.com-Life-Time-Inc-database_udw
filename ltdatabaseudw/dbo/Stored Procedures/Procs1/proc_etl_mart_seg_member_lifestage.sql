CREATE PROC [dbo].[proc_etl_mart_seg_member_lifestage] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_mart_seg_member_lifestage

set @insert_date_time = getdate()
insert into dbo.stage_hash_mart_seg_member_lifestage (
       bk_hash,
       lifestage_segment_id,
       lifestage_description,
       gender,
       has_kids,
       min_age,
       max_age,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(lifestage_segment_id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       lifestage_segment_id,
       lifestage_description,
       gender,
       has_kids,
       min_age,
       max_age,
       dummy_modified_date_time,
       isnull(cast(stage_mart_seg_member_lifestage.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_mart_seg_member_lifestage
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_mart_seg_member_lifestage @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_mart_seg_member_lifestage (
       bk_hash,
       lifestage_segment_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_mart_seg_member_lifestage.bk_hash,
       stage_hash_mart_seg_member_lifestage.lifestage_segment_id lifestage_segment_id,
       isnull(cast(stage_hash_mart_seg_member_lifestage.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       42,
       @insert_date_time,
       @user
  from stage_hash_mart_seg_member_lifestage
  left join h_mart_seg_member_lifestage
    on stage_hash_mart_seg_member_lifestage.bk_hash = h_mart_seg_member_lifestage.bk_hash
 where h_mart_seg_member_lifestage_id is null
   and stage_hash_mart_seg_member_lifestage.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current s_mart_seg_member_lifestage
if object_id('tempdb..#s_mart_seg_member_lifestage_inserts') is not null drop table #s_mart_seg_member_lifestage_inserts
create table #s_mart_seg_member_lifestage_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mart_seg_member_lifestage.bk_hash,
       stage_hash_mart_seg_member_lifestage.lifestage_segment_id lifestage_segment_id,
       stage_hash_mart_seg_member_lifestage.lifestage_description lifestage_description,
       stage_hash_mart_seg_member_lifestage.gender gender,
       stage_hash_mart_seg_member_lifestage.has_kids has_kids,
       stage_hash_mart_seg_member_lifestage.min_age min_age,
       stage_hash_mart_seg_member_lifestage.max_age max_age,
       stage_hash_mart_seg_member_lifestage.dummy_modified_date_time dummy_modified_date_time,
       isnull(cast(stage_hash_mart_seg_member_lifestage.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mart_seg_member_lifestage.lifestage_segment_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mart_seg_member_lifestage.lifestage_description,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mart_seg_member_lifestage.gender,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mart_seg_member_lifestage.has_kids as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mart_seg_member_lifestage.min_age as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mart_seg_member_lifestage.max_age as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mart_seg_member_lifestage
 where stage_hash_mart_seg_member_lifestage.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mart_seg_member_lifestage records
set @insert_date_time = getdate()
insert into s_mart_seg_member_lifestage (
       bk_hash,
       lifestage_segment_id,
       lifestage_description,
       gender,
       has_kids,
       min_age,
       max_age,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mart_seg_member_lifestage_inserts.bk_hash,
       #s_mart_seg_member_lifestage_inserts.lifestage_segment_id,
       #s_mart_seg_member_lifestage_inserts.lifestage_description,
       #s_mart_seg_member_lifestage_inserts.gender,
       #s_mart_seg_member_lifestage_inserts.has_kids,
       #s_mart_seg_member_lifestage_inserts.min_age,
       #s_mart_seg_member_lifestage_inserts.max_age,
       #s_mart_seg_member_lifestage_inserts.dummy_modified_date_time,
       case when s_mart_seg_member_lifestage.s_mart_seg_member_lifestage_id is null then isnull(#s_mart_seg_member_lifestage_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       42,
       #s_mart_seg_member_lifestage_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mart_seg_member_lifestage_inserts
  left join p_mart_seg_member_lifestage
    on #s_mart_seg_member_lifestage_inserts.bk_hash = p_mart_seg_member_lifestage.bk_hash
   and p_mart_seg_member_lifestage.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mart_seg_member_lifestage
    on p_mart_seg_member_lifestage.bk_hash = s_mart_seg_member_lifestage.bk_hash
   and p_mart_seg_member_lifestage.s_mart_seg_member_lifestage_id = s_mart_seg_member_lifestage.s_mart_seg_member_lifestage_id
 where s_mart_seg_member_lifestage.s_mart_seg_member_lifestage_id is null
    or (s_mart_seg_member_lifestage.s_mart_seg_member_lifestage_id is not null
        and s_mart_seg_member_lifestage.dv_hash <> #s_mart_seg_member_lifestage_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_mart_seg_member_lifestage @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_mart_seg_member_lifestage_history @current_dv_batch_id

end
