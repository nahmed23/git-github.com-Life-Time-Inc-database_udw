CREATE PROC [dbo].[proc_etl_chronotrack_entry] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_chronotrack_entry

set @insert_date_time = getdate()
insert into dbo.stage_hash_chronotrack_entry (
       bk_hash,
       id,
       race_id,
       trans_id,
       team_id,
       athlete_id,
       wave_id,
       apply_wave_rule,
       auto_bracket_policy,
       prefered_bracket_id,
       remove_bracket,
       primary_bracket_id,
       override_bracket_rule,
       external_id,
       reg_option_id,
       reg_sms,
       reg_soc_msg,
       allow_tracking,
       race_age,
       bib,
       name,
       type,
       status,
       location_string,
       notes,
       search_result,
       ctime,
       mtime,
       check_in,
       source_type,
       platform_source,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       id,
       race_id,
       trans_id,
       team_id,
       athlete_id,
       wave_id,
       apply_wave_rule,
       auto_bracket_policy,
       prefered_bracket_id,
       remove_bracket,
       primary_bracket_id,
       override_bracket_rule,
       external_id,
       reg_option_id,
       reg_sms,
       reg_soc_msg,
       allow_tracking,
       race_age,
       bib,
       name,
       type,
       status,
       location_string,
       notes,
       search_result,
       ctime,
       mtime,
       check_in,
       source_type,
       platform_source,
       dummy_modified_date_time,
       isnull(cast(stage_chronotrack_entry.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_chronotrack_entry
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_chronotrack_entry @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_chronotrack_entry (
       bk_hash,
       entry_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_chronotrack_entry.bk_hash,
       stage_hash_chronotrack_entry.id entry_id,
       isnull(cast(stage_hash_chronotrack_entry.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       46,
       @insert_date_time,
       @user
  from stage_hash_chronotrack_entry
  left join h_chronotrack_entry
    on stage_hash_chronotrack_entry.bk_hash = h_chronotrack_entry.bk_hash
 where h_chronotrack_entry_id is null
   and stage_hash_chronotrack_entry.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_chronotrack_entry
if object_id('tempdb..#l_chronotrack_entry_inserts') is not null drop table #l_chronotrack_entry_inserts
create table #l_chronotrack_entry_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_chronotrack_entry.bk_hash,
       stage_hash_chronotrack_entry.id entry_id,
       stage_hash_chronotrack_entry.race_id race_id,
       stage_hash_chronotrack_entry.trans_id trans_id,
       stage_hash_chronotrack_entry.team_id team_id,
       stage_hash_chronotrack_entry.athlete_id athlete_id,
       stage_hash_chronotrack_entry.wave_id wave_id,
       stage_hash_chronotrack_entry.prefered_bracket_id prefered_bracket_id,
       stage_hash_chronotrack_entry.primary_bracket_id primary_bracket_id,
       stage_hash_chronotrack_entry.external_id external_id,
       stage_hash_chronotrack_entry.reg_option_id reg_option_id,
       isnull(cast(stage_hash_chronotrack_entry.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_chronotrack_entry.id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_entry.race_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_entry.trans_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_entry.team_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_entry.athlete_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_entry.wave_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_entry.prefered_bracket_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_entry.primary_bracket_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_entry.external_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_entry.reg_option_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_chronotrack_entry
 where stage_hash_chronotrack_entry.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_chronotrack_entry records
set @insert_date_time = getdate()
insert into l_chronotrack_entry (
       bk_hash,
       entry_id,
       race_id,
       trans_id,
       team_id,
       athlete_id,
       wave_id,
       prefered_bracket_id,
       primary_bracket_id,
       external_id,
       reg_option_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_chronotrack_entry_inserts.bk_hash,
       #l_chronotrack_entry_inserts.entry_id,
       #l_chronotrack_entry_inserts.race_id,
       #l_chronotrack_entry_inserts.trans_id,
       #l_chronotrack_entry_inserts.team_id,
       #l_chronotrack_entry_inserts.athlete_id,
       #l_chronotrack_entry_inserts.wave_id,
       #l_chronotrack_entry_inserts.prefered_bracket_id,
       #l_chronotrack_entry_inserts.primary_bracket_id,
       #l_chronotrack_entry_inserts.external_id,
       #l_chronotrack_entry_inserts.reg_option_id,
       case when l_chronotrack_entry.l_chronotrack_entry_id is null then isnull(#l_chronotrack_entry_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       46,
       #l_chronotrack_entry_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_chronotrack_entry_inserts
  left join p_chronotrack_entry
    on #l_chronotrack_entry_inserts.bk_hash = p_chronotrack_entry.bk_hash
   and p_chronotrack_entry.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_chronotrack_entry
    on p_chronotrack_entry.bk_hash = l_chronotrack_entry.bk_hash
   and p_chronotrack_entry.l_chronotrack_entry_id = l_chronotrack_entry.l_chronotrack_entry_id
 where l_chronotrack_entry.l_chronotrack_entry_id is null
    or (l_chronotrack_entry.l_chronotrack_entry_id is not null
        and l_chronotrack_entry.dv_hash <> #l_chronotrack_entry_inserts.source_hash)

--calculate hash and lookup to current s_chronotrack_entry
if object_id('tempdb..#s_chronotrack_entry_inserts') is not null drop table #s_chronotrack_entry_inserts
create table #s_chronotrack_entry_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_chronotrack_entry.bk_hash,
       stage_hash_chronotrack_entry.id entry_id,
       stage_hash_chronotrack_entry.apply_wave_rule apply_wave_rule,
       stage_hash_chronotrack_entry.auto_bracket_policy auto_bracket_policy,
       stage_hash_chronotrack_entry.remove_bracket remove_bracket,
       stage_hash_chronotrack_entry.override_bracket_rule override_bracket_rule,
       stage_hash_chronotrack_entry.reg_sms reg_sms,
       stage_hash_chronotrack_entry.reg_soc_msg reg_soc_msg,
       stage_hash_chronotrack_entry.allow_tracking allow_tracking,
       stage_hash_chronotrack_entry.race_age race_age,
       stage_hash_chronotrack_entry.bib bib,
       stage_hash_chronotrack_entry.name name,
       stage_hash_chronotrack_entry.type type,
       stage_hash_chronotrack_entry.status status,
       stage_hash_chronotrack_entry.location_string location_string,
       stage_hash_chronotrack_entry.notes notes,
       stage_hash_chronotrack_entry.search_result search_result,
       stage_hash_chronotrack_entry.ctime ctime,
       stage_hash_chronotrack_entry.mtime mtime,
       stage_hash_chronotrack_entry.check_in check_in,
       stage_hash_chronotrack_entry.source_type source_type,
       stage_hash_chronotrack_entry.platform_source platform_source,
       stage_hash_chronotrack_entry.dummy_modified_date_time dummy_modified_date_time,
       isnull(cast(stage_hash_chronotrack_entry.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_chronotrack_entry.id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_entry.apply_wave_rule as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_entry.auto_bracket_policy,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_entry.remove_bracket,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_entry.override_bracket_rule as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_entry.reg_sms as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_entry.reg_soc_msg as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_entry.allow_tracking as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_entry.race_age as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_entry.bib,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_entry.name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_entry.type,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_entry.status,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_entry.location_string,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_entry.notes,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_entry.search_result,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_entry.ctime as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_entry.mtime as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_entry.check_in as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_entry.source_type,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_entry.platform_source,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_chronotrack_entry
 where stage_hash_chronotrack_entry.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_chronotrack_entry records
set @insert_date_time = getdate()
insert into s_chronotrack_entry (
       bk_hash,
       entry_id,
       apply_wave_rule,
       auto_bracket_policy,
       remove_bracket,
       override_bracket_rule,
       reg_sms,
       reg_soc_msg,
       allow_tracking,
       race_age,
       bib,
       name,
       type,
       status,
       location_string,
       notes,
       search_result,
       ctime,
       mtime,
       check_in,
       source_type,
       platform_source,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_chronotrack_entry_inserts.bk_hash,
       #s_chronotrack_entry_inserts.entry_id,
       #s_chronotrack_entry_inserts.apply_wave_rule,
       #s_chronotrack_entry_inserts.auto_bracket_policy,
       #s_chronotrack_entry_inserts.remove_bracket,
       #s_chronotrack_entry_inserts.override_bracket_rule,
       #s_chronotrack_entry_inserts.reg_sms,
       #s_chronotrack_entry_inserts.reg_soc_msg,
       #s_chronotrack_entry_inserts.allow_tracking,
       #s_chronotrack_entry_inserts.race_age,
       #s_chronotrack_entry_inserts.bib,
       #s_chronotrack_entry_inserts.name,
       #s_chronotrack_entry_inserts.type,
       #s_chronotrack_entry_inserts.status,
       #s_chronotrack_entry_inserts.location_string,
       #s_chronotrack_entry_inserts.notes,
       #s_chronotrack_entry_inserts.search_result,
       #s_chronotrack_entry_inserts.ctime,
       #s_chronotrack_entry_inserts.mtime,
       #s_chronotrack_entry_inserts.check_in,
       #s_chronotrack_entry_inserts.source_type,
       #s_chronotrack_entry_inserts.platform_source,
       #s_chronotrack_entry_inserts.dummy_modified_date_time,
       case when s_chronotrack_entry.s_chronotrack_entry_id is null then isnull(#s_chronotrack_entry_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       46,
       #s_chronotrack_entry_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_chronotrack_entry_inserts
  left join p_chronotrack_entry
    on #s_chronotrack_entry_inserts.bk_hash = p_chronotrack_entry.bk_hash
   and p_chronotrack_entry.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_chronotrack_entry
    on p_chronotrack_entry.bk_hash = s_chronotrack_entry.bk_hash
   and p_chronotrack_entry.s_chronotrack_entry_id = s_chronotrack_entry.s_chronotrack_entry_id
 where s_chronotrack_entry.s_chronotrack_entry_id is null
    or (s_chronotrack_entry.s_chronotrack_entry_id is not null
        and s_chronotrack_entry.dv_hash <> #s_chronotrack_entry_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_chronotrack_entry @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_chronotrack_entry @current_dv_batch_id

end
