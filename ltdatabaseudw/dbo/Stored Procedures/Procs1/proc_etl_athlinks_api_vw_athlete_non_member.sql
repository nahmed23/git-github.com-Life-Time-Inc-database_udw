CREATE PROC [dbo].[proc_etl_athlinks_api_vw_athlete_non_member] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_athlinks_api_vw_AthleteNonMember

set @insert_date_time = getdate()
insert into dbo.stage_hash_athlinks_api_vw_AthleteNonMember (
       bk_hash,
       RacerID,
       FName,
       LName,
       DisplayName,
       Age,
       Gender,
       City,
       StateProvID,
       StateProvName,
       StateProvAbbrev,
       CountryID,
       CountryID3,
       CountryName,
       PhotoPath,
       JoinDate,
       Notes,
       OwnerID,
       IsMember,
       ResultCount,
       CreateDate,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(RacerID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       RacerID,
       FName,
       LName,
       DisplayName,
       Age,
       Gender,
       City,
       StateProvID,
       StateProvName,
       StateProvAbbrev,
       CountryID,
       CountryID3,
       CountryName,
       PhotoPath,
       JoinDate,
       Notes,
       OwnerID,
       IsMember,
       ResultCount,
       CreateDate,
       isnull(cast(stage_athlinks_api_vw_AthleteNonMember.CreateDate as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_athlinks_api_vw_AthleteNonMember
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_athlinks_api_vw_athlete_non_member @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_athlinks_api_vw_athlete_non_member (
       bk_hash,
       racer_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_athlinks_api_vw_AthleteNonMember.bk_hash,
       stage_hash_athlinks_api_vw_AthleteNonMember.RacerID racer_id,
       isnull(cast(stage_hash_athlinks_api_vw_AthleteNonMember.CreateDate as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       45,
       @insert_date_time,
       @user
  from stage_hash_athlinks_api_vw_AthleteNonMember
  left join h_athlinks_api_vw_athlete_non_member
    on stage_hash_athlinks_api_vw_AthleteNonMember.bk_hash = h_athlinks_api_vw_athlete_non_member.bk_hash
 where h_athlinks_api_vw_athlete_non_member_id is null
   and stage_hash_athlinks_api_vw_AthleteNonMember.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current s_athlinks_api_vw_athlete_non_member
if object_id('tempdb..#s_athlinks_api_vw_athlete_non_member_inserts') is not null drop table #s_athlinks_api_vw_athlete_non_member_inserts
create table #s_athlinks_api_vw_athlete_non_member_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_athlinks_api_vw_AthleteNonMember.bk_hash,
       stage_hash_athlinks_api_vw_AthleteNonMember.RacerID racer_id,
       stage_hash_athlinks_api_vw_AthleteNonMember.FName f_name,
       stage_hash_athlinks_api_vw_AthleteNonMember.LName l_name,
       stage_hash_athlinks_api_vw_AthleteNonMember.DisplayName display_name,
       stage_hash_athlinks_api_vw_AthleteNonMember.Age age,
       stage_hash_athlinks_api_vw_AthleteNonMember.Gender gender,
       stage_hash_athlinks_api_vw_AthleteNonMember.City city,
       stage_hash_athlinks_api_vw_AthleteNonMember.StateProvID state_prov_id,
       stage_hash_athlinks_api_vw_AthleteNonMember.StateProvName state_prov_name,
       stage_hash_athlinks_api_vw_AthleteNonMember.StateProvAbbrev state_prov_abbrev,
       stage_hash_athlinks_api_vw_AthleteNonMember.CountryID country_id,
       stage_hash_athlinks_api_vw_AthleteNonMember.CountryID3 country_id_3,
       stage_hash_athlinks_api_vw_AthleteNonMember.CountryName country_name,
       stage_hash_athlinks_api_vw_AthleteNonMember.PhotoPath photo_path,
       stage_hash_athlinks_api_vw_AthleteNonMember.JoinDate join_date,
       stage_hash_athlinks_api_vw_AthleteNonMember.Notes notes,
       stage_hash_athlinks_api_vw_AthleteNonMember.OwnerID owner_id,
       stage_hash_athlinks_api_vw_AthleteNonMember.IsMember is_member,
       stage_hash_athlinks_api_vw_AthleteNonMember.ResultCount result_count,
       stage_hash_athlinks_api_vw_AthleteNonMember.CreateDate create_date,
       isnull(cast(stage_hash_athlinks_api_vw_AthleteNonMember.CreateDate as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_athlinks_api_vw_AthleteNonMember.RacerID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_athlinks_api_vw_AthleteNonMember.FName,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_athlinks_api_vw_AthleteNonMember.LName,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_athlinks_api_vw_AthleteNonMember.DisplayName,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_athlinks_api_vw_AthleteNonMember.Age as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_athlinks_api_vw_AthleteNonMember.Gender,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_athlinks_api_vw_AthleteNonMember.City,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_athlinks_api_vw_AthleteNonMember.StateProvID,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_athlinks_api_vw_AthleteNonMember.StateProvName,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_athlinks_api_vw_AthleteNonMember.StateProvAbbrev,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_athlinks_api_vw_AthleteNonMember.CountryID,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_athlinks_api_vw_AthleteNonMember.CountryID3,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_athlinks_api_vw_AthleteNonMember.CountryName,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_athlinks_api_vw_AthleteNonMember.PhotoPath,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_athlinks_api_vw_AthleteNonMember.JoinDate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_athlinks_api_vw_AthleteNonMember.Notes,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_athlinks_api_vw_AthleteNonMember.OwnerID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_athlinks_api_vw_AthleteNonMember.IsMember,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_athlinks_api_vw_AthleteNonMember.ResultCount as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_athlinks_api_vw_AthleteNonMember
 where stage_hash_athlinks_api_vw_AthleteNonMember.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_athlinks_api_vw_athlete_non_member records
set @insert_date_time = getdate()
insert into s_athlinks_api_vw_athlete_non_member (
       bk_hash,
       racer_id,
       f_name,
       l_name,
       display_name,
       age,
       gender,
       city,
       state_prov_id,
       state_prov_name,
       state_prov_abbrev,
       country_id,
       country_id_3,
       country_name,
       photo_path,
       join_date,
       notes,
       owner_id,
       is_member,
       result_count,
       create_date,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_athlinks_api_vw_athlete_non_member_inserts.bk_hash,
       #s_athlinks_api_vw_athlete_non_member_inserts.racer_id,
       #s_athlinks_api_vw_athlete_non_member_inserts.f_name,
       #s_athlinks_api_vw_athlete_non_member_inserts.l_name,
       #s_athlinks_api_vw_athlete_non_member_inserts.display_name,
       #s_athlinks_api_vw_athlete_non_member_inserts.age,
       #s_athlinks_api_vw_athlete_non_member_inserts.gender,
       #s_athlinks_api_vw_athlete_non_member_inserts.city,
       #s_athlinks_api_vw_athlete_non_member_inserts.state_prov_id,
       #s_athlinks_api_vw_athlete_non_member_inserts.state_prov_name,
       #s_athlinks_api_vw_athlete_non_member_inserts.state_prov_abbrev,
       #s_athlinks_api_vw_athlete_non_member_inserts.country_id,
       #s_athlinks_api_vw_athlete_non_member_inserts.country_id_3,
       #s_athlinks_api_vw_athlete_non_member_inserts.country_name,
       #s_athlinks_api_vw_athlete_non_member_inserts.photo_path,
       #s_athlinks_api_vw_athlete_non_member_inserts.join_date,
       #s_athlinks_api_vw_athlete_non_member_inserts.notes,
       #s_athlinks_api_vw_athlete_non_member_inserts.owner_id,
       #s_athlinks_api_vw_athlete_non_member_inserts.is_member,
       #s_athlinks_api_vw_athlete_non_member_inserts.result_count,
       #s_athlinks_api_vw_athlete_non_member_inserts.create_date,
       case when s_athlinks_api_vw_athlete_non_member.s_athlinks_api_vw_athlete_non_member_id is null then isnull(#s_athlinks_api_vw_athlete_non_member_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       45,
       #s_athlinks_api_vw_athlete_non_member_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_athlinks_api_vw_athlete_non_member_inserts
  left join p_athlinks_api_vw_athlete_non_member
    on #s_athlinks_api_vw_athlete_non_member_inserts.bk_hash = p_athlinks_api_vw_athlete_non_member.bk_hash
   and p_athlinks_api_vw_athlete_non_member.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_athlinks_api_vw_athlete_non_member
    on p_athlinks_api_vw_athlete_non_member.bk_hash = s_athlinks_api_vw_athlete_non_member.bk_hash
   and p_athlinks_api_vw_athlete_non_member.s_athlinks_api_vw_athlete_non_member_id = s_athlinks_api_vw_athlete_non_member.s_athlinks_api_vw_athlete_non_member_id
 where s_athlinks_api_vw_athlete_non_member.s_athlinks_api_vw_athlete_non_member_id is null
    or (s_athlinks_api_vw_athlete_non_member.s_athlinks_api_vw_athlete_non_member_id is not null
        and s_athlinks_api_vw_athlete_non_member.dv_hash <> #s_athlinks_api_vw_athlete_non_member_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_athlinks_api_vw_athlete_non_member @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_athlinks_api_vw_athlete_non_member @current_dv_batch_id

end
