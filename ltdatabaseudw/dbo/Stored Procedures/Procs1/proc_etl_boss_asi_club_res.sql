CREATE PROC [dbo].[proc_etl_boss_asi_club_res] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_boss_asiclubres

set @insert_date_time = getdate()
insert into dbo.stage_hash_boss_asiclubres (
       bk_hash,
       club,
       resource_id,
       status,
       default_upccode,
       empl_id,
       display_seq,
       resource_type,
       resource,
       comment,
       resource_type_id,
       square_feet,
       employee_id,
       created_at,
       updated_at,
       capacity,
       web_enable,
       web_start_date,
       web_active,
       inactive_start_date,
       inactive_end_date,
       phone,
       floor,
       web_description,
       supportPhone,
       supportEmail,
       resourcePhone,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(club as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(resource_id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       club,
       resource_id,
       status,
       default_upccode,
       empl_id,
       display_seq,
       resource_type,
       resource,
       comment,
       resource_type_id,
       square_feet,
       employee_id,
       created_at,
       updated_at,
       capacity,
       web_enable,
       web_start_date,
       web_active,
       inactive_start_date,
       inactive_end_date,
       phone,
       floor,
       web_description,
       supportPhone,
       supportEmail,
       resourcePhone,
       isnull(cast(stage_boss_asiclubres.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_boss_asiclubres
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_boss_asi_club_res @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_boss_asi_club_res (
       bk_hash,
       club,
       resource_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_boss_asiclubres.bk_hash,
       stage_hash_boss_asiclubres.club club,
       stage_hash_boss_asiclubres.resource_id resource_id,
       isnull(cast(stage_hash_boss_asiclubres.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       26,
       @insert_date_time,
       @user
  from stage_hash_boss_asiclubres
  left join h_boss_asi_club_res
    on stage_hash_boss_asiclubres.bk_hash = h_boss_asi_club_res.bk_hash
 where h_boss_asi_club_res_id is null
   and stage_hash_boss_asiclubres.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_boss_asi_club_res
if object_id('tempdb..#l_boss_asi_club_res_inserts') is not null drop table #l_boss_asi_club_res_inserts
create table #l_boss_asi_club_res_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_boss_asiclubres.bk_hash,
       stage_hash_boss_asiclubres.club club,
       stage_hash_boss_asiclubres.resource_id resource_id,
       stage_hash_boss_asiclubres.default_upccode default_upccode,
       stage_hash_boss_asiclubres.empl_id empl_id,
       stage_hash_boss_asiclubres.display_seq display_seq,
       stage_hash_boss_asiclubres.resource_type_id resource_type_id,
       stage_hash_boss_asiclubres.employee_id employee_id,
       isnull(cast(stage_hash_boss_asiclubres.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_boss_asiclubres.club as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asiclubres.resource_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asiclubres.default_upccode,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asiclubres.empl_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asiclubres.display_seq as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asiclubres.resource_type_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asiclubres.employee_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_boss_asiclubres
 where stage_hash_boss_asiclubres.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_boss_asi_club_res records
set @insert_date_time = getdate()
insert into l_boss_asi_club_res (
       bk_hash,
       club,
       resource_id,
       default_upccode,
       empl_id,
       display_seq,
       resource_type_id,
       employee_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_boss_asi_club_res_inserts.bk_hash,
       #l_boss_asi_club_res_inserts.club,
       #l_boss_asi_club_res_inserts.resource_id,
       #l_boss_asi_club_res_inserts.default_upccode,
       #l_boss_asi_club_res_inserts.empl_id,
       #l_boss_asi_club_res_inserts.display_seq,
       #l_boss_asi_club_res_inserts.resource_type_id,
       #l_boss_asi_club_res_inserts.employee_id,
       case when l_boss_asi_club_res.l_boss_asi_club_res_id is null then isnull(#l_boss_asi_club_res_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       26,
       #l_boss_asi_club_res_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_boss_asi_club_res_inserts
  left join p_boss_asi_club_res
    on #l_boss_asi_club_res_inserts.bk_hash = p_boss_asi_club_res.bk_hash
   and p_boss_asi_club_res.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_boss_asi_club_res
    on p_boss_asi_club_res.bk_hash = l_boss_asi_club_res.bk_hash
   and p_boss_asi_club_res.l_boss_asi_club_res_id = l_boss_asi_club_res.l_boss_asi_club_res_id
 where l_boss_asi_club_res.l_boss_asi_club_res_id is null
    or (l_boss_asi_club_res.l_boss_asi_club_res_id is not null
        and l_boss_asi_club_res.dv_hash <> #l_boss_asi_club_res_inserts.source_hash)

--calculate hash and lookup to current s_boss_asi_club_res
if object_id('tempdb..#s_boss_asi_club_res_inserts') is not null drop table #s_boss_asi_club_res_inserts
create table #s_boss_asi_club_res_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_boss_asiclubres.bk_hash,
       stage_hash_boss_asiclubres.club club,
       stage_hash_boss_asiclubres.resource_id resource_id,
       stage_hash_boss_asiclubres.status status,
       stage_hash_boss_asiclubres.resource_type resource_type,
       stage_hash_boss_asiclubres.resource resource,
       stage_hash_boss_asiclubres.comment comment,
       stage_hash_boss_asiclubres.square_feet square_feet,
       stage_hash_boss_asiclubres.created_at created_at,
       stage_hash_boss_asiclubres.updated_at updated_at,
       stage_hash_boss_asiclubres.capacity capacity,
       stage_hash_boss_asiclubres.web_enable web_enable,
       stage_hash_boss_asiclubres.web_start_date web_start_date,
       stage_hash_boss_asiclubres.web_active web_active,
       stage_hash_boss_asiclubres.inactive_start_date inactive_start_date,
       stage_hash_boss_asiclubres.inactive_end_date inactive_end_date,
       stage_hash_boss_asiclubres.phone phone,
       stage_hash_boss_asiclubres.floor floor,
       stage_hash_boss_asiclubres.web_description web_description,
       stage_hash_boss_asiclubres.supportPhone support_phone,
       stage_hash_boss_asiclubres.supportEmail support_email,
       stage_hash_boss_asiclubres.resourcePhone resource_phone,
       isnull(cast(stage_hash_boss_asiclubres.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_boss_asiclubres.club as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asiclubres.resource_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asiclubres.status,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asiclubres.resource_type,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asiclubres.resource,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asiclubres.comment,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asiclubres.square_feet as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_boss_asiclubres.created_at,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_boss_asiclubres.updated_at,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asiclubres.capacity as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asiclubres.web_enable,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_boss_asiclubres.web_start_date,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asiclubres.web_active,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_boss_asiclubres.inactive_start_date,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_boss_asiclubres.inactive_end_date,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asiclubres.phone,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asiclubres.floor as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asiclubres.web_description,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asiclubres.supportPhone,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asiclubres.supportEmail,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asiclubres.resourcePhone,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_boss_asiclubres
 where stage_hash_boss_asiclubres.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_boss_asi_club_res records
set @insert_date_time = getdate()
insert into s_boss_asi_club_res (
       bk_hash,
       club,
       resource_id,
       status,
       resource_type,
       resource,
       comment,
       square_feet,
       created_at,
       updated_at,
       capacity,
       web_enable,
       web_start_date,
       web_active,
       inactive_start_date,
       inactive_end_date,
       phone,
       floor,
       web_description,
       support_phone,
       support_email,
       resource_phone,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_boss_asi_club_res_inserts.bk_hash,
       #s_boss_asi_club_res_inserts.club,
       #s_boss_asi_club_res_inserts.resource_id,
       #s_boss_asi_club_res_inserts.status,
       #s_boss_asi_club_res_inserts.resource_type,
       #s_boss_asi_club_res_inserts.resource,
       #s_boss_asi_club_res_inserts.comment,
       #s_boss_asi_club_res_inserts.square_feet,
       #s_boss_asi_club_res_inserts.created_at,
       #s_boss_asi_club_res_inserts.updated_at,
       #s_boss_asi_club_res_inserts.capacity,
       #s_boss_asi_club_res_inserts.web_enable,
       #s_boss_asi_club_res_inserts.web_start_date,
       #s_boss_asi_club_res_inserts.web_active,
       #s_boss_asi_club_res_inserts.inactive_start_date,
       #s_boss_asi_club_res_inserts.inactive_end_date,
       #s_boss_asi_club_res_inserts.phone,
       #s_boss_asi_club_res_inserts.floor,
       #s_boss_asi_club_res_inserts.web_description,
       #s_boss_asi_club_res_inserts.support_phone,
       #s_boss_asi_club_res_inserts.support_email,
       #s_boss_asi_club_res_inserts.resource_phone,
       case when s_boss_asi_club_res.s_boss_asi_club_res_id is null then isnull(#s_boss_asi_club_res_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       26,
       #s_boss_asi_club_res_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_boss_asi_club_res_inserts
  left join p_boss_asi_club_res
    on #s_boss_asi_club_res_inserts.bk_hash = p_boss_asi_club_res.bk_hash
   and p_boss_asi_club_res.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_boss_asi_club_res
    on p_boss_asi_club_res.bk_hash = s_boss_asi_club_res.bk_hash
   and p_boss_asi_club_res.s_boss_asi_club_res_id = s_boss_asi_club_res.s_boss_asi_club_res_id
 where s_boss_asi_club_res.s_boss_asi_club_res_id is null
    or (s_boss_asi_club_res.s_boss_asi_club_res_id is not null
        and s_boss_asi_club_res.dv_hash <> #s_boss_asi_club_res_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_boss_asi_club_res @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_boss_asi_club_res @current_dv_batch_id

end
