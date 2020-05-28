CREATE PROC [dbo].[proc_etl_lt_bucks_users] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_lt_bucks_Users

set @insert_date_time = getdate()
insert into dbo.stage_hash_lt_bucks_Users (
       bk_hash,
       user_id,
       user_username,
       user_pass,
       user_fname,
       user_lname,
       user_email,
       user_phone,
       user_fax,
       user_taxid,
       user_birthdate,
       user_job_title,
       user_business_name,
       user_addr1,
       user_addr2,
       user_city,
       user_state,
       user_zip,
       user_language,
       user_country,
       user_type,
       user_register_date,
       user_pending_points,
       user_curr_points,
       user_parent,
       user_dist_id,
       user_promotion,
       user_ref1,
       user_ref2,
       user_ref3,
       user_ref4,
       user_ref5,
       user_optout,
       user_gender,
       user_web_addr,
       user_test,
       user_active,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(user_id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       user_id,
       user_username,
       user_pass,
       user_fname,
       user_lname,
       user_email,
       user_phone,
       user_fax,
       user_taxid,
       user_birthdate,
       user_job_title,
       user_business_name,
       user_addr1,
       user_addr2,
       user_city,
       user_state,
       user_zip,
       user_language,
       user_country,
       user_type,
       user_register_date,
       user_pending_points,
       user_curr_points,
       user_parent,
       user_dist_id,
       user_promotion,
       user_ref1,
       user_ref2,
       user_ref3,
       user_ref4,
       user_ref5,
       user_optout,
       user_gender,
       user_web_addr,
       user_test,
       user_active,
       isnull(cast(stage_lt_bucks_Users.user_register_date as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_lt_bucks_Users
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_lt_bucks_users @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_lt_bucks_users (
       bk_hash,
       user_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_lt_bucks_Users.bk_hash,
       stage_hash_lt_bucks_Users.user_id user_id,
       isnull(cast(stage_hash_lt_bucks_Users.user_register_date as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       5,
       @insert_date_time,
       @user
  from stage_hash_lt_bucks_Users
  left join h_lt_bucks_users
    on stage_hash_lt_bucks_Users.bk_hash = h_lt_bucks_users.bk_hash
 where h_lt_bucks_users_id is null
   and stage_hash_lt_bucks_Users.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_lt_bucks_users
if object_id('tempdb..#l_lt_bucks_users_inserts') is not null drop table #l_lt_bucks_users_inserts
create table #l_lt_bucks_users_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_lt_bucks_Users.bk_hash,
       stage_hash_lt_bucks_Users.user_id user_id,
       stage_hash_lt_bucks_Users.user_parent user_parent,
       stage_hash_lt_bucks_Users.user_dist_id user_dist_id,
       isnull(cast(stage_hash_lt_bucks_Users.user_register_date as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_Users.user_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_Users.user_parent as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_lt_bucks_Users.user_dist_id,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_lt_bucks_Users
 where stage_hash_lt_bucks_Users.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_lt_bucks_users records
set @insert_date_time = getdate()
insert into l_lt_bucks_users (
       bk_hash,
       user_id,
       user_parent,
       user_dist_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_lt_bucks_users_inserts.bk_hash,
       #l_lt_bucks_users_inserts.user_id,
       #l_lt_bucks_users_inserts.user_parent,
       #l_lt_bucks_users_inserts.user_dist_id,
       case when l_lt_bucks_users.l_lt_bucks_users_id is null then isnull(#l_lt_bucks_users_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       5,
       #l_lt_bucks_users_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_lt_bucks_users_inserts
  left join p_lt_bucks_users
    on #l_lt_bucks_users_inserts.bk_hash = p_lt_bucks_users.bk_hash
   and p_lt_bucks_users.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_lt_bucks_users
    on p_lt_bucks_users.bk_hash = l_lt_bucks_users.bk_hash
   and p_lt_bucks_users.l_lt_bucks_users_id = l_lt_bucks_users.l_lt_bucks_users_id
 where l_lt_bucks_users.l_lt_bucks_users_id is null
    or (l_lt_bucks_users.l_lt_bucks_users_id is not null
        and l_lt_bucks_users.dv_hash <> #l_lt_bucks_users_inserts.source_hash)

--calculate hash and lookup to current s_lt_bucks_users
if object_id('tempdb..#s_lt_bucks_users_inserts') is not null drop table #s_lt_bucks_users_inserts
create table #s_lt_bucks_users_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_lt_bucks_Users.bk_hash,
       stage_hash_lt_bucks_Users.user_id user_id,
       stage_hash_lt_bucks_Users.user_username user_username,
       stage_hash_lt_bucks_Users.user_pass user_pass,
       stage_hash_lt_bucks_Users.user_fname user_fname,
       stage_hash_lt_bucks_Users.user_lname user_lname,
       stage_hash_lt_bucks_Users.user_email user_email,
       stage_hash_lt_bucks_Users.user_phone user_phone,
       stage_hash_lt_bucks_Users.user_fax user_fax,
       stage_hash_lt_bucks_Users.user_taxid user_taxid,
       stage_hash_lt_bucks_Users.user_birthdate user_birthdate,
       stage_hash_lt_bucks_Users.user_job_title user_job_title,
       stage_hash_lt_bucks_Users.user_business_name user_business_name,
       stage_hash_lt_bucks_Users.user_addr1 user_addr1,
       stage_hash_lt_bucks_Users.user_addr2 user_addr2,
       stage_hash_lt_bucks_Users.user_city user_city,
       stage_hash_lt_bucks_Users.user_state user_state,
       stage_hash_lt_bucks_Users.user_zip user_zip,
       stage_hash_lt_bucks_Users.user_language user_language,
       stage_hash_lt_bucks_Users.user_country user_country,
       stage_hash_lt_bucks_Users.user_type user_type,
       stage_hash_lt_bucks_Users.user_register_date user_register_date,
       stage_hash_lt_bucks_Users.user_pending_points user_pending_points,
       stage_hash_lt_bucks_Users.user_curr_points user_curr_points,
       stage_hash_lt_bucks_Users.user_promotion user_promotion,
       stage_hash_lt_bucks_Users.user_ref1 user_ref1,
       stage_hash_lt_bucks_Users.user_ref2 user_ref2,
       stage_hash_lt_bucks_Users.user_ref3 user_ref3,
       stage_hash_lt_bucks_Users.user_ref4 user_ref4,
       stage_hash_lt_bucks_Users.user_ref5 user_ref5,
       stage_hash_lt_bucks_Users.user_optout user_optout,
       stage_hash_lt_bucks_Users.user_gender user_gender,
       stage_hash_lt_bucks_Users.user_web_addr user_web_addr,
       stage_hash_lt_bucks_Users.user_test user_test,
       stage_hash_lt_bucks_Users.user_active user_active,
       isnull(cast(stage_hash_lt_bucks_Users.user_register_date as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_Users.user_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_lt_bucks_Users.user_username,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_lt_bucks_Users.user_pass,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_lt_bucks_Users.user_fname,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_lt_bucks_Users.user_lname,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_lt_bucks_Users.user_email,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_lt_bucks_Users.user_phone,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_lt_bucks_Users.user_fax,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_lt_bucks_Users.user_taxid,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_lt_bucks_Users.user_birthdate,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_lt_bucks_Users.user_job_title,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_lt_bucks_Users.user_business_name,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_lt_bucks_Users.user_addr1,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_lt_bucks_Users.user_addr2,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_lt_bucks_Users.user_city,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_lt_bucks_Users.user_state,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_lt_bucks_Users.user_zip,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_lt_bucks_Users.user_language,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_Users.user_country as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_Users.user_type as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_lt_bucks_Users.user_register_date,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_Users.user_pending_points as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_Users.user_curr_points as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_Users.user_promotion as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_Users.user_ref1 as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_Users.user_ref2 as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_Users.user_ref3 as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_Users.user_ref4 as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_lt_bucks_Users.user_ref5,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_Users.user_optout as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_lt_bucks_Users.user_gender,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_lt_bucks_Users.user_web_addr,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_Users.user_test as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_Users.user_active as varchar(42)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_lt_bucks_Users
 where stage_hash_lt_bucks_Users.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_lt_bucks_users records
set @insert_date_time = getdate()
insert into s_lt_bucks_users (
       bk_hash,
       user_id,
       user_username,
       user_pass,
       user_fname,
       user_lname,
       user_email,
       user_phone,
       user_fax,
       user_taxid,
       user_birthdate,
       user_job_title,
       user_business_name,
       user_addr1,
       user_addr2,
       user_city,
       user_state,
       user_zip,
       user_language,
       user_country,
       user_type,
       user_register_date,
       user_pending_points,
       user_curr_points,
       user_promotion,
       user_ref1,
       user_ref2,
       user_ref3,
       user_ref4,
       user_ref5,
       user_optout,
       user_gender,
       user_web_addr,
       user_test,
       user_active,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_lt_bucks_users_inserts.bk_hash,
       #s_lt_bucks_users_inserts.user_id,
       #s_lt_bucks_users_inserts.user_username,
       #s_lt_bucks_users_inserts.user_pass,
       #s_lt_bucks_users_inserts.user_fname,
       #s_lt_bucks_users_inserts.user_lname,
       #s_lt_bucks_users_inserts.user_email,
       #s_lt_bucks_users_inserts.user_phone,
       #s_lt_bucks_users_inserts.user_fax,
       #s_lt_bucks_users_inserts.user_taxid,
       #s_lt_bucks_users_inserts.user_birthdate,
       #s_lt_bucks_users_inserts.user_job_title,
       #s_lt_bucks_users_inserts.user_business_name,
       #s_lt_bucks_users_inserts.user_addr1,
       #s_lt_bucks_users_inserts.user_addr2,
       #s_lt_bucks_users_inserts.user_city,
       #s_lt_bucks_users_inserts.user_state,
       #s_lt_bucks_users_inserts.user_zip,
       #s_lt_bucks_users_inserts.user_language,
       #s_lt_bucks_users_inserts.user_country,
       #s_lt_bucks_users_inserts.user_type,
       #s_lt_bucks_users_inserts.user_register_date,
       #s_lt_bucks_users_inserts.user_pending_points,
       #s_lt_bucks_users_inserts.user_curr_points,
       #s_lt_bucks_users_inserts.user_promotion,
       #s_lt_bucks_users_inserts.user_ref1,
       #s_lt_bucks_users_inserts.user_ref2,
       #s_lt_bucks_users_inserts.user_ref3,
       #s_lt_bucks_users_inserts.user_ref4,
       #s_lt_bucks_users_inserts.user_ref5,
       #s_lt_bucks_users_inserts.user_optout,
       #s_lt_bucks_users_inserts.user_gender,
       #s_lt_bucks_users_inserts.user_web_addr,
       #s_lt_bucks_users_inserts.user_test,
       #s_lt_bucks_users_inserts.user_active,
       case when s_lt_bucks_users.s_lt_bucks_users_id is null then isnull(#s_lt_bucks_users_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       5,
       #s_lt_bucks_users_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_lt_bucks_users_inserts
  left join p_lt_bucks_users
    on #s_lt_bucks_users_inserts.bk_hash = p_lt_bucks_users.bk_hash
   and p_lt_bucks_users.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_lt_bucks_users
    on p_lt_bucks_users.bk_hash = s_lt_bucks_users.bk_hash
   and p_lt_bucks_users.s_lt_bucks_users_id = s_lt_bucks_users.s_lt_bucks_users_id
 where s_lt_bucks_users.s_lt_bucks_users_id is null
    or (s_lt_bucks_users.s_lt_bucks_users_id is not null
        and s_lt_bucks_users.dv_hash <> #s_lt_bucks_users_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_lt_bucks_users @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_lt_bucks_users @current_dv_batch_id

end
