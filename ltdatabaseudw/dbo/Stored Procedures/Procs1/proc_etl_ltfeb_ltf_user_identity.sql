CREATE PROC [dbo].[proc_etl_ltfeb_ltf_user_identity] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_ltfeb_LTFUserIdentity

set @insert_date_time = getdate()
insert into dbo.stage_hash_ltfeb_LTFUserIdentity (
       bk_hash,
       party_id,
       ltf_user_name,
       ltf_user_secret_question,
       ltf_user_secret_answer,
       lui_identity_status,
       lui_identity_status_from_datetime,
       lui_identity_status_thru_datetime,
       lui_n_failed_attempts,
       lui_user_agreement_version_number,
       update_datetime,
       update_userid,
       password_update_datetime,
       password_change_required,
       last_successful_login_datetime,
       ltf_user_token,
       token_expiration_datetime,
       token_create_datetime,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(party_id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       party_id,
       ltf_user_name,
       ltf_user_secret_question,
       ltf_user_secret_answer,
       lui_identity_status,
       lui_identity_status_from_datetime,
       lui_identity_status_thru_datetime,
       lui_n_failed_attempts,
       lui_user_agreement_version_number,
       update_datetime,
       update_userid,
       password_update_datetime,
       password_change_required,
       last_successful_login_datetime,
       ltf_user_token,
       token_expiration_datetime,
       token_create_datetime,
       isnull(cast(stage_ltfeb_LTFUserIdentity.update_datetime as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_ltfeb_LTFUserIdentity
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_ltfeb_ltf_user_identity @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_ltfeb_ltf_user_identity (
       bk_hash,
       party_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_ltfeb_LTFUserIdentity.bk_hash,
       stage_hash_ltfeb_LTFUserIdentity.party_id party_id,
       isnull(cast(stage_hash_ltfeb_LTFUserIdentity.update_datetime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       18,
       @insert_date_time,
       @user
  from stage_hash_ltfeb_LTFUserIdentity
  left join h_ltfeb_ltf_user_identity
    on stage_hash_ltfeb_LTFUserIdentity.bk_hash = h_ltfeb_ltf_user_identity.bk_hash
 where h_ltfeb_ltf_user_identity_id is null
   and stage_hash_ltfeb_LTFUserIdentity.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current s_ltfeb_ltf_user_identity
if object_id('tempdb..#s_ltfeb_ltf_user_identity_inserts') is not null drop table #s_ltfeb_ltf_user_identity_inserts
create table #s_ltfeb_ltf_user_identity_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_ltfeb_LTFUserIdentity.bk_hash,
       stage_hash_ltfeb_LTFUserIdentity.party_id party_id,
       stage_hash_ltfeb_LTFUserIdentity.ltf_user_name ltf_user_name,
       stage_hash_ltfeb_LTFUserIdentity.ltf_user_secret_question ltf_user_secret_question,
       stage_hash_ltfeb_LTFUserIdentity.ltf_user_secret_answer ltf_user_secret_answer,
       stage_hash_ltfeb_LTFUserIdentity.lui_identity_status lui_identity_status,
       stage_hash_ltfeb_LTFUserIdentity.lui_identity_status_from_datetime lui_identity_status_from_date_time,
       stage_hash_ltfeb_LTFUserIdentity.lui_identity_status_thru_datetime lui_identity_status_thru_date_time,
       stage_hash_ltfeb_LTFUserIdentity.lui_n_failed_attempts lui_n_failed_attempts,
       stage_hash_ltfeb_LTFUserIdentity.lui_user_agreement_version_number lui_user_agreement_version_number,
       stage_hash_ltfeb_LTFUserIdentity.update_datetime update_date_time,
       stage_hash_ltfeb_LTFUserIdentity.update_userid update_user_id,
       stage_hash_ltfeb_LTFUserIdentity.password_update_datetime password_update_date_time,
       stage_hash_ltfeb_LTFUserIdentity.password_change_required password_change_required,
       stage_hash_ltfeb_LTFUserIdentity.last_successful_login_datetime last_successful_login_date_time,
       stage_hash_ltfeb_LTFUserIdentity.ltf_user_token ltf_user_token,
       stage_hash_ltfeb_LTFUserIdentity.token_expiration_datetime token_expiration_date_time,
       stage_hash_ltfeb_LTFUserIdentity.token_create_datetime token_create_date_time,
       isnull(cast(stage_hash_ltfeb_LTFUserIdentity.update_datetime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_ltfeb_LTFUserIdentity.party_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ltfeb_LTFUserIdentity.ltf_user_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ltfeb_LTFUserIdentity.ltf_user_secret_question,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar(500), stage_hash_ltfeb_LTFUserIdentity.ltf_user_secret_answer, 2),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ltfeb_LTFUserIdentity.lui_identity_status,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_ltfeb_LTFUserIdentity.lui_identity_status_from_datetime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_ltfeb_LTFUserIdentity.lui_identity_status_thru_datetime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ltfeb_LTFUserIdentity.lui_n_failed_attempts as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ltfeb_LTFUserIdentity.lui_user_agreement_version_number as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_ltfeb_LTFUserIdentity.update_datetime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ltfeb_LTFUserIdentity.update_userid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_ltfeb_LTFUserIdentity.password_update_datetime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ltfeb_LTFUserIdentity.password_change_required as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_ltfeb_LTFUserIdentity.last_successful_login_datetime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ltfeb_LTFUserIdentity.ltf_user_token,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_ltfeb_LTFUserIdentity.token_expiration_datetime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_ltfeb_LTFUserIdentity.token_create_datetime,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_ltfeb_LTFUserIdentity
 where stage_hash_ltfeb_LTFUserIdentity.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_ltfeb_ltf_user_identity records
set @insert_date_time = getdate()
insert into s_ltfeb_ltf_user_identity (
       bk_hash,
       party_id,
       ltf_user_name,
       ltf_user_secret_question,
       ltf_user_secret_answer,
       lui_identity_status,
       lui_identity_status_from_date_time,
       lui_identity_status_thru_date_time,
       lui_n_failed_attempts,
       lui_user_agreement_version_number,
       update_date_time,
       update_user_id,
       password_update_date_time,
       password_change_required,
       last_successful_login_date_time,
       ltf_user_token,
       token_expiration_date_time,
       token_create_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_ltfeb_ltf_user_identity_inserts.bk_hash,
       #s_ltfeb_ltf_user_identity_inserts.party_id,
       #s_ltfeb_ltf_user_identity_inserts.ltf_user_name,
       #s_ltfeb_ltf_user_identity_inserts.ltf_user_secret_question,
       #s_ltfeb_ltf_user_identity_inserts.ltf_user_secret_answer,
       #s_ltfeb_ltf_user_identity_inserts.lui_identity_status,
       #s_ltfeb_ltf_user_identity_inserts.lui_identity_status_from_date_time,
       #s_ltfeb_ltf_user_identity_inserts.lui_identity_status_thru_date_time,
       #s_ltfeb_ltf_user_identity_inserts.lui_n_failed_attempts,
       #s_ltfeb_ltf_user_identity_inserts.lui_user_agreement_version_number,
       #s_ltfeb_ltf_user_identity_inserts.update_date_time,
       #s_ltfeb_ltf_user_identity_inserts.update_user_id,
       #s_ltfeb_ltf_user_identity_inserts.password_update_date_time,
       #s_ltfeb_ltf_user_identity_inserts.password_change_required,
       #s_ltfeb_ltf_user_identity_inserts.last_successful_login_date_time,
       #s_ltfeb_ltf_user_identity_inserts.ltf_user_token,
       #s_ltfeb_ltf_user_identity_inserts.token_expiration_date_time,
       #s_ltfeb_ltf_user_identity_inserts.token_create_date_time,
       case when s_ltfeb_ltf_user_identity.s_ltfeb_ltf_user_identity_id is null then isnull(#s_ltfeb_ltf_user_identity_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       18,
       #s_ltfeb_ltf_user_identity_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_ltfeb_ltf_user_identity_inserts
  left join p_ltfeb_ltf_user_identity
    on #s_ltfeb_ltf_user_identity_inserts.bk_hash = p_ltfeb_ltf_user_identity.bk_hash
   and p_ltfeb_ltf_user_identity.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_ltfeb_ltf_user_identity
    on p_ltfeb_ltf_user_identity.bk_hash = s_ltfeb_ltf_user_identity.bk_hash
   and p_ltfeb_ltf_user_identity.s_ltfeb_ltf_user_identity_id = s_ltfeb_ltf_user_identity.s_ltfeb_ltf_user_identity_id
 where s_ltfeb_ltf_user_identity.s_ltfeb_ltf_user_identity_id is null
    or (s_ltfeb_ltf_user_identity.s_ltfeb_ltf_user_identity_id is not null
        and s_ltfeb_ltf_user_identity.dv_hash <> #s_ltfeb_ltf_user_identity_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_ltfeb_ltf_user_identity @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_ltfeb_ltf_user_identity @current_dv_batch_id

end
