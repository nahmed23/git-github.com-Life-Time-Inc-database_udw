CREATE PROC [dbo].[proc_etl_magento_customer_entity] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_magento_customer_entity

set @insert_date_time = getdate()
insert into dbo.stage_hash_magento_customer_entity (
       bk_hash,
       entity_id,
       website_id,
       email,
       group_id,
       increment_id,
       store_id,
       created_at,
       updated_at,
       is_active,
       disable_auto_group_change,
       created_in,
       prefix,
       firstname,
       middlename,
       lastname,
       suffix,
       dob,
       password_hash,
       rp_token,
       rp_token_created_at,
       default_billing,
       default_shipping,
       taxvat,
       confirmation,
       gender,
       failures_num,
       first_failure,
       lock_expires,
       m1_customer_id,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(entity_id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       entity_id,
       website_id,
       email,
       group_id,
       increment_id,
       store_id,
       created_at,
       updated_at,
       is_active,
       disable_auto_group_change,
       created_in,
       prefix,
       firstname,
       middlename,
       lastname,
       suffix,
       dob,
       password_hash,
       rp_token,
       rp_token_created_at,
       default_billing,
       default_shipping,
       taxvat,
       confirmation,
       gender,
       failures_num,
       first_failure,
       lock_expires,
       m1_customer_id,
       isnull(cast(stage_magento_customer_entity.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_magento_customer_entity
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_magento_customer_entity @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_magento_customer_entity (
       bk_hash,
       entity_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_magento_customer_entity.bk_hash,
       stage_hash_magento_customer_entity.entity_id entity_id,
       isnull(cast(stage_hash_magento_customer_entity.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       39,
       @insert_date_time,
       @user
  from stage_hash_magento_customer_entity
  left join h_magento_customer_entity
    on stage_hash_magento_customer_entity.bk_hash = h_magento_customer_entity.bk_hash
 where h_magento_customer_entity_id is null
   and stage_hash_magento_customer_entity.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_magento_customer_entity
if object_id('tempdb..#l_magento_customer_entity_inserts') is not null drop table #l_magento_customer_entity_inserts
create table #l_magento_customer_entity_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_magento_customer_entity.bk_hash,
       stage_hash_magento_customer_entity.entity_id entity_id,
       stage_hash_magento_customer_entity.website_id website_id,
       stage_hash_magento_customer_entity.store_id store_id,
       stage_hash_magento_customer_entity.group_id group_id,
       stage_hash_magento_customer_entity.increment_id increment_id,
       stage_hash_magento_customer_entity.m1_customer_id m1_customer_id,
       isnull(cast(stage_hash_magento_customer_entity.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_magento_customer_entity.entity_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_customer_entity.website_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_customer_entity.store_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_customer_entity.group_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_customer_entity.increment_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_customer_entity.m1_customer_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_magento_customer_entity
 where stage_hash_magento_customer_entity.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_magento_customer_entity records
set @insert_date_time = getdate()
insert into l_magento_customer_entity (
       bk_hash,
       entity_id,
       website_id,
       store_id,
       group_id,
       increment_id,
       m1_customer_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_magento_customer_entity_inserts.bk_hash,
       #l_magento_customer_entity_inserts.entity_id,
       #l_magento_customer_entity_inserts.website_id,
       #l_magento_customer_entity_inserts.store_id,
       #l_magento_customer_entity_inserts.group_id,
       #l_magento_customer_entity_inserts.increment_id,
       #l_magento_customer_entity_inserts.m1_customer_id,
       case when l_magento_customer_entity.l_magento_customer_entity_id is null then isnull(#l_magento_customer_entity_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       39,
       #l_magento_customer_entity_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_magento_customer_entity_inserts
  left join p_magento_customer_entity
    on #l_magento_customer_entity_inserts.bk_hash = p_magento_customer_entity.bk_hash
   and p_magento_customer_entity.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_magento_customer_entity
    on p_magento_customer_entity.bk_hash = l_magento_customer_entity.bk_hash
   and p_magento_customer_entity.l_magento_customer_entity_id = l_magento_customer_entity.l_magento_customer_entity_id
 where l_magento_customer_entity.l_magento_customer_entity_id is null
    or (l_magento_customer_entity.l_magento_customer_entity_id is not null
        and l_magento_customer_entity.dv_hash <> #l_magento_customer_entity_inserts.source_hash)

--calculate hash and lookup to current s_magento_customer_entity
if object_id('tempdb..#s_magento_customer_entity_inserts') is not null drop table #s_magento_customer_entity_inserts
create table #s_magento_customer_entity_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_magento_customer_entity.bk_hash,
       stage_hash_magento_customer_entity.entity_id entity_id,
       stage_hash_magento_customer_entity.email email,
       stage_hash_magento_customer_entity.created_at created_at,
       stage_hash_magento_customer_entity.updated_at updated_at,
       stage_hash_magento_customer_entity.is_active is_active,
       stage_hash_magento_customer_entity.disable_auto_group_change disable_auto_group_change,
       stage_hash_magento_customer_entity.created_in created_in,
       stage_hash_magento_customer_entity.prefix prefix,
       stage_hash_magento_customer_entity.firstname first_name,
       stage_hash_magento_customer_entity.middlename middle_name,
       stage_hash_magento_customer_entity.lastname last_name,
       stage_hash_magento_customer_entity.suffix suffix,
       stage_hash_magento_customer_entity.dob dob,
       stage_hash_magento_customer_entity.password_hash password_hash,
       stage_hash_magento_customer_entity.rp_token rp_token,
       stage_hash_magento_customer_entity.rp_token_created_at rp_token_created_at,
       stage_hash_magento_customer_entity.default_billing default_billing,
       stage_hash_magento_customer_entity.default_shipping default_shipping,
       stage_hash_magento_customer_entity.taxvat taxvat,
       stage_hash_magento_customer_entity.confirmation confirmation,
       stage_hash_magento_customer_entity.gender gender,
       stage_hash_magento_customer_entity.failures_num failures_num,
       stage_hash_magento_customer_entity.first_failure first_failure,
       stage_hash_magento_customer_entity.lock_expires lock_expires,
       isnull(cast(stage_hash_magento_customer_entity.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_magento_customer_entity.entity_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_customer_entity.email,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_magento_customer_entity.created_at,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_magento_customer_entity.updated_at,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_customer_entity.is_active as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_customer_entity.disable_auto_group_change as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_customer_entity.created_in,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_customer_entity.prefix,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_customer_entity.firstname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_customer_entity.middlename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_customer_entity.lastname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_customer_entity.suffix,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_customer_entity.dob as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_customer_entity.password_hash,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_customer_entity.rp_token,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_magento_customer_entity.rp_token_created_at,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_customer_entity.default_billing as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_customer_entity.default_shipping as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_customer_entity.taxvat,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_customer_entity.confirmation,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_customer_entity.gender as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_customer_entity.failures_num as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_magento_customer_entity.first_failure,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_magento_customer_entity.lock_expires,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_magento_customer_entity
 where stage_hash_magento_customer_entity.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_magento_customer_entity records
set @insert_date_time = getdate()
insert into s_magento_customer_entity (
       bk_hash,
       entity_id,
       email,
       created_at,
       updated_at,
       is_active,
       disable_auto_group_change,
       created_in,
       prefix,
       first_name,
       middle_name,
       last_name,
       suffix,
       dob,
       password_hash,
       rp_token,
       rp_token_created_at,
       default_billing,
       default_shipping,
       taxvat,
       confirmation,
       gender,
       failures_num,
       first_failure,
       lock_expires,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_magento_customer_entity_inserts.bk_hash,
       #s_magento_customer_entity_inserts.entity_id,
       #s_magento_customer_entity_inserts.email,
       #s_magento_customer_entity_inserts.created_at,
       #s_magento_customer_entity_inserts.updated_at,
       #s_magento_customer_entity_inserts.is_active,
       #s_magento_customer_entity_inserts.disable_auto_group_change,
       #s_magento_customer_entity_inserts.created_in,
       #s_magento_customer_entity_inserts.prefix,
       #s_magento_customer_entity_inserts.first_name,
       #s_magento_customer_entity_inserts.middle_name,
       #s_magento_customer_entity_inserts.last_name,
       #s_magento_customer_entity_inserts.suffix,
       #s_magento_customer_entity_inserts.dob,
       #s_magento_customer_entity_inserts.password_hash,
       #s_magento_customer_entity_inserts.rp_token,
       #s_magento_customer_entity_inserts.rp_token_created_at,
       #s_magento_customer_entity_inserts.default_billing,
       #s_magento_customer_entity_inserts.default_shipping,
       #s_magento_customer_entity_inserts.taxvat,
       #s_magento_customer_entity_inserts.confirmation,
       #s_magento_customer_entity_inserts.gender,
       #s_magento_customer_entity_inserts.failures_num,
       #s_magento_customer_entity_inserts.first_failure,
       #s_magento_customer_entity_inserts.lock_expires,
       case when s_magento_customer_entity.s_magento_customer_entity_id is null then isnull(#s_magento_customer_entity_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       39,
       #s_magento_customer_entity_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_magento_customer_entity_inserts
  left join p_magento_customer_entity
    on #s_magento_customer_entity_inserts.bk_hash = p_magento_customer_entity.bk_hash
   and p_magento_customer_entity.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_magento_customer_entity
    on p_magento_customer_entity.bk_hash = s_magento_customer_entity.bk_hash
   and p_magento_customer_entity.s_magento_customer_entity_id = s_magento_customer_entity.s_magento_customer_entity_id
 where s_magento_customer_entity.s_magento_customer_entity_id is null
    or (s_magento_customer_entity.s_magento_customer_entity_id is not null
        and s_magento_customer_entity.dv_hash <> #s_magento_customer_entity_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_magento_customer_entity @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_magento_customer_entity @current_dv_batch_id

end
