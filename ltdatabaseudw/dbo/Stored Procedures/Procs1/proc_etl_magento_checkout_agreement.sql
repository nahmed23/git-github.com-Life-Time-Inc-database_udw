CREATE PROC [dbo].[proc_etl_magento_checkout_agreement] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_magento_checkout_agreement

set @insert_date_time = getdate()
insert into dbo.stage_hash_magento_checkout_agreement (
       bk_hash,
       agreement_id,
       name,
       content,
       content_height,
       checkbox_text,
       is_active,
       is_html,
       mode,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(agreement_id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       agreement_id,
       name,
       content,
       content_height,
       checkbox_text,
       is_active,
       is_html,
       mode,
       dummy_modified_date_time,
       isnull(cast(stage_magento_checkout_agreement.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_magento_checkout_agreement
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_magento_checkout_agreement @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_magento_checkout_agreement (
       bk_hash,
       agreement_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_magento_checkout_agreement.bk_hash,
       stage_hash_magento_checkout_agreement.agreement_id agreement_id,
       isnull(cast(stage_hash_magento_checkout_agreement.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       39,
       @insert_date_time,
       @user
  from stage_hash_magento_checkout_agreement
  left join h_magento_checkout_agreement
    on stage_hash_magento_checkout_agreement.bk_hash = h_magento_checkout_agreement.bk_hash
 where h_magento_checkout_agreement_id is null
   and stage_hash_magento_checkout_agreement.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current s_magento_checkout_agreement
if object_id('tempdb..#s_magento_checkout_agreement_inserts') is not null drop table #s_magento_checkout_agreement_inserts
create table #s_magento_checkout_agreement_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_magento_checkout_agreement.bk_hash,
       stage_hash_magento_checkout_agreement.agreement_id agreement_id,
       stage_hash_magento_checkout_agreement.name name,
       stage_hash_magento_checkout_agreement.content content,
       stage_hash_magento_checkout_agreement.content_height content_height,
       stage_hash_magento_checkout_agreement.checkbox_text checkbox_text,
       stage_hash_magento_checkout_agreement.is_active is_active,
       stage_hash_magento_checkout_agreement.is_html is_html,
       stage_hash_magento_checkout_agreement.mode mode,
       stage_hash_magento_checkout_agreement.dummy_modified_date_time dummy_modified_date_time,
       isnull(cast(stage_hash_magento_checkout_agreement.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_magento_checkout_agreement.agreement_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_checkout_agreement.name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_checkout_agreement.content,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_checkout_agreement.content_height,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_checkout_agreement.checkbox_text,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_checkout_agreement.is_active as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_checkout_agreement.is_html as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_checkout_agreement.mode as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_magento_checkout_agreement
 where stage_hash_magento_checkout_agreement.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_magento_checkout_agreement records
set @insert_date_time = getdate()
insert into s_magento_checkout_agreement (
       bk_hash,
       agreement_id,
       name,
       content,
       content_height,
       checkbox_text,
       is_active,
       is_html,
       mode,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_magento_checkout_agreement_inserts.bk_hash,
       #s_magento_checkout_agreement_inserts.agreement_id,
       #s_magento_checkout_agreement_inserts.name,
       #s_magento_checkout_agreement_inserts.content,
       #s_magento_checkout_agreement_inserts.content_height,
       #s_magento_checkout_agreement_inserts.checkbox_text,
       #s_magento_checkout_agreement_inserts.is_active,
       #s_magento_checkout_agreement_inserts.is_html,
       #s_magento_checkout_agreement_inserts.mode,
       #s_magento_checkout_agreement_inserts.dummy_modified_date_time,
       case when s_magento_checkout_agreement.s_magento_checkout_agreement_id is null then isnull(#s_magento_checkout_agreement_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       39,
       #s_magento_checkout_agreement_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_magento_checkout_agreement_inserts
  left join p_magento_checkout_agreement
    on #s_magento_checkout_agreement_inserts.bk_hash = p_magento_checkout_agreement.bk_hash
   and p_magento_checkout_agreement.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_magento_checkout_agreement
    on p_magento_checkout_agreement.bk_hash = s_magento_checkout_agreement.bk_hash
   and p_magento_checkout_agreement.s_magento_checkout_agreement_id = s_magento_checkout_agreement.s_magento_checkout_agreement_id
 where s_magento_checkout_agreement.s_magento_checkout_agreement_id is null
    or (s_magento_checkout_agreement.s_magento_checkout_agreement_id is not null
        and s_magento_checkout_agreement.dv_hash <> #s_magento_checkout_agreement_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_magento_checkout_agreement @current_dv_batch_id

end
