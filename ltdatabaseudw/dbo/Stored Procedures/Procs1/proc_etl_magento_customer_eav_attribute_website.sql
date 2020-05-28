CREATE PROC [dbo].[proc_etl_magento_customer_eav_attribute_website] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_magento_customer_eav_attribute_website

set @insert_date_time = getdate()
insert into dbo.stage_hash_magento_customer_eav_attribute_website (
       bk_hash,
       attribute_id,
       website_id,
       is_visible,
       is_required,
       default_value,
       multiline_count,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(attribute_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(website_id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       attribute_id,
       website_id,
       is_visible,
       is_required,
       default_value,
       multiline_count,
       dummy_modified_date_time,
       isnull(cast(stage_magento_customer_eav_attribute_website.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_magento_customer_eav_attribute_website
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_magento_customer_eav_attribute_website @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_magento_customer_eav_attribute_website (
       bk_hash,
       attribute_id,
       website_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_magento_customer_eav_attribute_website.bk_hash,
       stage_hash_magento_customer_eav_attribute_website.attribute_id attribute_id,
       stage_hash_magento_customer_eav_attribute_website.website_id website_id,
       isnull(cast(stage_hash_magento_customer_eav_attribute_website.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       39,
       @insert_date_time,
       @user
  from stage_hash_magento_customer_eav_attribute_website
  left join h_magento_customer_eav_attribute_website
    on stage_hash_magento_customer_eav_attribute_website.bk_hash = h_magento_customer_eav_attribute_website.bk_hash
 where h_magento_customer_eav_attribute_website_id is null
   and stage_hash_magento_customer_eav_attribute_website.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current s_magento_customer_eav_attribute_website
if object_id('tempdb..#s_magento_customer_eav_attribute_website_inserts') is not null drop table #s_magento_customer_eav_attribute_website_inserts
create table #s_magento_customer_eav_attribute_website_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_magento_customer_eav_attribute_website.bk_hash,
       stage_hash_magento_customer_eav_attribute_website.attribute_id attribute_id,
       stage_hash_magento_customer_eav_attribute_website.website_id website_id,
       stage_hash_magento_customer_eav_attribute_website.is_visible is_visible,
       stage_hash_magento_customer_eav_attribute_website.is_required is_required,
       stage_hash_magento_customer_eav_attribute_website.default_value default_value,
       stage_hash_magento_customer_eav_attribute_website.multiline_count multiline_count,
       stage_hash_magento_customer_eav_attribute_website.dummy_modified_date_time dummy_modified_date_time,
       isnull(cast(stage_hash_magento_customer_eav_attribute_website.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_magento_customer_eav_attribute_website.attribute_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_customer_eav_attribute_website.website_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_customer_eav_attribute_website.is_visible as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_customer_eav_attribute_website.is_required as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_customer_eav_attribute_website.default_value,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_customer_eav_attribute_website.multiline_count as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_magento_customer_eav_attribute_website.dummy_modified_date_time,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_magento_customer_eav_attribute_website
 where stage_hash_magento_customer_eav_attribute_website.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_magento_customer_eav_attribute_website records
set @insert_date_time = getdate()
insert into s_magento_customer_eav_attribute_website (
       bk_hash,
       attribute_id,
       website_id,
       is_visible,
       is_required,
       default_value,
       multiline_count,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_magento_customer_eav_attribute_website_inserts.bk_hash,
       #s_magento_customer_eav_attribute_website_inserts.attribute_id,
       #s_magento_customer_eav_attribute_website_inserts.website_id,
       #s_magento_customer_eav_attribute_website_inserts.is_visible,
       #s_magento_customer_eav_attribute_website_inserts.is_required,
       #s_magento_customer_eav_attribute_website_inserts.default_value,
       #s_magento_customer_eav_attribute_website_inserts.multiline_count,
       #s_magento_customer_eav_attribute_website_inserts.dummy_modified_date_time,
       case when s_magento_customer_eav_attribute_website.s_magento_customer_eav_attribute_website_id is null then isnull(#s_magento_customer_eav_attribute_website_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       39,
       #s_magento_customer_eav_attribute_website_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_magento_customer_eav_attribute_website_inserts
  left join p_magento_customer_eav_attribute_website
    on #s_magento_customer_eav_attribute_website_inserts.bk_hash = p_magento_customer_eav_attribute_website.bk_hash
   and p_magento_customer_eav_attribute_website.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_magento_customer_eav_attribute_website
    on p_magento_customer_eav_attribute_website.bk_hash = s_magento_customer_eav_attribute_website.bk_hash
   and p_magento_customer_eav_attribute_website.s_magento_customer_eav_attribute_website_id = s_magento_customer_eav_attribute_website.s_magento_customer_eav_attribute_website_id
 where s_magento_customer_eav_attribute_website.s_magento_customer_eav_attribute_website_id is null
    or (s_magento_customer_eav_attribute_website.s_magento_customer_eav_attribute_website_id is not null
        and s_magento_customer_eav_attribute_website.dv_hash <> #s_magento_customer_eav_attribute_website_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_magento_customer_eav_attribute_website @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_magento_customer_eav_attribute_website @current_dv_batch_id

end
