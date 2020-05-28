CREATE PROC [dbo].[proc_etl_magento_customer_address_entity] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_magento_customer_address_entity

set @insert_date_time = getdate()
insert into dbo.stage_hash_magento_customer_address_entity (
       bk_hash,
       entity_id,
       increment_id,
       parent_id,
       created_at,
       updated_at,
       is_active,
       city,
       company,
       country_id,
       fax,
       firstname,
       lastname,
       middlename,
       postcode,
       prefix,
       region,
       region_id,
       street,
       suffix,
       telephone,
       vat_id,
       vat_is_valid,
       vat_request_date,
       vat_request_id,
       vat_request_success,
       m1_customer_address_id,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(entity_id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       entity_id,
       increment_id,
       parent_id,
       created_at,
       updated_at,
       is_active,
       city,
       company,
       country_id,
       fax,
       firstname,
       lastname,
       middlename,
       postcode,
       prefix,
       region,
       region_id,
       street,
       suffix,
       telephone,
       vat_id,
       vat_is_valid,
       vat_request_date,
       vat_request_id,
       vat_request_success,
       m1_customer_address_id,
       isnull(cast(stage_magento_customer_address_entity.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_magento_customer_address_entity
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_magento_customer_address_entity @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_magento_customer_address_entity (
       bk_hash,
       entity_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_magento_customer_address_entity.bk_hash,
       stage_hash_magento_customer_address_entity.entity_id entity_id,
       isnull(cast(stage_hash_magento_customer_address_entity.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       39,
       @insert_date_time,
       @user
  from stage_hash_magento_customer_address_entity
  left join h_magento_customer_address_entity
    on stage_hash_magento_customer_address_entity.bk_hash = h_magento_customer_address_entity.bk_hash
 where h_magento_customer_address_entity_id is null
   and stage_hash_magento_customer_address_entity.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_magento_customer_address_entity
if object_id('tempdb..#l_magento_customer_address_entity_inserts') is not null drop table #l_magento_customer_address_entity_inserts
create table #l_magento_customer_address_entity_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_magento_customer_address_entity.bk_hash,
       stage_hash_magento_customer_address_entity.entity_id entity_id,
       stage_hash_magento_customer_address_entity.parent_id parent_id,
       stage_hash_magento_customer_address_entity.region_id region_id,
       stage_hash_magento_customer_address_entity.m1_customer_address_id m1_customer_address_id,
       isnull(cast(stage_hash_magento_customer_address_entity.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_magento_customer_address_entity.entity_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_customer_address_entity.parent_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_customer_address_entity.region_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_customer_address_entity.m1_customer_address_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_magento_customer_address_entity
 where stage_hash_magento_customer_address_entity.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_magento_customer_address_entity records
set @insert_date_time = getdate()
insert into l_magento_customer_address_entity (
       bk_hash,
       entity_id,
       parent_id,
       region_id,
       m1_customer_address_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_magento_customer_address_entity_inserts.bk_hash,
       #l_magento_customer_address_entity_inserts.entity_id,
       #l_magento_customer_address_entity_inserts.parent_id,
       #l_magento_customer_address_entity_inserts.region_id,
       #l_magento_customer_address_entity_inserts.m1_customer_address_id,
       case when l_magento_customer_address_entity.l_magento_customer_address_entity_id is null then isnull(#l_magento_customer_address_entity_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       39,
       #l_magento_customer_address_entity_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_magento_customer_address_entity_inserts
  left join p_magento_customer_address_entity
    on #l_magento_customer_address_entity_inserts.bk_hash = p_magento_customer_address_entity.bk_hash
   and p_magento_customer_address_entity.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_magento_customer_address_entity
    on p_magento_customer_address_entity.bk_hash = l_magento_customer_address_entity.bk_hash
   and p_magento_customer_address_entity.l_magento_customer_address_entity_id = l_magento_customer_address_entity.l_magento_customer_address_entity_id
 where l_magento_customer_address_entity.l_magento_customer_address_entity_id is null
    or (l_magento_customer_address_entity.l_magento_customer_address_entity_id is not null
        and l_magento_customer_address_entity.dv_hash <> #l_magento_customer_address_entity_inserts.source_hash)

--calculate hash and lookup to current s_magento_customer_address_entity
if object_id('tempdb..#s_magento_customer_address_entity_inserts') is not null drop table #s_magento_customer_address_entity_inserts
create table #s_magento_customer_address_entity_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_magento_customer_address_entity.bk_hash,
       stage_hash_magento_customer_address_entity.entity_id entity_id,
       stage_hash_magento_customer_address_entity.increment_id increment_id,
       stage_hash_magento_customer_address_entity.created_at created_at,
       stage_hash_magento_customer_address_entity.updated_at updated_at,
       stage_hash_magento_customer_address_entity.is_active is_active,
       stage_hash_magento_customer_address_entity.city city,
       stage_hash_magento_customer_address_entity.company company,
       stage_hash_magento_customer_address_entity.country_id country_id,
       stage_hash_magento_customer_address_entity.fax fax,
       stage_hash_magento_customer_address_entity.firstname first_name,
       stage_hash_magento_customer_address_entity.lastname last_name,
       stage_hash_magento_customer_address_entity.middlename middle_name,
       stage_hash_magento_customer_address_entity.postcode post_code,
       stage_hash_magento_customer_address_entity.prefix prefix,
       stage_hash_magento_customer_address_entity.region region,
       stage_hash_magento_customer_address_entity.street street,
       stage_hash_magento_customer_address_entity.suffix suffix,
       stage_hash_magento_customer_address_entity.telephone telephone,
       stage_hash_magento_customer_address_entity.vat_id vat_id,
       stage_hash_magento_customer_address_entity.vat_is_valid vat_is_valid,
       stage_hash_magento_customer_address_entity.vat_request_date vat_request_date,
       stage_hash_magento_customer_address_entity.vat_request_id vat_request_id,
       stage_hash_magento_customer_address_entity.vat_request_success vat_request_success,
       isnull(cast(stage_hash_magento_customer_address_entity.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_magento_customer_address_entity.entity_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_customer_address_entity.increment_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_magento_customer_address_entity.created_at,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_magento_customer_address_entity.updated_at,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_customer_address_entity.is_active as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_customer_address_entity.city,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_customer_address_entity.company,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_customer_address_entity.country_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_customer_address_entity.fax,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_customer_address_entity.firstname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_customer_address_entity.lastname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_customer_address_entity.middlename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_customer_address_entity.postcode,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_customer_address_entity.prefix,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_customer_address_entity.region,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_customer_address_entity.street,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_customer_address_entity.suffix,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_customer_address_entity.telephone,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_customer_address_entity.vat_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_customer_address_entity.vat_is_valid as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_customer_address_entity.vat_request_date,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_customer_address_entity.vat_request_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_customer_address_entity.vat_request_success as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_magento_customer_address_entity
 where stage_hash_magento_customer_address_entity.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_magento_customer_address_entity records
set @insert_date_time = getdate()
insert into s_magento_customer_address_entity (
       bk_hash,
       entity_id,
       increment_id,
       created_at,
       updated_at,
       is_active,
       city,
       company,
       country_id,
       fax,
       first_name,
       last_name,
       middle_name,
       post_code,
       prefix,
       region,
       street,
       suffix,
       telephone,
       vat_id,
       vat_is_valid,
       vat_request_date,
       vat_request_id,
       vat_request_success,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_magento_customer_address_entity_inserts.bk_hash,
       #s_magento_customer_address_entity_inserts.entity_id,
       #s_magento_customer_address_entity_inserts.increment_id,
       #s_magento_customer_address_entity_inserts.created_at,
       #s_magento_customer_address_entity_inserts.updated_at,
       #s_magento_customer_address_entity_inserts.is_active,
       #s_magento_customer_address_entity_inserts.city,
       #s_magento_customer_address_entity_inserts.company,
       #s_magento_customer_address_entity_inserts.country_id,
       #s_magento_customer_address_entity_inserts.fax,
       #s_magento_customer_address_entity_inserts.first_name,
       #s_magento_customer_address_entity_inserts.last_name,
       #s_magento_customer_address_entity_inserts.middle_name,
       #s_magento_customer_address_entity_inserts.post_code,
       #s_magento_customer_address_entity_inserts.prefix,
       #s_magento_customer_address_entity_inserts.region,
       #s_magento_customer_address_entity_inserts.street,
       #s_magento_customer_address_entity_inserts.suffix,
       #s_magento_customer_address_entity_inserts.telephone,
       #s_magento_customer_address_entity_inserts.vat_id,
       #s_magento_customer_address_entity_inserts.vat_is_valid,
       #s_magento_customer_address_entity_inserts.vat_request_date,
       #s_magento_customer_address_entity_inserts.vat_request_id,
       #s_magento_customer_address_entity_inserts.vat_request_success,
       case when s_magento_customer_address_entity.s_magento_customer_address_entity_id is null then isnull(#s_magento_customer_address_entity_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       39,
       #s_magento_customer_address_entity_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_magento_customer_address_entity_inserts
  left join p_magento_customer_address_entity
    on #s_magento_customer_address_entity_inserts.bk_hash = p_magento_customer_address_entity.bk_hash
   and p_magento_customer_address_entity.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_magento_customer_address_entity
    on p_magento_customer_address_entity.bk_hash = s_magento_customer_address_entity.bk_hash
   and p_magento_customer_address_entity.s_magento_customer_address_entity_id = s_magento_customer_address_entity.s_magento_customer_address_entity_id
 where s_magento_customer_address_entity.s_magento_customer_address_entity_id is null
    or (s_magento_customer_address_entity.s_magento_customer_address_entity_id is not null
        and s_magento_customer_address_entity.dv_hash <> #s_magento_customer_address_entity_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_magento_customer_address_entity @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_magento_customer_address_entity @current_dv_batch_id

end
