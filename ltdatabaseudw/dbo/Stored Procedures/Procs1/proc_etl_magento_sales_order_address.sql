CREATE PROC [dbo].[proc_etl_magento_sales_order_address] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_magento_sales_order_address

set @insert_date_time = getdate()
insert into dbo.stage_hash_magento_sales_order_address (
       bk_hash,
       entity_id,
       parent_id,
       customer_address_id,
       quote_address_id,
       region_id,
       customer_id,
       fax,
       region,
       postcode,
       lastname,
       street,
       city,
       email,
       telephone,
       country_id,
       firstname,
       address_type,
       prefix,
       middlename,
       suffix,
       company,
       vat_id,
       vat_is_valid,
       vat_request_id,
       vat_request_date,
       vat_request_success,
       giftregistry_item_id,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(entity_id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       entity_id,
       parent_id,
       customer_address_id,
       quote_address_id,
       region_id,
       customer_id,
       fax,
       region,
       postcode,
       lastname,
       street,
       city,
       email,
       telephone,
       country_id,
       firstname,
       address_type,
       prefix,
       middlename,
       suffix,
       company,
       vat_id,
       vat_is_valid,
       vat_request_id,
       vat_request_date,
       vat_request_success,
       giftregistry_item_id,
       dummy_modified_date_time,
       isnull(cast(stage_magento_sales_order_address.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_magento_sales_order_address
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_magento_sales_order_address @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_magento_sales_order_address (
       bk_hash,
       entity_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_magento_sales_order_address.bk_hash,
       stage_hash_magento_sales_order_address.entity_id entity_id,
       isnull(cast(stage_hash_magento_sales_order_address.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       39,
       @insert_date_time,
       @user
  from stage_hash_magento_sales_order_address
  left join h_magento_sales_order_address
    on stage_hash_magento_sales_order_address.bk_hash = h_magento_sales_order_address.bk_hash
 where h_magento_sales_order_address_id is null
   and stage_hash_magento_sales_order_address.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_magento_sales_order_address
if object_id('tempdb..#l_magento_sales_order_address_inserts') is not null drop table #l_magento_sales_order_address_inserts
create table #l_magento_sales_order_address_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_magento_sales_order_address.bk_hash,
       stage_hash_magento_sales_order_address.entity_id entity_id,
       stage_hash_magento_sales_order_address.parent_id parent_id,
       stage_hash_magento_sales_order_address.customer_address_id customer_address_id,
       stage_hash_magento_sales_order_address.quote_address_id quote_address_id,
       stage_hash_magento_sales_order_address.region_id region_id,
       stage_hash_magento_sales_order_address.customer_id customer_id,
       stage_hash_magento_sales_order_address.giftregistry_item_id gift_registry_item_id,
       isnull(cast(stage_hash_magento_sales_order_address.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_address.entity_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_address.parent_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_address.customer_address_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_address.quote_address_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_address.region_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_address.customer_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_address.giftregistry_item_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_magento_sales_order_address
 where stage_hash_magento_sales_order_address.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_magento_sales_order_address records
set @insert_date_time = getdate()
insert into l_magento_sales_order_address (
       bk_hash,
       entity_id,
       parent_id,
       customer_address_id,
       quote_address_id,
       region_id,
       customer_id,
       gift_registry_item_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_magento_sales_order_address_inserts.bk_hash,
       #l_magento_sales_order_address_inserts.entity_id,
       #l_magento_sales_order_address_inserts.parent_id,
       #l_magento_sales_order_address_inserts.customer_address_id,
       #l_magento_sales_order_address_inserts.quote_address_id,
       #l_magento_sales_order_address_inserts.region_id,
       #l_magento_sales_order_address_inserts.customer_id,
       #l_magento_sales_order_address_inserts.gift_registry_item_id,
       case when l_magento_sales_order_address.l_magento_sales_order_address_id is null then isnull(#l_magento_sales_order_address_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       39,
       #l_magento_sales_order_address_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_magento_sales_order_address_inserts
  left join p_magento_sales_order_address
    on #l_magento_sales_order_address_inserts.bk_hash = p_magento_sales_order_address.bk_hash
   and p_magento_sales_order_address.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_magento_sales_order_address
    on p_magento_sales_order_address.bk_hash = l_magento_sales_order_address.bk_hash
   and p_magento_sales_order_address.l_magento_sales_order_address_id = l_magento_sales_order_address.l_magento_sales_order_address_id
 where l_magento_sales_order_address.l_magento_sales_order_address_id is null
    or (l_magento_sales_order_address.l_magento_sales_order_address_id is not null
        and l_magento_sales_order_address.dv_hash <> #l_magento_sales_order_address_inserts.source_hash)

--calculate hash and lookup to current s_magento_sales_order_address
if object_id('tempdb..#s_magento_sales_order_address_inserts') is not null drop table #s_magento_sales_order_address_inserts
create table #s_magento_sales_order_address_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_magento_sales_order_address.bk_hash,
       stage_hash_magento_sales_order_address.entity_id entity_id,
       stage_hash_magento_sales_order_address.fax fax,
       stage_hash_magento_sales_order_address.region region,
       stage_hash_magento_sales_order_address.postcode post_code,
       stage_hash_magento_sales_order_address.lastname last_name,
       stage_hash_magento_sales_order_address.street street,
       stage_hash_magento_sales_order_address.city city,
       stage_hash_magento_sales_order_address.email email,
       stage_hash_magento_sales_order_address.telephone telephone,
       stage_hash_magento_sales_order_address.country_id country_id,
       stage_hash_magento_sales_order_address.firstname first_name,
       stage_hash_magento_sales_order_address.address_type address_type,
       stage_hash_magento_sales_order_address.prefix prefix,
       stage_hash_magento_sales_order_address.middlename middle_name,
       stage_hash_magento_sales_order_address.suffix suffix,
       stage_hash_magento_sales_order_address.company company,
       stage_hash_magento_sales_order_address.vat_id vat_id,
       stage_hash_magento_sales_order_address.vat_is_valid vat_is_valid,
       stage_hash_magento_sales_order_address.vat_request_id vat_request_id,
       stage_hash_magento_sales_order_address.vat_request_date vat_request_date,
       stage_hash_magento_sales_order_address.vat_request_success vat_request_success,
       stage_hash_magento_sales_order_address.dummy_modified_date_time dummy_modified_date_time,
       isnull(cast(stage_hash_magento_sales_order_address.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_address.entity_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order_address.fax,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order_address.region,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order_address.postcode,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order_address.lastname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order_address.street,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order_address.city,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order_address.email,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order_address.telephone,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order_address.country_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order_address.firstname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order_address.address_type,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order_address.prefix,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order_address.middlename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order_address.suffix,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order_address.company,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order_address.vat_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_address.vat_is_valid as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order_address.vat_request_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order_address.vat_request_date,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_address.vat_request_success as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_magento_sales_order_address.dummy_modified_date_time,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_magento_sales_order_address
 where stage_hash_magento_sales_order_address.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_magento_sales_order_address records
set @insert_date_time = getdate()
insert into s_magento_sales_order_address (
       bk_hash,
       entity_id,
       fax,
       region,
       post_code,
       last_name,
       street,
       city,
       email,
       telephone,
       country_id,
       first_name,
       address_type,
       prefix,
       middle_name,
       suffix,
       company,
       vat_id,
       vat_is_valid,
       vat_request_id,
       vat_request_date,
       vat_request_success,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_magento_sales_order_address_inserts.bk_hash,
       #s_magento_sales_order_address_inserts.entity_id,
       #s_magento_sales_order_address_inserts.fax,
       #s_magento_sales_order_address_inserts.region,
       #s_magento_sales_order_address_inserts.post_code,
       #s_magento_sales_order_address_inserts.last_name,
       #s_magento_sales_order_address_inserts.street,
       #s_magento_sales_order_address_inserts.city,
       #s_magento_sales_order_address_inserts.email,
       #s_magento_sales_order_address_inserts.telephone,
       #s_magento_sales_order_address_inserts.country_id,
       #s_magento_sales_order_address_inserts.first_name,
       #s_magento_sales_order_address_inserts.address_type,
       #s_magento_sales_order_address_inserts.prefix,
       #s_magento_sales_order_address_inserts.middle_name,
       #s_magento_sales_order_address_inserts.suffix,
       #s_magento_sales_order_address_inserts.company,
       #s_magento_sales_order_address_inserts.vat_id,
       #s_magento_sales_order_address_inserts.vat_is_valid,
       #s_magento_sales_order_address_inserts.vat_request_id,
       #s_magento_sales_order_address_inserts.vat_request_date,
       #s_magento_sales_order_address_inserts.vat_request_success,
       #s_magento_sales_order_address_inserts.dummy_modified_date_time,
       case when s_magento_sales_order_address.s_magento_sales_order_address_id is null then isnull(#s_magento_sales_order_address_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       39,
       #s_magento_sales_order_address_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_magento_sales_order_address_inserts
  left join p_magento_sales_order_address
    on #s_magento_sales_order_address_inserts.bk_hash = p_magento_sales_order_address.bk_hash
   and p_magento_sales_order_address.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_magento_sales_order_address
    on p_magento_sales_order_address.bk_hash = s_magento_sales_order_address.bk_hash
   and p_magento_sales_order_address.s_magento_sales_order_address_id = s_magento_sales_order_address.s_magento_sales_order_address_id
 where s_magento_sales_order_address.s_magento_sales_order_address_id is null
    or (s_magento_sales_order_address.s_magento_sales_order_address_id is not null
        and s_magento_sales_order_address.dv_hash <> #s_magento_sales_order_address_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_magento_sales_order_address @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_magento_sales_order_address @current_dv_batch_id

end
