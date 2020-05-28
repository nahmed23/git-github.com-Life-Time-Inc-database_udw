CREATE PROC [dbo].[proc_dim_magento_customer] @dv_batch_id [varchar](500) AS
begin

set xact_abort on
set nocount on


declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-1)  from dim_magento_customer)
declare @current_dv_batch_id bigint = @dv_batch_id
declare @load_dv_batch_id bigint = case when @max_dv_batch_id < @current_dv_batch_id then @max_dv_batch_id else @current_dv_batch_id end

if object_id('tempdb..#d_magento_eav_attribute') is not null drop table #d_magento_eav_attribute
create table dbo.#d_magento_eav_attribute with (distribution = hash (bk_hash),location = user_db) as
select bk_hash,attribute_code
from d_magento_eav_attribute
where attribute_code in (
		'mms_party_id','mms_member_id','mms_club_id','mms_employee_id'
		/*
		,'email','first_name','middle_name','last_name','prefix','suffix','dob','is_active','gender','default_billing','default_shipping'
		,'group_id','store_id','created_at','updated_at','tax_vat','m1_customer_id','city','company','country_id','fax','post_code'
		,'prefix','region','region_id','street','suffix','telephone','vat_id','vat_is_valid','vat_request_date'	,'vat_request_id','vat_request_success'
		*/
		)

if object_id('tempdb..#etl_step_1') is not null drop table #etl_step_1
create table dbo.#etl_step_1 with (distribution = hash (customer_id),location = user_db) as
/* get the product data in the current batch*/
select d_magento_customer_entity.customer_id
	,d_magento_customer_entity.created_dim_date_key
	,d_magento_customer_entity.created_dim_time_key
	,d_magento_customer_entity.email
	,d_magento_customer_entity.first_name
	,d_magento_customer_entity.middle_name
	,d_magento_customer_entity.last_name
	,d_magento_customer_entity.prefix
	,d_magento_customer_entity.suffix
	,d_magento_customer_entity.dob
	,d_magento_customer_entity.dob_dim_date_key
	,d_magento_customer_entity.gender
	,d_magento_customer_entity.group_id
	,d_magento_customer_entity.store_id
	,d_magento_customer_entity.is_active_flag
	,d_magento_customer_entity.default_billing
	,d_magento_customer_entity.default_shipping
	,d_magento_customer_entity.m1_customer_id
	,d_magento_customer_entity.updated_dim_date_key
	,d_magento_customer_entity.updated_dim_time_key
	,d_magento_customer_entity.created_at
	,d_magento_customer_entity.updated_at
	,d_magento_customer_entity_varchar.d_magento_eav_attribute_bk_hash
	,d_magento_eav_attribute.attribute_code
	,d_magento_customer_entity_varchar.value
	,d_magento_customer_entity.dv_batch_id
	,d_magento_customer_entity.dv_load_date_time
from d_magento_customer_entity
join d_magento_customer_entity_varchar on d_magento_customer_entity.bk_hash = d_magento_customer_entity_varchar.d_magento_customer_entity_bk_hash
join #d_magento_eav_attribute d_magento_eav_attribute on d_magento_eav_attribute.bk_hash = d_magento_customer_entity_varchar.d_magento_eav_attribute_bk_hash
where d_magento_customer_entity.dv_batch_id >= @load_dv_batch_id


if object_id('tempdb..#etl_step_2') is not null drop table #etl_step_2
create table dbo.#etl_step_2 with (distribution = hash (dim_magento_customer_key),location = user_db) as
select case when #etl_step_1.customer_id is null then '-998'
       	when ltrim(rtrim(#etl_step_1.customer_id))='' then '-998'
          when isnumeric(#etl_step_1.customer_id)=0 then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(ltrim(rtrim(#etl_step_1.customer_id)) as int) as varchar(500)),'z#@$k%&P'))),2) end
	 dim_magento_customer_key
    ,customer_id
	,created_dim_date_key
	,created_dim_time_key
	,email
	,first_name
	,middle_name
	,last_name
	,prefix
	,suffix
	,dob
	,dob_dim_date_key
	,gender
	,group_id
	,store_id
	,is_active_flag
	,default_billing
	,default_shipping
	,m1_customer_id
	,updated_dim_date_key
	,updated_dim_time_key
	,created_at
	,updated_at
	,dv_batch_id
	,dv_load_date_time
	,max(case when attribute_code = 'mms_party_id' then value else null end) mms_party_id
	,max(case when attribute_code = 'mms_member_id' then value else null end) mms_member_id
	,max(case when attribute_code = 'mms_club_id' then value else null end) mms_club_id
	,max(case when attribute_code = 'mms_employee_id' then value else null end) mms_employee_id
from #etl_step_1
group by case when #etl_step_1.customer_id is null then '-998'
       	when ltrim(rtrim(#etl_step_1.customer_id))='' then '-998'
          when isnumeric(#etl_step_1.customer_id)=0 then '-998'
     else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(ltrim(rtrim(#etl_step_1.customer_id)) as int) as varchar(500)),'z#@$k%&P'))),2) end
    ,customer_id
	,created_dim_date_key
	,created_dim_time_key
	,email
	,first_name
	,middle_name
	,last_name
	,prefix
	,suffix
	,dob
	,dob_dim_date_key
	,gender
	,group_id
	,store_id
	,is_active_flag
	,default_billing
	,default_shipping
	,m1_customer_id
	,updated_dim_date_key
	,updated_dim_time_key
	,created_at
	,updated_at
	,dv_batch_id
	,dv_load_date_time


if object_id('tempdb..#etl_step_3') is not null drop table #etl_step_3
create table dbo.#etl_step_3 with (distribution = hash (dim_magento_customer_key),location = user_db) as
select  dim_magento_customer_key
    ,customer_id
	,created_dim_date_key
	,created_dim_time_key
	,email
	,first_name
	,middle_name
	,last_name
	,prefix
	,suffix
	,dob
	,dob_dim_date_key
	,gender
	,group_id
	,store_id
	,is_active_flag
	,default_billing
	,default_shipping
	,m1_customer_id
    ,mms_party_id
	,case when #etl_step_2.mms_member_id is null then '-998'
       	when ltrim(rtrim(#etl_step_2.mms_member_id))='' then '-998'
          when isnumeric(#etl_step_2.mms_member_id)=0 then '-998'
		   when charindex(',', #etl_step_2.mms_member_id) > 0 then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(ltrim(rtrim(#etl_step_2.mms_member_id)) as int) as varchar(500)),'z#@$k%&P'))),2) end dim_mms_member_key

	,case when #etl_step_2.mms_club_id is null then '-998'
       	when ltrim(rtrim(#etl_step_2.mms_club_id))='' then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(ltrim(rtrim(#etl_step_2.mms_club_id)) as int) as varchar(500)),'z#@$k%&P'))),2) end dim_club_key

	,case when #etl_step_2.mms_employee_id is null then '-998'
       	when ltrim(rtrim(#etl_step_2.mms_employee_id))='' then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(ltrim(rtrim(#etl_step_2.mms_employee_id)) as int) as varchar(500)),'z#@$k%&P'))),2) end dim_employee_key
	,updated_dim_date_key
	,updated_dim_time_key
	,created_at
	,updated_at
	,dv_batch_id
	,dv_load_date_time
from #etl_step_2

if object_id('tempdb..#etl_step_4') is not null drop table #etl_step_4
create table dbo.#etl_step_4 with (distribution = hash (dim_magento_customer_key),location = user_db) as
select  #etl_step_3.dim_magento_customer_key
    ,#etl_step_3.customer_id
	,#etl_step_3.created_dim_date_key
	,#etl_step_3.created_dim_time_key
	,#etl_step_3.email
	,#etl_step_3.first_name
	,#etl_step_3.middle_name
	,#etl_step_3.last_name
	,#etl_step_3.prefix
	,#etl_step_3.suffix
	,#etl_step_3.dob
	,#etl_step_3.dob_dim_date_key
	,#etl_step_3.gender
	,#etl_step_3.group_id
	,#etl_step_3.store_id
	,#etl_step_3.is_active_flag
	,#etl_step_3.default_billing
	,#etl_step_3.default_shipping
	,#etl_step_3.m1_customer_id
    ,#etl_step_3.mms_party_id
	,#etl_step_3.dim_mms_member_key
	,#etl_step_3.dim_club_key
	,#etl_step_3.dim_employee_key
	,d_magento_customer_address_entity.city
	,d_magento_customer_address_entity.company
	,d_magento_customer_address_entity.country_id
	,d_magento_customer_address_entity.fax
	,d_magento_customer_address_entity.post_code
	,d_magento_customer_address_entity.region
	,d_magento_customer_address_entity.region_id
	,d_magento_customer_address_entity.street
	,d_magento_customer_address_entity.telephone
	,#etl_step_3.updated_dim_date_key
	,#etl_step_3.updated_dim_time_key
	,#etl_step_3.created_at
	,#etl_step_3.updated_at
	,#etl_step_3.dv_batch_id
	,#etl_step_3.dv_load_date_time
from #etl_step_3
left join d_magento_customer_address_entity on #etl_step_3.dim_magento_customer_key=d_magento_customer_address_entity.parent_d_magento_customer_entity_bk_hash




/* delete and re-insert as a single transaction*/
/*   delete records from the table that exist*/
/*   insert records from records from current and missing batches*/

begin tran

  delete dbo.dim_magento_customer
   where dim_magento_customer_key in (select dim_magento_customer_key from #etl_step_3)

  insert into dim_magento_customer
        (
	 dim_magento_customer_key
	,customer_id
	,mms_party_id
	,dim_mms_member_key
	,dim_club_key
	,dim_employee_key
	,email
	,first_name
	,middle_name
	,last_name
	,prefix
	,suffix
	,dob
	,dob_dim_date_key
	,is_active_flag
	,gender
	,default_billing
	,default_shipping
	,m1_customer_id
	,group_id
	,store_id
	,created_at
	,updated_at
	,created_dim_date_key
	,created_dim_time_key
	,updated_dim_date_key
	,updated_dim_time_key
	,city
	,company
	,country_id
	,fax
	,post_code
	,region
	,region_id
	,street
	,telephone
	,dv_load_date_time
	,dv_load_end_date_time
	,dv_batch_id
	,dv_inserted_date_time
	,dv_insert_user
	)
  select
     dim_magento_customer_key
	,customer_id
	,mms_party_id
	,dim_mms_member_key
	,dim_club_key
	,dim_employee_key
	,email
	,first_name
	,middle_name
	,last_name
	,prefix
	,suffix
	,dob
	,dob_dim_date_key
	,is_active_flag
	,gender
	,default_billing
	,default_shipping
	,m1_customer_id
	,group_id
	,store_id
	,created_at
	,updated_at
	,created_dim_date_key
	,created_dim_time_key
	,updated_dim_date_key
	,updated_dim_time_key
	,city
	,company
	,country_id
	,fax
	,post_code
	,region
	,region_id
	,street
	,telephone
    ,dv_load_date_time
	,convert(datetime, '99991231', 112)
    ,dv_batch_id
    ,getdate()
    ,suser_sname()
  from #etl_step_4

commit tran

end

