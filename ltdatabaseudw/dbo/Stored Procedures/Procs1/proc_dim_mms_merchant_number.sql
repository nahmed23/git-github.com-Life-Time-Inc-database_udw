CREATE PROC [dbo].[proc_dim_mms_merchant_number] @dv_batch_id [varchar](500) AS
begin

set xact_abort on
set nocount on

/* main query*/
if object_id('tempdb..#etl_step_1') is not null drop table #etl_step_1
create table dbo.#etl_step_1 with(distribution=hash(bk_hash), location=user_db) as
select
d_mms_club_merchant_number.bk_hash,
d_mms_club_merchant_number.club_merchant_number_id,
d_mms_club_merchant_number.dim_club_key,
d_mms_club_merchant_number.val_business_area_id,
d_mms_club_merchant_number.merchant_number,
d_mms_club_merchant_number.merchant_location_number,
d_mms_club_merchant_number.auto_reconcile_flag,
r_mms_val_currency_code.currency_code,
dim_description.dim_description_key,
dim_club.club_id,
d_mms_club_merchant_number.dv_load_date_time,
d_mms_club_merchant_number.dv_load_end_date_time,
d_mms_club_merchant_number.dv_batch_id,
d_mms_club_merchant_number.dv_inserted_date_time,
d_mms_club_merchant_number.dv_insert_user
from
d_mms_club_merchant_number d_mms_club_merchant_number
Join r_mms_val_currency_code r_mms_val_currency_code
on r_mms_val_currency_code.val_currency_code_id = d_mms_club_merchant_number.val_currency_code_id
left join dim_description dim_description
on dim_description.dim_description_id = d_mms_club_merchant_number.val_business_area_id
left join dim_club dim_club
on dim_club.dim_club_key = d_mms_club_merchant_number.dim_club_key
where
d_mms_club_merchant_number.dv_batch_id >= @dv_batch_id


begin tran

  delete dbo.dim_mms_merchant_number

  insert into dim_mms_merchant_number
  (
  dim_mms_merchant_number_key,
  club_merchant_number_id,
  dim_club_key,
  merchant_number,
  merchant_location_number,
  auto_reconcile_flag,
  club_id,
  val_business_area_id,
  currency_code,
  business_area_dim_description_key,
   dv_load_date_time,
   dv_load_end_date_time,
   dv_batch_id,
   dv_inserted_date_time,
   dv_insert_user
  )
  select  
  bk_hash,
  club_merchant_number_id,
  dim_club_key,
  merchant_number,
  merchant_location_number,
  auto_reconcile_flag,
  club_id,
  val_business_area_id,
  currency_code,
  dim_description_key,
     dv_load_date_time,
   dv_load_end_date_time,
   dv_batch_id,
   dv_inserted_date_time,
   dv_insert_user
  from #etl_step_1

commit tran

end
