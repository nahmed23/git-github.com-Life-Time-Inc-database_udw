CREATE PROC [dbo].[proc_etl_mms_bundle_sub_product] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @start int, @end int, @task_description varchar(500), @row_count int,   @user varchar(50), @start_c_id bigint , @c int

set @user = suser_sname()

--Run PIT proc for retry logic
exec dbo.proc_p_mms_bundle_sub_product @current_dv_batch_id

if object_id('tempdb..#incrementals') is not null drop table #incrementals
create table #incrementals with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as
select stage_mms_BundleSubProduct_id source_table_id,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(BundleSubProductID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       dv_batch_id
  from dbo.stage_mms_BundleSubProduct
 where (BundleSubProductID is not null)
   and dv_batch_id = @current_dv_batch_id

--Find new hub business keys
if object_id('tempdb..#h_mms_bundle_sub_product_insert_stage_mms_BundleSubProduct') is not null drop table #h_mms_bundle_sub_product_insert_stage_mms_BundleSubProduct
create table #h_mms_bundle_sub_product_insert_stage_mms_BundleSubProduct with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as
select #incrementals.bk_hash,
       stage_mms_BundleSubProduct.BundleSubProductID bundle_sub_product_id,
       isnull(stage_mms_BundleSubProduct.InsertedDateTime,'Jan 1, 1980') dv_load_date_time,
       h_mms_bundle_sub_product.h_mms_bundle_sub_product_id,
       row_number() over (order by #incrementals.bk_hash) r
  from dbo.stage_mms_BundleSubProduct
  join #incrementals
    on stage_mms_BundleSubProduct.stage_mms_BundleSubProduct_id = #incrementals.source_table_id
   and stage_mms_BundleSubProduct.dv_batch_id = #incrementals.dv_batch_id
  left join h_mms_bundle_sub_product
    on #incrementals.bk_hash = h_mms_bundle_sub_product.bk_hash

--Insert/update new hub business keys
set @start = 1
set @end = (select max(r) from #h_mms_bundle_sub_product_insert_stage_mms_BundleSubProduct)

while @start <= @end
begin

insert into h_mms_bundle_sub_product (
       bk_hash,
       bundle_sub_product_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select bk_hash,
       bundle_sub_product_id,
       dv_load_date_time,
       @current_dv_batch_id,
       2,
       getdate(),
       @user
  from #h_mms_bundle_sub_product_insert_stage_mms_BundleSubProduct
 where h_mms_bundle_sub_product_id is null
   and r >= @start
   and r < @start + 1000000

set @start = @start + 1000000
end

--Get PIT data for records that already exist
if object_id('tempdb..#p_mms_bundle_sub_product_current') is not null drop table #p_mms_bundle_sub_product_current
create table #p_mms_bundle_sub_product_current with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as
select p_mms_bundle_sub_product.bk_hash,
       p_mms_bundle_sub_product.p_mms_bundle_sub_product_id,
       p_mms_bundle_sub_product.bundle_sub_product_id,
       p_mms_bundle_sub_product.l_mms_bundle_sub_product_id,
       p_mms_bundle_sub_product.s_mms_bundle_sub_product_id,
       p_mms_bundle_sub_product.dv_load_end_date_time
  from dbo.p_mms_bundle_sub_product
  join (select distinct bk_hash from #incrementals) inc
    on p_mms_bundle_sub_product.bk_hash = inc.bk_hash
 where p_mms_bundle_sub_product.dv_load_end_date_time = convert(datetime,'dec 31, 9999',120)

--Get l_mms_bundle_sub_product current hash
if object_id('tempdb..#l_mms_bundle_sub_product_current') is not null drop table #l_mms_bundle_sub_product_current
create table #l_mms_bundle_sub_product_current with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as
select l_mms_bundle_sub_product.l_mms_bundle_sub_product_id,
       l_mms_bundle_sub_product.bk_hash,
       l_mms_bundle_sub_product.dv_hash
  from dbo.l_mms_bundle_sub_product
  join #p_mms_bundle_sub_product_current
    on l_mms_bundle_sub_product.l_mms_bundle_sub_product_id = #p_mms_bundle_sub_product_current.l_mms_bundle_sub_product_id
   and l_mms_bundle_sub_product.bk_hash = #p_mms_bundle_sub_product_current.bk_hash

--calculate hash and lookup to current l_mms_bundle_sub_product
if object_id('tempdb..#l_mms_bundle_sub_product_inserts') is not null drop table #l_mms_bundle_sub_product_inserts
create table #l_mms_bundle_sub_product_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select #incrementals.bk_hash,
       stage_mms_BundleSubProduct.BundleSubProductID bundle_sub_product_id,
       stage_mms_BundleSubProduct.BundleProductID bundle_product_id,
       stage_mms_BundleSubProduct.SubProductID sub_product_id,
       stage_mms_BundleSubProduct.GLOverRideClubID gl_over_ride_club_id,
       stage_mms_BundleSubProduct.ValGLGroupID val_gl_group_id,
       stage_mms_BundleSubProduct.InsertedDateTime dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_mms_BundleSubProduct.BundleSubProductID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_mms_BundleSubProduct.BundleProductID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_mms_BundleSubProduct.SubProductID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_mms_BundleSubProduct.GLOverRideClubID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_mms_BundleSubProduct.ValGLGroupID as varchar(500)),'z#@$k%&P'))),2) source_hash,
       #l_mms_bundle_sub_product_current.l_mms_bundle_sub_product_id,
       #l_mms_bundle_sub_product_current.dv_hash,
       row_number() over (order by #incrementals.bk_hash) r
  from dbo.stage_mms_BundleSubProduct
  join #incrementals
    on stage_mms_BundleSubProduct.stage_mms_BundleSubProduct_id = #incrementals.source_table_id
   and stage_mms_BundleSubProduct.dv_batch_id = #incrementals.dv_batch_id
  left join #l_mms_bundle_sub_product_current
    on #incrementals.bk_hash = #l_mms_bundle_sub_product_current.bk_hash

--Insert all updated and new l_mms_bundle_sub_product records
set @start = 1
set @end = (select max(r) from #l_mms_bundle_sub_product_inserts)

while @start <= @end
begin

insert into l_mms_bundle_sub_product (
       bk_hash,
       bundle_sub_product_id,
       bundle_product_id,
       sub_product_id,
       gl_over_ride_club_id,
       val_gl_group_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select bk_hash,
       bundle_sub_product_id,
       bundle_product_id,
       sub_product_id,
       gl_over_ride_club_id,
       val_gl_group_id,
       case when l_mms_bundle_sub_product_id is null then isnull(dv_load_date_time,convert(datetime,'jan 1, 1980',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       source_hash,
       getdate(),
       @user
  from #l_mms_bundle_sub_product_inserts
 where (l_mms_bundle_sub_product_id is null
        or (l_mms_bundle_sub_product_id is not null
            and dv_hash <> source_hash))
   and r >= @start
   and r < @start+1000000

set @start = @start+1000000
end

--Get s_mms_bundle_sub_product current hash
if object_id('tempdb..#s_mms_bundle_sub_product_current') is not null drop table #s_mms_bundle_sub_product_current
create table #s_mms_bundle_sub_product_current with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as
select s_mms_bundle_sub_product.s_mms_bundle_sub_product_id,
       s_mms_bundle_sub_product.bk_hash,
       s_mms_bundle_sub_product.dv_hash
  from dbo.s_mms_bundle_sub_product
  join #p_mms_bundle_sub_product_current
    on s_mms_bundle_sub_product.s_mms_bundle_sub_product_id = #p_mms_bundle_sub_product_current.s_mms_bundle_sub_product_id
   and s_mms_bundle_sub_product.bk_hash = #p_mms_bundle_sub_product_current.bk_hash

--calculate hash and lookup to current s_mms_bundle_sub_product
if object_id('tempdb..#s_mms_bundle_sub_product_inserts') is not null drop table #s_mms_bundle_sub_product_inserts
create table #s_mms_bundle_sub_product_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select #incrementals.bk_hash,
       stage_mms_BundleSubProduct.BundleSubProductID bundle_sub_product_id,
       stage_mms_BundleSubProduct.BundleProductGroupNumber bundle_product_group_number,
       stage_mms_BundleSubProduct.Quantity quantity,
       stage_mms_BundleSubProduct.GLAccountNumber gl_account_number,
       stage_mms_BundleSubProduct.GLSubAccountNumber gl_sub_account_number,
       stage_mms_BundleSubProduct.InsertedDateTime inserted_date_time,
       stage_mms_BundleSubProduct.UpdatedDateTime updated_date_time,
       stage_mms_BundleSubProduct.WorkdayAccount workday_account,
       stage_mms_BundleSubProduct.WorkdayCostCenter workday_cost_center,
       stage_mms_BundleSubProduct.WorkdayOffering workday_offering,
       stage_mms_BundleSubProduct.WorkdayOverRideRegion workday_over_ride_region,
       stage_mms_BundleSubProduct.WorkdayRevenueProductGroupAccount workday_revenue_product_group_account,
       stage_mms_BundleSubProduct.InsertedDateTime dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_mms_BundleSubProduct.BundleSubProductID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_mms_BundleSubProduct.BundleProductGroupNumber as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_mms_BundleSubProduct.Quantity as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_mms_BundleSubProduct.GLAccountNumber,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_mms_BundleSubProduct.GLSubAccountNumber,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_mms_BundleSubProduct.InsertedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_mms_BundleSubProduct.UpdatedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_mms_BundleSubProduct.WorkdayAccount,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_mms_BundleSubProduct.WorkdayCostCenter,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_mms_BundleSubProduct.WorkdayOffering,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_mms_BundleSubProduct.WorkdayOverRideRegion,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_mms_BundleSubProduct.WorkdayRevenueProductGroupAccount,'z#@$k%&P'))),2) source_hash,
       #s_mms_bundle_sub_product_current.s_mms_bundle_sub_product_id,
       #s_mms_bundle_sub_product_current.dv_hash,
       row_number() over (order by #incrementals.bk_hash) r
  from dbo.stage_mms_BundleSubProduct
  join #incrementals
    on stage_mms_BundleSubProduct.stage_mms_BundleSubProduct_id = #incrementals.source_table_id
   and stage_mms_BundleSubProduct.dv_batch_id = #incrementals.dv_batch_id
  left join #s_mms_bundle_sub_product_current
    on #incrementals.bk_hash = #s_mms_bundle_sub_product_current.bk_hash

--Insert all updated and new s_mms_bundle_sub_product records
set @start = 1
set @end = (select max(r) from #s_mms_bundle_sub_product_inserts)

while @start <= @end
begin

insert into s_mms_bundle_sub_product (
       bk_hash,
       bundle_sub_product_id,
       bundle_product_group_number,
       quantity,
       gl_account_number,
       gl_sub_account_number,
       inserted_date_time,
       updated_date_time,
       workday_account,
       workday_cost_center,
       workday_offering,
       workday_over_ride_region,
       workday_revenue_product_group_account,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select bk_hash,
       bundle_sub_product_id,
       bundle_product_group_number,
       quantity,
       gl_account_number,
       gl_sub_account_number,
       inserted_date_time,
       updated_date_time,
       workday_account,
       workday_cost_center,
       workday_offering,
       workday_over_ride_region,
       workday_revenue_product_group_account,
       case when s_mms_bundle_sub_product_id is null then isnull(dv_load_date_time,convert(datetime,'jan 1, 1980',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       source_hash,
       getdate(),
       @user
  from #s_mms_bundle_sub_product_inserts
 where (s_mms_bundle_sub_product_id is null
        or (s_mms_bundle_sub_product_id is not null
            and dv_hash <> source_hash))
   and r >= @start
   and r < @start+1000000

set @start = @start+1000000
end

--Run the PIT proc
exec dbo.proc_p_mms_bundle_sub_product @current_dv_batch_id

--Done!
drop table #incrementals
end
