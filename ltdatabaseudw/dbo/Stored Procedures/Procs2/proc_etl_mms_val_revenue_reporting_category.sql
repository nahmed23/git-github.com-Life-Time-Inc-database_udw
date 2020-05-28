CREATE PROC [dbo].[proc_etl_mms_val_revenue_reporting_category] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

--grab incremental changes from staging
if object_id('tempdb..#source') is not null drop table #source
create table dbo.#source with (distribution=round_robin, location=user_db, heap) as
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(ValRevenueReportingCategoryID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       isnull(UpdatedDateTime, convert(datetime,'jan 1, 1980',120)) dv_load_date_time,
       stage_mms_ValRevenueReportingCategory.ValRevenueReportingCategoryID val_revenue_reporting_category_id,
       stage_mms_ValRevenueReportingCategory.Description description,
       stage_mms_ValRevenueReportingCategory.SortOrder sort_order,
       stage_mms_ValRevenueReportingCategory.InsertedDateTime inserted_date_time,
       stage_mms_ValRevenueReportingCategory.UpdatedDateTime updated_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_mms_ValRevenueReportingCategory.ValRevenueReportingCategoryID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_mms_ValRevenueReportingCategory.Description,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_mms_ValRevenueReportingCategory.SortOrder as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_mms_ValRevenueReportingCategory.InsertedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_mms_ValRevenueReportingCategory.UpdatedDateTime,120),'z#@$k%&P'))),2) source_hash,
       dv_batch_id
  from dbo.stage_mms_ValRevenueReportingCategory
 where (ValRevenueReportingCategoryID is not null)
 
--grab current values in lt_udw
if object_id('tempdb..#current') is not null drop table #current
create table dbo.#current with (distribution=round_robin, location=user_db, heap) as
select r_mms_val_revenue_reporting_category.r_mms_val_revenue_reporting_category_id,
       r_mms_val_revenue_reporting_category.bk_hash,
       r_mms_val_revenue_reporting_category.dv_hash
  from dbo.r_mms_val_revenue_reporting_category
  join #source
    on r_mms_val_revenue_reporting_category.bk_hash = #source.bk_hash
   and r_mms_val_revenue_reporting_category.dv_load_end_date_time = convert(varchar,'dec 31, 9999',120)

--join up incremental and current
if object_id('tempdb..#process') is not null drop table #process
create table dbo.#process with (distribution=round_robin, location=user_db, heap) as
select row_number() over (order by #source.bk_hash) rownum,
       #source.bk_hash,
       val_revenue_reporting_category_id,
       description,
       sort_order,
       inserted_date_time,
       updated_date_time,
       case when #current.r_mms_val_revenue_reporting_category_id is null then dv_load_date_time
            else @job_start_date_time end dv_load_date_time,
       convert(datetime,'Dec 31, 9999',120) dv_load_end_date_time,
       @current_dv_batch_id dv_batch_id,
       2 dv_r_load_source_id,
       #source.source_hash dv_hash,
       #current.r_mms_val_revenue_reporting_category_id
  from #source
  left join #current
    on #source.bk_hash = #current.bk_hash
 where #current.r_mms_val_revenue_reporting_category_id is null
    or (#current.r_mms_val_revenue_reporting_category_id is not null
        and #source.source_hash <> #current.dv_hash)

declare @start_r_id bigint, @c int, @user varchar(50)
set @c = isnull((select max(rownum) from #process),0)

exec dbo.proc_util_sequence_number_get_next @table_name = 'r_mms_val_revenue_reporting_category', @id_count = @c, @start_id = @start_r_id out

begin tran
--end date existing business keys that have a new record with a different hash coming in
set @user = suser_sname()
update dbo.r_mms_val_revenue_reporting_category
   set dv_load_end_date_time = @job_start_date_time,
       dv_updated_date_time = getdate(),
	   dv_update_user = @user
  from #process
 where r_mms_val_revenue_reporting_category.r_mms_val_revenue_reporting_category_id = #process.r_mms_val_revenue_reporting_category_id

--insert incremental changes 
insert into dbo.r_mms_val_revenue_reporting_category (
       r_mms_val_revenue_reporting_category_id,
       bk_hash,
       val_revenue_reporting_category_id,
       description,
       sort_order,
       inserted_date_time,
       updated_date_time,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
	   dv_inserted_date_time,
	   dv_insert_user,
	   dv_updated_date_time,
	   dv_update_user)
select @start_r_id + rownum - 1,
       bk_hash,
       val_revenue_reporting_category_id,
       description,
       sort_order,
       inserted_date_time,
       updated_date_time,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
	   getdate(),
	   suser_sname(),
	   null,
	   null
  from #process
commit tran

end
