CREATE PROC [dbo].[proc_etl_mms_package_adjustment] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_mms_PackageAdjustment

set @insert_date_time = getdate()
insert into dbo.stage_hash_mms_PackageAdjustment (
       bk_hash,
       PackageAdjustmentID,
       PackageID,
       AdjustedDateTime,
       UTCAdjustedDateTime,
       AdjustedDateTimeZone,
       EmployeeID,
       MMSTranID,
       SessionsAdjusted,
       AmountAdjusted,
       Comment,
       ValPackageAdjustmentTypeID,
       InsertedDateTime,
       UpdatedDateTime,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(PackageAdjustmentID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       PackageAdjustmentID,
       PackageID,
       AdjustedDateTime,
       UTCAdjustedDateTime,
       AdjustedDateTimeZone,
       EmployeeID,
       MMSTranID,
       SessionsAdjusted,
       AmountAdjusted,
       Comment,
       ValPackageAdjustmentTypeID,
       InsertedDateTime,
       UpdatedDateTime,
       isnull(cast(stage_mms_PackageAdjustment.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_mms_PackageAdjustment
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_mms_package_adjustment @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_mms_package_adjustment (
       bk_hash,
       package_adjustment_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_mms_PackageAdjustment.bk_hash,
       stage_hash_mms_PackageAdjustment.PackageAdjustmentID package_adjustment_id,
       isnull(cast(stage_hash_mms_PackageAdjustment.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       2,
       @insert_date_time,
       @user
  from stage_hash_mms_PackageAdjustment
  left join h_mms_package_adjustment
    on stage_hash_mms_PackageAdjustment.bk_hash = h_mms_package_adjustment.bk_hash
 where h_mms_package_adjustment_id is null
   and stage_hash_mms_PackageAdjustment.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_mms_package_adjustment
if object_id('tempdb..#l_mms_package_adjustment_inserts') is not null drop table #l_mms_package_adjustment_inserts
create table #l_mms_package_adjustment_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_PackageAdjustment.bk_hash,
       stage_hash_mms_PackageAdjustment.PackageAdjustmentID package_adjustment_id,
       stage_hash_mms_PackageAdjustment.PackageID package_id,
       stage_hash_mms_PackageAdjustment.EmployeeID employee_id,
       stage_hash_mms_PackageAdjustment.MMSTranID mms_tran_id,
       stage_hash_mms_PackageAdjustment.ValPackageAdjustmentTypeID val_package_adjustment_type_id,
       isnull(cast(stage_hash_mms_PackageAdjustment.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_PackageAdjustment.PackageAdjustmentID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_PackageAdjustment.PackageID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_PackageAdjustment.EmployeeID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_PackageAdjustment.MMSTranID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_PackageAdjustment.ValPackageAdjustmentTypeID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_PackageAdjustment
 where stage_hash_mms_PackageAdjustment.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_mms_package_adjustment records
set @insert_date_time = getdate()
insert into l_mms_package_adjustment (
       bk_hash,
       package_adjustment_id,
       package_id,
       employee_id,
       mms_tran_id,
       val_package_adjustment_type_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_mms_package_adjustment_inserts.bk_hash,
       #l_mms_package_adjustment_inserts.package_adjustment_id,
       #l_mms_package_adjustment_inserts.package_id,
       #l_mms_package_adjustment_inserts.employee_id,
       #l_mms_package_adjustment_inserts.mms_tran_id,
       #l_mms_package_adjustment_inserts.val_package_adjustment_type_id,
       case when l_mms_package_adjustment.l_mms_package_adjustment_id is null then isnull(#l_mms_package_adjustment_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #l_mms_package_adjustment_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_mms_package_adjustment_inserts
  left join p_mms_package_adjustment
    on #l_mms_package_adjustment_inserts.bk_hash = p_mms_package_adjustment.bk_hash
   and p_mms_package_adjustment.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_mms_package_adjustment
    on p_mms_package_adjustment.bk_hash = l_mms_package_adjustment.bk_hash
   and p_mms_package_adjustment.l_mms_package_adjustment_id = l_mms_package_adjustment.l_mms_package_adjustment_id
 where l_mms_package_adjustment.l_mms_package_adjustment_id is null
    or (l_mms_package_adjustment.l_mms_package_adjustment_id is not null
        and l_mms_package_adjustment.dv_hash <> #l_mms_package_adjustment_inserts.source_hash)

--calculate hash and lookup to current s_mms_package_adjustment
if object_id('tempdb..#s_mms_package_adjustment_inserts') is not null drop table #s_mms_package_adjustment_inserts
create table #s_mms_package_adjustment_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_PackageAdjustment.bk_hash,
       stage_hash_mms_PackageAdjustment.PackageAdjustmentID package_adjustment_id,
       stage_hash_mms_PackageAdjustment.AdjustedDateTime adjusted_date_time,
       stage_hash_mms_PackageAdjustment.UTCAdjustedDateTime utc_adjusted_date_time,
       stage_hash_mms_PackageAdjustment.AdjustedDateTimeZone adjusted_date_time_zone,
       stage_hash_mms_PackageAdjustment.SessionsAdjusted sessions_adjusted,
       stage_hash_mms_PackageAdjustment.AmountAdjusted amount_adjusted,
       stage_hash_mms_PackageAdjustment.Comment comment,
       stage_hash_mms_PackageAdjustment.InsertedDateTime inserted_date_time,
       stage_hash_mms_PackageAdjustment.UpdatedDateTime updated_date_time,
       isnull(cast(stage_hash_mms_PackageAdjustment.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_PackageAdjustment.PackageAdjustmentID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_PackageAdjustment.AdjustedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_PackageAdjustment.UTCAdjustedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_PackageAdjustment.AdjustedDateTimeZone,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_PackageAdjustment.SessionsAdjusted as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_PackageAdjustment.AmountAdjusted as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_PackageAdjustment.Comment,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_PackageAdjustment.InsertedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_PackageAdjustment.UpdatedDateTime,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_PackageAdjustment
 where stage_hash_mms_PackageAdjustment.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mms_package_adjustment records
set @insert_date_time = getdate()
insert into s_mms_package_adjustment (
       bk_hash,
       package_adjustment_id,
       adjusted_date_time,
       utc_adjusted_date_time,
       adjusted_date_time_zone,
       sessions_adjusted,
       amount_adjusted,
       comment,
       inserted_date_time,
       updated_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mms_package_adjustment_inserts.bk_hash,
       #s_mms_package_adjustment_inserts.package_adjustment_id,
       #s_mms_package_adjustment_inserts.adjusted_date_time,
       #s_mms_package_adjustment_inserts.utc_adjusted_date_time,
       #s_mms_package_adjustment_inserts.adjusted_date_time_zone,
       #s_mms_package_adjustment_inserts.sessions_adjusted,
       #s_mms_package_adjustment_inserts.amount_adjusted,
       #s_mms_package_adjustment_inserts.comment,
       #s_mms_package_adjustment_inserts.inserted_date_time,
       #s_mms_package_adjustment_inserts.updated_date_time,
       case when s_mms_package_adjustment.s_mms_package_adjustment_id is null then isnull(#s_mms_package_adjustment_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #s_mms_package_adjustment_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mms_package_adjustment_inserts
  left join p_mms_package_adjustment
    on #s_mms_package_adjustment_inserts.bk_hash = p_mms_package_adjustment.bk_hash
   and p_mms_package_adjustment.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mms_package_adjustment
    on p_mms_package_adjustment.bk_hash = s_mms_package_adjustment.bk_hash
   and p_mms_package_adjustment.s_mms_package_adjustment_id = s_mms_package_adjustment.s_mms_package_adjustment_id
 where s_mms_package_adjustment.s_mms_package_adjustment_id is null
    or (s_mms_package_adjustment.s_mms_package_adjustment_id is not null
        and s_mms_package_adjustment.dv_hash <> #s_mms_package_adjustment_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_mms_package_adjustment @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_mms_package_adjustment @current_dv_batch_id

end
