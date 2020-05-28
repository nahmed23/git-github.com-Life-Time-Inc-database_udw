CREATE PROC [dbo].[proc_etl_mms_package] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_mms_Package

set @insert_date_time = getdate()
insert into dbo.stage_hash_mms_Package (
       bk_hash,
       PackageID,
       MemberID,
       MembershipID,
       ClubID,
       NumberOfSessions,
       PricePerSession,
       EmployeeID,
       ValPackageStatusID,
       MMSTranID,
       ProductID,
       TranItemID,
       CreatedDateTime,
       UTCCreatedDateTime,
       CreatedDateTimeZone,
       SessionsLeft,
       BalanceAmount,
       InsertedDateTime,
       UpdatedDateTime,
       PackageEditedFlag,
       PackageEditDateTime,
       UTCPackageEditDateTime,
       PackageEditDateTimeZone,
       ExpirationDateTime,
       UnexpireCount,
       LastUnexpiredDateTime,
       TransactionSource,
       ExternalPackageID,
       OriginalBalanceAmount,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(PackageID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       PackageID,
       MemberID,
       MembershipID,
       ClubID,
       NumberOfSessions,
       PricePerSession,
       EmployeeID,
       ValPackageStatusID,
       MMSTranID,
       ProductID,
       TranItemID,
       CreatedDateTime,
       UTCCreatedDateTime,
       CreatedDateTimeZone,
       SessionsLeft,
       BalanceAmount,
       InsertedDateTime,
       UpdatedDateTime,
       PackageEditedFlag,
       PackageEditDateTime,
       UTCPackageEditDateTime,
       PackageEditDateTimeZone,
       ExpirationDateTime,
       UnexpireCount,
       LastUnexpiredDateTime,
       TransactionSource,
       ExternalPackageID,
       OriginalBalanceAmount,
       isnull(cast(stage_mms_Package.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_mms_Package
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_mms_package @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_mms_package (
       bk_hash,
       package_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_mms_Package.bk_hash,
       stage_hash_mms_Package.PackageID package_id,
       isnull(cast(stage_hash_mms_Package.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       2,
       @insert_date_time,
       @user
  from stage_hash_mms_Package
  left join h_mms_package
    on stage_hash_mms_Package.bk_hash = h_mms_package.bk_hash
 where h_mms_package_id is null
   and stage_hash_mms_Package.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_mms_package
if object_id('tempdb..#l_mms_package_inserts') is not null drop table #l_mms_package_inserts
create table #l_mms_package_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_Package.bk_hash,
       stage_hash_mms_Package.PackageID package_id,
       stage_hash_mms_Package.MemberID member_id,
       stage_hash_mms_Package.MembershipID membership_id,
       stage_hash_mms_Package.ClubID club_id,
       stage_hash_mms_Package.EmployeeID employee_id,
       stage_hash_mms_Package.ValPackageStatusID val_package_status_id,
       stage_hash_mms_Package.MMSTranID mms_tran_id,
       stage_hash_mms_Package.ProductID product_id,
       stage_hash_mms_Package.TranItemID tran_item_id,
       stage_hash_mms_Package.ExternalPackageID external_package_id,
       isnull(cast(stage_hash_mms_Package.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_Package.PackageID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Package.MemberID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Package.MembershipID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Package.ClubID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Package.EmployeeID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Package.ValPackageStatusID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Package.MMSTranID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Package.ProductID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Package.TranItemID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_Package.ExternalPackageID,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_Package
 where stage_hash_mms_Package.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_mms_package records
set @insert_date_time = getdate()
insert into l_mms_package (
       bk_hash,
       package_id,
       member_id,
       membership_id,
       club_id,
       employee_id,
       val_package_status_id,
       mms_tran_id,
       product_id,
       tran_item_id,
       external_package_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_mms_package_inserts.bk_hash,
       #l_mms_package_inserts.package_id,
       #l_mms_package_inserts.member_id,
       #l_mms_package_inserts.membership_id,
       #l_mms_package_inserts.club_id,
       #l_mms_package_inserts.employee_id,
       #l_mms_package_inserts.val_package_status_id,
       #l_mms_package_inserts.mms_tran_id,
       #l_mms_package_inserts.product_id,
       #l_mms_package_inserts.tran_item_id,
       #l_mms_package_inserts.external_package_id,
       case when l_mms_package.l_mms_package_id is null then isnull(#l_mms_package_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #l_mms_package_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_mms_package_inserts
  left join p_mms_package
    on #l_mms_package_inserts.bk_hash = p_mms_package.bk_hash
   and p_mms_package.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_mms_package
    on p_mms_package.bk_hash = l_mms_package.bk_hash
   and p_mms_package.l_mms_package_id = l_mms_package.l_mms_package_id
 where l_mms_package.l_mms_package_id is null
    or (l_mms_package.l_mms_package_id is not null
        and l_mms_package.dv_hash <> #l_mms_package_inserts.source_hash)

--calculate hash and lookup to current s_mms_package
if object_id('tempdb..#s_mms_package_inserts') is not null drop table #s_mms_package_inserts
create table #s_mms_package_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_Package.bk_hash,
       stage_hash_mms_Package.PackageID package_id,
       stage_hash_mms_Package.NumberOfSessions number_of_sessions,
       stage_hash_mms_Package.PricePerSession price_per_session,
       stage_hash_mms_Package.CreatedDateTime created_date_time,
       stage_hash_mms_Package.UTCCreatedDateTime utc_created_date_time,
       stage_hash_mms_Package.CreatedDateTimeZone created_date_time_zone,
       stage_hash_mms_Package.SessionsLeft sessions_left,
       stage_hash_mms_Package.BalanceAmount balance_amount,
       stage_hash_mms_Package.InsertedDateTime inserted_date_time,
       stage_hash_mms_Package.UpdatedDateTime updated_date_time,
       stage_hash_mms_Package.PackageEditedFlag package_edited_flag,
       stage_hash_mms_Package.PackageEditDateTime package_edit_date_time,
       stage_hash_mms_Package.UTCPackageEditDateTime utc_package_edit_date_time,
       stage_hash_mms_Package.PackageEditDateTimeZone package_edit_date_time_zone,
       stage_hash_mms_Package.ExpirationDateTime expiration_date_time,
       stage_hash_mms_Package.UnexpireCount unexpire_count,
       stage_hash_mms_Package.LastUnexpiredDateTime last_unexpired_date_time,
       stage_hash_mms_Package.TransactionSource transaction_source,
       stage_hash_mms_Package.OriginalBalanceAmount original_balance_amount,
       isnull(cast(stage_hash_mms_Package.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_Package.PackageID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Package.NumberOfSessions as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Package.PricePerSession as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_Package.CreatedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_Package.UTCCreatedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_Package.CreatedDateTimeZone,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Package.SessionsLeft as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Package.BalanceAmount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_Package.InsertedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_Package.UpdatedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Package.PackageEditedFlag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_Package.PackageEditDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_Package.UTCPackageEditDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_Package.PackageEditDateTimeZone,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_Package.ExpirationDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Package.UnexpireCount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_Package.LastUnexpiredDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_Package.TransactionSource,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Package.OriginalBalanceAmount as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_Package
 where stage_hash_mms_Package.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mms_package records
set @insert_date_time = getdate()
insert into s_mms_package (
       bk_hash,
       package_id,
       number_of_sessions,
       price_per_session,
       created_date_time,
       utc_created_date_time,
       created_date_time_zone,
       sessions_left,
       balance_amount,
       inserted_date_time,
       updated_date_time,
       package_edited_flag,
       package_edit_date_time,
       utc_package_edit_date_time,
       package_edit_date_time_zone,
       expiration_date_time,
       unexpire_count,
       last_unexpired_date_time,
       transaction_source,
       original_balance_amount,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mms_package_inserts.bk_hash,
       #s_mms_package_inserts.package_id,
       #s_mms_package_inserts.number_of_sessions,
       #s_mms_package_inserts.price_per_session,
       #s_mms_package_inserts.created_date_time,
       #s_mms_package_inserts.utc_created_date_time,
       #s_mms_package_inserts.created_date_time_zone,
       #s_mms_package_inserts.sessions_left,
       #s_mms_package_inserts.balance_amount,
       #s_mms_package_inserts.inserted_date_time,
       #s_mms_package_inserts.updated_date_time,
       #s_mms_package_inserts.package_edited_flag,
       #s_mms_package_inserts.package_edit_date_time,
       #s_mms_package_inserts.utc_package_edit_date_time,
       #s_mms_package_inserts.package_edit_date_time_zone,
       #s_mms_package_inserts.expiration_date_time,
       #s_mms_package_inserts.unexpire_count,
       #s_mms_package_inserts.last_unexpired_date_time,
       #s_mms_package_inserts.transaction_source,
       #s_mms_package_inserts.original_balance_amount,
       case when s_mms_package.s_mms_package_id is null then isnull(#s_mms_package_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #s_mms_package_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mms_package_inserts
  left join p_mms_package
    on #s_mms_package_inserts.bk_hash = p_mms_package.bk_hash
   and p_mms_package.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mms_package
    on p_mms_package.bk_hash = s_mms_package.bk_hash
   and p_mms_package.s_mms_package_id = s_mms_package.s_mms_package_id
 where s_mms_package.s_mms_package_id is null
    or (s_mms_package.s_mms_package_id is not null
        and s_mms_package.dv_hash <> #s_mms_package_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_mms_package @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_mms_package @current_dv_batch_id

end
