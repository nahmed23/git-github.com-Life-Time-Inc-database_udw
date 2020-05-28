CREATE PROC [dbo].[proc_etl_mms_membership_recurrent_product] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_mms_MembershipRecurrentProduct

set @insert_date_time = getdate()
insert into dbo.stage_hash_mms_MembershipRecurrentProduct (
       bk_hash,
       MembershipRecurrentProductID,
       MembershipID,
       ProductID,
       ActivationDate,
       CancellationRequestDate,
       TerminationDate,
       ValRecurrentProductTerminationReasonID,
       InsertedDateTime,
       UpdatedDateTime,
       ClubID,
       Price,
       CreatedDateTime,
       UTCCreatedDateTime,
       CreatedDateTimeZone,
       LastUpdatedEmployeeID,
       LastUpdatedDateTime,
       UTCLastUpdatedDateTime,
       LastUpdatedDateTimeZone,
       ProductAssessedDateTime,
       Comments,
       NumberOfSessions,
       PricePerSession,
       CommissionEmployeeID,
       MemberID,
       ValRecurrentProductSourceID,
       ValAssessmentDayID,
       ProductHoldBeginDate,
       ProductHoldEndDate,
       SoldNotServicedFlag,
       RetailPrice,
       RetailPricePerSession,
       PromotionCode,
       PricingDiscountID,
       ValDiscountReasonID,
       DisplayOnlyFlag,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(MembershipRecurrentProductID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       MembershipRecurrentProductID,
       MembershipID,
       ProductID,
       ActivationDate,
       CancellationRequestDate,
       TerminationDate,
       ValRecurrentProductTerminationReasonID,
       InsertedDateTime,
       UpdatedDateTime,
       ClubID,
       Price,
       CreatedDateTime,
       UTCCreatedDateTime,
       CreatedDateTimeZone,
       LastUpdatedEmployeeID,
       LastUpdatedDateTime,
       UTCLastUpdatedDateTime,
       LastUpdatedDateTimeZone,
       ProductAssessedDateTime,
       Comments,
       NumberOfSessions,
       PricePerSession,
       CommissionEmployeeID,
       MemberID,
       ValRecurrentProductSourceID,
       ValAssessmentDayID,
       ProductHoldBeginDate,
       ProductHoldEndDate,
       SoldNotServicedFlag,
       RetailPrice,
       RetailPricePerSession,
       PromotionCode,
       PricingDiscountID,
       ValDiscountReasonID,
       DisplayOnlyFlag,
       isnull(cast(stage_mms_MembershipRecurrentProduct.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_mms_MembershipRecurrentProduct
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_mms_membership_recurrent_product @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_mms_membership_recurrent_product (
       bk_hash,
       membership_recurrent_product_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_mms_MembershipRecurrentProduct.bk_hash,
       stage_hash_mms_MembershipRecurrentProduct.MembershipRecurrentProductID membership_recurrent_product_id,
       isnull(cast(stage_hash_mms_MembershipRecurrentProduct.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       2,
       @insert_date_time,
       @user
  from stage_hash_mms_MembershipRecurrentProduct
  left join h_mms_membership_recurrent_product
    on stage_hash_mms_MembershipRecurrentProduct.bk_hash = h_mms_membership_recurrent_product.bk_hash
 where h_mms_membership_recurrent_product_id is null
   and stage_hash_mms_MembershipRecurrentProduct.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_mms_membership_recurrent_product
if object_id('tempdb..#l_mms_membership_recurrent_product_inserts') is not null drop table #l_mms_membership_recurrent_product_inserts
create table #l_mms_membership_recurrent_product_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_MembershipRecurrentProduct.bk_hash,
       stage_hash_mms_MembershipRecurrentProduct.MembershipRecurrentProductID membership_recurrent_product_id,
       stage_hash_mms_MembershipRecurrentProduct.MembershipID membership_id,
       stage_hash_mms_MembershipRecurrentProduct.ProductID product_id,
       stage_hash_mms_MembershipRecurrentProduct.ValRecurrentProductTerminationReasonID val_recurrent_product_termination_reason_id,
       stage_hash_mms_MembershipRecurrentProduct.ClubID club_id,
       stage_hash_mms_MembershipRecurrentProduct.LastUpdatedEmployeeID last_updated_employee_id,
       stage_hash_mms_MembershipRecurrentProduct.CommissionEmployeeID commission_employee_id,
       stage_hash_mms_MembershipRecurrentProduct.MemberID member_id,
       stage_hash_mms_MembershipRecurrentProduct.ValRecurrentProductSourceID val_recurrent_product_source_id,
       stage_hash_mms_MembershipRecurrentProduct.ValAssessmentDayID val_assessment_day_id,
       stage_hash_mms_MembershipRecurrentProduct.PricingDiscountID pricing_discount_id,
       stage_hash_mms_MembershipRecurrentProduct.ValDiscountReasonID val_discount_reason_id,
       isnull(cast(stage_hash_mms_MembershipRecurrentProduct.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipRecurrentProduct.MembershipRecurrentProductID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipRecurrentProduct.MembershipID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipRecurrentProduct.ProductID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipRecurrentProduct.ValRecurrentProductTerminationReasonID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipRecurrentProduct.ClubID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipRecurrentProduct.LastUpdatedEmployeeID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipRecurrentProduct.CommissionEmployeeID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipRecurrentProduct.MemberID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipRecurrentProduct.ValRecurrentProductSourceID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipRecurrentProduct.ValAssessmentDayID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipRecurrentProduct.PricingDiscountID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipRecurrentProduct.ValDiscountReasonID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_MembershipRecurrentProduct
 where stage_hash_mms_MembershipRecurrentProduct.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_mms_membership_recurrent_product records
set @insert_date_time = getdate()
insert into l_mms_membership_recurrent_product (
       bk_hash,
       membership_recurrent_product_id,
       membership_id,
       product_id,
       val_recurrent_product_termination_reason_id,
       club_id,
       last_updated_employee_id,
       commission_employee_id,
       member_id,
       val_recurrent_product_source_id,
       val_assessment_day_id,
       pricing_discount_id,
       val_discount_reason_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_mms_membership_recurrent_product_inserts.bk_hash,
       #l_mms_membership_recurrent_product_inserts.membership_recurrent_product_id,
       #l_mms_membership_recurrent_product_inserts.membership_id,
       #l_mms_membership_recurrent_product_inserts.product_id,
       #l_mms_membership_recurrent_product_inserts.val_recurrent_product_termination_reason_id,
       #l_mms_membership_recurrent_product_inserts.club_id,
       #l_mms_membership_recurrent_product_inserts.last_updated_employee_id,
       #l_mms_membership_recurrent_product_inserts.commission_employee_id,
       #l_mms_membership_recurrent_product_inserts.member_id,
       #l_mms_membership_recurrent_product_inserts.val_recurrent_product_source_id,
       #l_mms_membership_recurrent_product_inserts.val_assessment_day_id,
       #l_mms_membership_recurrent_product_inserts.pricing_discount_id,
       #l_mms_membership_recurrent_product_inserts.val_discount_reason_id,
       case when l_mms_membership_recurrent_product.l_mms_membership_recurrent_product_id is null then isnull(#l_mms_membership_recurrent_product_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #l_mms_membership_recurrent_product_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_mms_membership_recurrent_product_inserts
  left join p_mms_membership_recurrent_product
    on #l_mms_membership_recurrent_product_inserts.bk_hash = p_mms_membership_recurrent_product.bk_hash
   and p_mms_membership_recurrent_product.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_mms_membership_recurrent_product
    on p_mms_membership_recurrent_product.bk_hash = l_mms_membership_recurrent_product.bk_hash
   and p_mms_membership_recurrent_product.l_mms_membership_recurrent_product_id = l_mms_membership_recurrent_product.l_mms_membership_recurrent_product_id
 where l_mms_membership_recurrent_product.l_mms_membership_recurrent_product_id is null
    or (l_mms_membership_recurrent_product.l_mms_membership_recurrent_product_id is not null
        and l_mms_membership_recurrent_product.dv_hash <> #l_mms_membership_recurrent_product_inserts.source_hash)

--calculate hash and lookup to current s_mms_membership_recurrent_product
if object_id('tempdb..#s_mms_membership_recurrent_product_inserts') is not null drop table #s_mms_membership_recurrent_product_inserts
create table #s_mms_membership_recurrent_product_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_MembershipRecurrentProduct.bk_hash,
       stage_hash_mms_MembershipRecurrentProduct.MembershipRecurrentProductID membership_recurrent_product_id,
       stage_hash_mms_MembershipRecurrentProduct.ActivationDate activation_date,
       stage_hash_mms_MembershipRecurrentProduct.CancellationRequestDate cancellation_request_date,
       stage_hash_mms_MembershipRecurrentProduct.TerminationDate termination_date,
       stage_hash_mms_MembershipRecurrentProduct.InsertedDateTime inserted_date_time,
       stage_hash_mms_MembershipRecurrentProduct.UpdatedDateTime updated_date_time,
       stage_hash_mms_MembershipRecurrentProduct.Price price,
       stage_hash_mms_MembershipRecurrentProduct.CreatedDateTime created_date_time,
       stage_hash_mms_MembershipRecurrentProduct.UTCCreatedDateTime utc_created_date_time,
       stage_hash_mms_MembershipRecurrentProduct.CreatedDateTimeZone created_date_time_zone,
       stage_hash_mms_MembershipRecurrentProduct.LastUpdatedDateTime last_updated_date_time,
       stage_hash_mms_MembershipRecurrentProduct.UTCLastUpdatedDateTime utc_last_updated_date_time,
       stage_hash_mms_MembershipRecurrentProduct.LastUpdatedDateTimeZone last_updated_date_time_zone,
       stage_hash_mms_MembershipRecurrentProduct.ProductAssessedDateTime product_assessed_date_time,
       stage_hash_mms_MembershipRecurrentProduct.Comments comments,
       stage_hash_mms_MembershipRecurrentProduct.NumberOfSessions number_of_sessions,
       stage_hash_mms_MembershipRecurrentProduct.PricePerSession price_per_session,
       stage_hash_mms_MembershipRecurrentProduct.ProductHoldBeginDate product_hold_begin_date,
       stage_hash_mms_MembershipRecurrentProduct.ProductHoldEndDate product_hold_end_date,
       stage_hash_mms_MembershipRecurrentProduct.SoldNotServicedFlag sold_not_serviced_flag,
       stage_hash_mms_MembershipRecurrentProduct.RetailPrice retail_price,
       stage_hash_mms_MembershipRecurrentProduct.RetailPricePerSession retail_price_per_session,
       stage_hash_mms_MembershipRecurrentProduct.PromotionCode promotion_code,
       stage_hash_mms_MembershipRecurrentProduct.DisplayOnlyFlag display_only_flag,
       isnull(cast(stage_hash_mms_MembershipRecurrentProduct.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipRecurrentProduct.MembershipRecurrentProductID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_MembershipRecurrentProduct.ActivationDate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_MembershipRecurrentProduct.CancellationRequestDate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_MembershipRecurrentProduct.TerminationDate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_MembershipRecurrentProduct.InsertedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_MembershipRecurrentProduct.UpdatedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipRecurrentProduct.Price as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_MembershipRecurrentProduct.CreatedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_MembershipRecurrentProduct.UTCCreatedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_MembershipRecurrentProduct.CreatedDateTimeZone,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_MembershipRecurrentProduct.LastUpdatedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_MembershipRecurrentProduct.UTCLastUpdatedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_MembershipRecurrentProduct.LastUpdatedDateTimeZone,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_MembershipRecurrentProduct.ProductAssessedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_MembershipRecurrentProduct.Comments,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipRecurrentProduct.NumberOfSessions as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipRecurrentProduct.PricePerSession as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_MembershipRecurrentProduct.ProductHoldBeginDate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_MembershipRecurrentProduct.ProductHoldEndDate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipRecurrentProduct.SoldNotServicedFlag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipRecurrentProduct.RetailPrice as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipRecurrentProduct.RetailPricePerSession as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_MembershipRecurrentProduct.PromotionCode,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipRecurrentProduct.DisplayOnlyFlag as varchar(42)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_MembershipRecurrentProduct
 where stage_hash_mms_MembershipRecurrentProduct.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mms_membership_recurrent_product records
set @insert_date_time = getdate()
insert into s_mms_membership_recurrent_product (
       bk_hash,
       membership_recurrent_product_id,
       activation_date,
       cancellation_request_date,
       termination_date,
       inserted_date_time,
       updated_date_time,
       price,
       created_date_time,
       utc_created_date_time,
       created_date_time_zone,
       last_updated_date_time,
       utc_last_updated_date_time,
       last_updated_date_time_zone,
       product_assessed_date_time,
       comments,
       number_of_sessions,
       price_per_session,
       product_hold_begin_date,
       product_hold_end_date,
       sold_not_serviced_flag,
       retail_price,
       retail_price_per_session,
       promotion_code,
       display_only_flag,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mms_membership_recurrent_product_inserts.bk_hash,
       #s_mms_membership_recurrent_product_inserts.membership_recurrent_product_id,
       #s_mms_membership_recurrent_product_inserts.activation_date,
       #s_mms_membership_recurrent_product_inserts.cancellation_request_date,
       #s_mms_membership_recurrent_product_inserts.termination_date,
       #s_mms_membership_recurrent_product_inserts.inserted_date_time,
       #s_mms_membership_recurrent_product_inserts.updated_date_time,
       #s_mms_membership_recurrent_product_inserts.price,
       #s_mms_membership_recurrent_product_inserts.created_date_time,
       #s_mms_membership_recurrent_product_inserts.utc_created_date_time,
       #s_mms_membership_recurrent_product_inserts.created_date_time_zone,
       #s_mms_membership_recurrent_product_inserts.last_updated_date_time,
       #s_mms_membership_recurrent_product_inserts.utc_last_updated_date_time,
       #s_mms_membership_recurrent_product_inserts.last_updated_date_time_zone,
       #s_mms_membership_recurrent_product_inserts.product_assessed_date_time,
       #s_mms_membership_recurrent_product_inserts.comments,
       #s_mms_membership_recurrent_product_inserts.number_of_sessions,
       #s_mms_membership_recurrent_product_inserts.price_per_session,
       #s_mms_membership_recurrent_product_inserts.product_hold_begin_date,
       #s_mms_membership_recurrent_product_inserts.product_hold_end_date,
       #s_mms_membership_recurrent_product_inserts.sold_not_serviced_flag,
       #s_mms_membership_recurrent_product_inserts.retail_price,
       #s_mms_membership_recurrent_product_inserts.retail_price_per_session,
       #s_mms_membership_recurrent_product_inserts.promotion_code,
       #s_mms_membership_recurrent_product_inserts.display_only_flag,
       case when s_mms_membership_recurrent_product.s_mms_membership_recurrent_product_id is null then isnull(#s_mms_membership_recurrent_product_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #s_mms_membership_recurrent_product_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mms_membership_recurrent_product_inserts
  left join p_mms_membership_recurrent_product
    on #s_mms_membership_recurrent_product_inserts.bk_hash = p_mms_membership_recurrent_product.bk_hash
   and p_mms_membership_recurrent_product.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mms_membership_recurrent_product
    on p_mms_membership_recurrent_product.bk_hash = s_mms_membership_recurrent_product.bk_hash
   and p_mms_membership_recurrent_product.s_mms_membership_recurrent_product_id = s_mms_membership_recurrent_product.s_mms_membership_recurrent_product_id
 where s_mms_membership_recurrent_product.s_mms_membership_recurrent_product_id is null
    or (s_mms_membership_recurrent_product.s_mms_membership_recurrent_product_id is not null
        and s_mms_membership_recurrent_product.dv_hash <> #s_mms_membership_recurrent_product_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_mms_membership_recurrent_product @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_mms_membership_recurrent_product @current_dv_batch_id

end
