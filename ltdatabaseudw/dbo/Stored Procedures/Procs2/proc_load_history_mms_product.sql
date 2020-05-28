CREATE PROC [dbo].[proc_load_history_mms_product] AS
begin

set nocount on
set xact_abort on

--Select the records from [dbo].[MMSProduct] to be staged and inserted into the dv tables

if object_id('tempdb.dbo.#stage_mms_Product_History') is not null drop table #stage_mms_Product_History
create table dbo.#stage_mms_Product_History with (location=user_db, distribution = hash(ProductID)) as
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(ProductID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(ProductID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(DepartmentID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(GLOverRideClubID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(ValGLGroupID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(ValRecurrentProductTypeID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(ValProductStatusID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(ValAssessmentDayID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(WorkdayAccount,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(WorkdayCostCenter,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(WorkdayOffering,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(WorkdayOverRideRegion,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(WorkdayRevenueProductGroupAccount,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(RevenueCategory,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(SpendCategory,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(PayComponent,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(ValEmployeeLevelTypeID as varchar(500)),'z#@$k%&P'))),2)  l_mms_Product_hash,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(ProductID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(Name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(Description,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(DisplayUIFlag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(SortOrder as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,InsertedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,TurnOffDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,StartDate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,EndDate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(GLAccountNumber,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(GLSubAccountNumber,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(CompletePackageFlag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(AllowZeroDollarFlag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(PackageProductFlag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(SoldNotServicedFlag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,UpdatedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(TipAllowedFlag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(JrMemberDuesFlag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(EligibleForHoldFlag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(ConfirmMemberDataFlag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(MedicalProductFlag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(BundleProductFlag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(DeferredRevenueFlag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(PriceLockedFlag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(AssessAsDuesFlag as varchar(42)),'z#@$k%&P') )),2)  s_mms_Product_hash ,
        row_number() over(partition by ProductID order by x.update_insert_date) rank2,
		*
  from (select row_number() over(partition by ProductID,
              case when MMSUpdatedDateTime is null then MMSInsertedDateTime
                   when datepart(hh, isnull(MMSUpdatedDateTime,InsertedDateTime)) in (0, 1) then dateadd(hh, 3, convert(datetime, convert(date, isnull(MMSUpdatedDateTime,InsertedDateTime))))
                   else dateadd(hh, 27, convert(datetime, convert(date, isnull(MMSUpdatedDateTime,InsertedDateTime))))
                   end
                   order by [MMSProductKey] desc) rank1,
              case when MMSUpdatedDateTime is null then MMSInsertedDateTime
                   when datepart(hh, isnull(MMSUpdatedDateTime,InsertedDateTime)) in (0, 1) then dateadd(hh, 3, convert(datetime, convert(date, isnull(MMSUpdatedDateTime,InsertedDateTime))))
                   else dateadd(hh, 27, convert(datetime, convert(date, isnull(MMSUpdatedDateTime,InsertedDateTime))))
               end update_insert_date,
               *
          from stage_mms_product_history) x
 where rank1 = 1					
                              
-- Create the h records.

-- dv_load_date_time is the MMSInsertedDateTime or Jan 1, 1980 if MMSInsertedDateTime is null.
-- dv_batch_id is the dv_load_date_time converted to YYYYMMDDHHMISS
insert into dbo.h_mms_product(
      --h_mms_product_id,
		bk_hash,
		product_id,
		dv_load_date_time, 
		dv_batch_id, 
		dv_r_load_source_id, 
		dv_inserted_date_time, 
		dv_insert_user)
select 
       x.*
  from (select bk_hash,
               ProductID product_id,
               isnull(MMSInsertedDateTime, convert(datetime,'jan 1, 1980',107)) dv_load_date_time, 
               case when MMSInsertedDateTime is null then 19800101000000
                    else replace(replace(replace(convert(varchar, MMSInsertedDateTime,120 ), '-', ''),' ', ''), ':', '')
                end dv_batch_id,
               2 dv_r_load_source_id, 
               getdate() dv_inserted_date_time, 
               suser_sname() dv_insert_user 
          from dbo.#stage_mms_Product_History 
         where rank2 = 1) x
         		
-- Create the l records.
-- Calculate dv_load_date_time
-- dv_batch_id is the dv_load_date_time converted to YYYYMMDDHHMISS
insert into dbo.l_mms_product (
		bk_hash,
       product_id,
       department_id,
       gl_over_ride_club_id,
       val_gl_group_id,
       val_recurrent_product_type_id,
       val_product_status_id,
       val_assessment_day_id,
       workday_account,
       workday_cost_center,
       workday_offering,
	   workday_over_ride_region,
	   workday_revenue_product_group_account,
	   revenue_category,
	   spend_category,
	   pay_component,
	   val_employee_level_type_id,
	   dv_load_date_time,
	    dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
	   	
select 
       x.*
  from (select #stage_mms_Product_History.bk_hash,
       #stage_mms_Product_History.ProductID product_id,
       #stage_mms_Product_History.DepartmentID department_id,
       #stage_mms_Product_History.GLOverRideClubID gl_over_ride_club_id,
       #stage_mms_Product_History.ValGLGroupID val_gl_group_id,
       #stage_mms_Product_History.ValRecurrentProductTypeID val_recurrent_product_type_id,
       #stage_mms_Product_History.ValProductStatusID val_product_status_id,
       #stage_mms_Product_History.ValAssessmentDayID val_assessment_day_id,
       #stage_mms_Product_History.WorkdayAccount workday_account,
       #stage_mms_Product_History.WorkdayCostCenter workday_cost_center,
       #stage_mms_Product_History.WorkdayOffering workday_offering,
       #stage_mms_Product_History.WorkdayOverRideRegion workday_over_ride_region,
       #stage_mms_Product_History.WorkdayRevenueProductGroupAccount workday_revenue_product_group_account,
       #stage_mms_Product_History.RevenueCategory revenue_category,
       #stage_mms_Product_History.SpendCategory spend_category,
       #stage_mms_Product_History.PayComponent pay_component,
       #stage_mms_Product_History.ValEmployeeLevelTypeID val_employee_level_type_id,
              case when #stage_mms_Product_History.rank2 = 1 then
                         case when #stage_mms_Product_History.MMSInsertedDateTime is null then convert(datetime,'jan 1, 1980',107)
                              else #stage_mms_Product_History.MMSInsertedDateTime
                          end
                    when datepart(hh, isnull(#stage_mms_Product_History.MMSUpdatedDateTime,#stage_mms_Product_History.InsertedDateTime)) in (0, 1) 
					then dateadd(hh, 3, convert(datetime, convert(date, isnull(#stage_mms_Product_History.MMSUpdatedDateTime,#stage_mms_Product_History.InsertedDateTime))))
                    else dateadd(hh, 27, convert(datetime, convert(date, isnull(#stage_mms_Product_History.MMSUpdatedDateTime,#stage_mms_Product_History.InsertedDateTime))))
                end dv_load_date_time,
               case when #stage_mms_Product_History.rank2 = 1 then
                         case when #stage_mms_Product_History.MMSInsertedDateTime is null then 19800101000000
                              else replace(replace(replace(convert(varchar, #stage_mms_Product_History.MMSInsertedDateTime,120), '-', ''),' ', ''), ':', '')
                          end
                    when datepart(hh, isnull(#stage_mms_Product_History.MMSUpdatedDateTime,#stage_mms_Product_History.InsertedDateTime)) in (0, 1) then 
					replace(replace(replace(convert(varchar, dateadd(hh, 3, convert(datetime, convert(date, isnull(#stage_mms_Product_History.MMSUpdatedDateTime,#stage_mms_Product_History.InsertedDateTime)))),120), '-', ''),' ', ''), ':', '')
                    else replace(replace(replace(convert(varchar, dateadd(hh, 27, convert(datetime, convert(date, isnull(#stage_mms_Product_History.MMSUpdatedDateTime,#stage_mms_Product_History.InsertedDateTime)))),120), '-', ''),' ', ''), ':', '')
                end dv_batch_id,
               2 dv_r_load_source_id, 
               #stage_mms_Product_History.l_mms_Product_hash,
               getdate() dv_inserted_date_time, 
               suser_sname() dv_insert_user 
           from dbo.#stage_mms_Product_History
          left join dbo.#stage_mms_Product_History prior
            on #stage_mms_Product_History.ProductID = prior.ProductID
           and #stage_mms_Product_History.rank2 = prior.rank2 + 1
         where #stage_mms_Product_History.l_mms_Product_hash != isnull(prior.l_mms_Product_hash, ''))x

-- Create the s records.
-- dv_batch_id is the dv_load_date_time converted to YYYYMMDDHHMISS
insert into dbo.s_mms_product (
      
	   bk_hash,
       product_id,
       name,
       description,
       display_ui_flag,
       sort_order,
       inserted_date_time,
       turn_off_date_time,
       start_date,
       end_date,
       gl_account_number,
       gl_sub_account_number,
       complete_package_flag,
       allow_zero_dollar_flag,
       package_product_flag,
       sold_not_serviced_flag,
       updated_date_time,
       tip_allowed_flag,
       jr_member_dues_flag,
       eligible_for_hold_flag,
       confirm_member_data_flag,
       medical_product_flag,
       bundle_product_flag,
       deferred_revenue_flag,
       price_locked_flag,
       assess_as_dues_flag,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select 
       x.*
  from (select #stage_mms_Product_History.bk_hash,
       #stage_mms_Product_History.ProductID product_id,
       #stage_mms_Product_History.Name name,
       #stage_mms_Product_History.Description description,
       #stage_mms_Product_History.DisplayUIFlag display_ui_flag,
       #stage_mms_Product_History.SortOrder sort_order,
       #stage_mms_Product_History.MMSInsertedDateTime inserted_date_time,
       #stage_mms_Product_History.TurnOffDateTime turn_off_date_time,
       #stage_mms_Product_History.StartDate start_date,
       #stage_mms_Product_History.EndDate end_date,
       #stage_mms_Product_History.GLAccountNumber gl_account_number,
       #stage_mms_Product_History.GLSubAccountNumber gl_sub_account_number,
       #stage_mms_Product_History.CompletePackageFlag complete_package_flag,
       #stage_mms_Product_History.AllowZeroDollarFlag allow_zero_dollar_flag,
       #stage_mms_Product_History.PackageProductFlag package_product_flag,
       #stage_mms_Product_History.SoldNotServicedFlag sold_not_serviced_flag,
       #stage_mms_Product_History.MMSUpdatedDateTime updated_date_time,
       #stage_mms_Product_History.TipAllowedFlag tip_allowed_flag,
       #stage_mms_Product_History.JrMemberDuesFlag jr_member_dues_flag,
       #stage_mms_Product_History.EligibleForHoldFlag eligible_for_hold_flag,
       #stage_mms_Product_History.ConfirmMemberDataFlag confirm_member_data_flag,
       #stage_mms_Product_History.MedicalProductFlag medical_product_flag,
       #stage_mms_Product_History.BundleProductFlag bundle_product_flag,
       #stage_mms_Product_History.DeferredRevenueFlag deferred_revenue_flag,
       #stage_mms_Product_History.PriceLockedFlag price_locked_flag,
       #stage_mms_Product_History.AssessAsDuesFlag assess_as_dues_flag,
               case when #stage_mms_Product_History.rank2 = 1 then
                         case when #stage_mms_Product_History.MMSInsertedDateTime is null then convert(datetime,'jan 1, 1980',107)
                              else #stage_mms_Product_History.MMSInsertedDateTime
                          end
                    when datepart(hh, isnull(#stage_mms_Product_History.MMSUpdatedDateTime,#stage_mms_Product_History.InsertedDateTime)) in (0, 1) 
					then dateadd(hh, 3, convert(datetime, convert(date, isnull(#stage_mms_Product_History.MMSUpdatedDateTime,#stage_mms_Product_History.InsertedDateTime))))
                    else dateadd(hh, 27, convert(datetime, convert(date, isnull(#stage_mms_Product_History.MMSUpdatedDateTime,#stage_mms_Product_History.InsertedDateTime))))
                end dv_load_date_time,
               case when #stage_mms_Product_History.rank2 = 1 then
                         case when #stage_mms_Product_History.MMSInsertedDateTime is null then 19800101000000
                              else replace(replace(replace(convert(varchar, #stage_mms_Product_History.MMSInsertedDateTime,120), '-', ''),' ', ''), ':', '')
                          end
                    when datepart(hh, isnull(#stage_mms_Product_History.MMSUpdatedDateTime,#stage_mms_Product_History.InsertedDateTime)) in (0, 1) 
					then replace(replace(replace(convert(varchar, dateadd(hh, 3, convert(datetime, convert(date, isnull(#stage_mms_Product_History.MMSUpdatedDateTime,#stage_mms_Product_History.InsertedDateTime)))),120), '-', ''),' ', ''), ':', '')
                    else replace(replace(replace(convert(varchar, dateadd(hh, 27, convert(datetime, convert(date, isnull(#stage_mms_Product_History.MMSUpdatedDateTime,#stage_mms_Product_History.InsertedDateTime)))),120), '-', ''),' ', ''), ':', '')
                end dv_batch_id,
               2 dv_r_load_source_id, 
               #stage_mms_Product_History.s_mms_Product_hash,
               getdate() dv_inserted_date_time, 
               suser_sname() dv_insert_user
                   from dbo.#stage_mms_Product_History
          left join dbo.#stage_mms_Product_History prior
            on #stage_mms_Product_History.ProductID = prior.ProductID
           and #stage_mms_Product_History.rank2 = prior.rank2 + 1
         where #stage_mms_Product_History.s_mms_Product_hash != isnull(prior.s_mms_Product_hash, ''))x


-- Create the s_mms_product_1 records.
-- dv_batch_id is the dv_load_date_time converted to YYYYMMDDHHMISS
insert into dbo.s_mms_product_1 (
   
	   bk_hash,
       product_id,
       lt_buck_eligible,
	   lt_buck_cost_percent,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select 
       x.*
  from (select #stage_mms_Product_History.bk_hash,
       #stage_mms_Product_History.ProductID product_id,
	   #stage_mms_Product_History.LTBuckEligible lt_buck_eligible,
	   #stage_mms_Product_History.LTBuckCostPercent lt_buck_cost_percent,
               case when #stage_mms_Product_History.rank2 = 1 then
                         case when #stage_mms_Product_History.MMSInsertedDateTime is null then convert(datetime,'jan 1, 1980',107)
                              else #stage_mms_Product_History.MMSInsertedDateTime
                          end
                    when datepart(hh, isnull(#stage_mms_Product_History.MMSUpdatedDateTime,#stage_mms_Product_History.InsertedDateTime)) in (0, 1) 
					then dateadd(hh, 3, convert(datetime, convert(date, isnull(#stage_mms_Product_History.MMSUpdatedDateTime,#stage_mms_Product_History.InsertedDateTime))))
                    else dateadd(hh, 27, convert(datetime, convert(date, isnull(#stage_mms_Product_History.MMSUpdatedDateTime,#stage_mms_Product_History.InsertedDateTime))))
                end dv_load_date_time,
               case when #stage_mms_Product_History.rank2 = 1 then
                         case when #stage_mms_Product_History.MMSInsertedDateTime is null then 19800101000000
                              else replace(replace(replace(convert(varchar, #stage_mms_Product_History.MMSInsertedDateTime,120), '-', ''),' ', ''), ':', '')
                          end
                    when datepart(hh, isnull(#stage_mms_Product_History.MMSUpdatedDateTime,#stage_mms_Product_History.InsertedDateTime)) in (0, 1) 
					then replace(replace(replace(convert(varchar, dateadd(hh, 3, convert(datetime, convert(date, isnull(#stage_mms_Product_History.MMSUpdatedDateTime,#stage_mms_Product_History.InsertedDateTime)))),120), '-', ''),' ', ''), ':', '')
                    else replace(replace(replace(convert(varchar, dateadd(hh, 27, convert(datetime, convert(date, isnull(#stage_mms_Product_History.MMSUpdatedDateTime,#stage_mms_Product_History.InsertedDateTime)))),120), '-', ''),' ', ''), ':', '')
                end dv_batch_id,
               2 dv_r_load_source_id, 
               #stage_mms_Product_History.s_mms_Product_hash,
               getdate() dv_inserted_date_time, 
               suser_sname() dv_insert_user
                   from dbo.#stage_mms_Product_History
          left join dbo.#stage_mms_Product_History prior
            on #stage_mms_Product_History.ProductID = prior.ProductID
           and #stage_mms_Product_History.rank2 = prior.rank2 + 1
         where #stage_mms_Product_History.s_mms_Product_hash != isnull(prior.s_mms_Product_hash, ''))x


-- Populate the pit table
truncate table dbo.p_mms_product						
exec dbo.proc_p_mms_product @current_dv_batch_id = -1 

end
