CREATE PROC [sandbox].[proc_mart_sw_d_udw_membership_sales_transaction_summary] AS  -- @min_batch_id bigint, @max_batch_id bigint AS

BEGIN

DECLARE @ReportDate datetime, @StartingDate datetime, @EndingDate datetime;
SET @ReportDate = DATEADD(dd, -1, DATEADD(mm, DATEDIFF(mm, 0, GETDATE()), 0));
SET @StartingDate = DATEADD(mm, -11, DATEADD(mm, DATEDIFF(mm, 0, @ReportDate), 0));
SET @EndingDate = @ReportDate; --DATEADD(dd, 0, DATEADD(dd, DATEDIFF(dd, 0, @ReportDate), 0));

SELECT FST.[membership_id]
     , DD_Post.[month_starting_date]
     , Prd.[product_id]
     , [amount] = SUM(FST.[sales_dollar_amount])
  FROM [dbo].[fact_mms_sales_transaction_item] FST
       INNER JOIN [dbo].[d_mms_product] Prd
         ON Prd.dim_mms_product_key = FST.dim_mms_product_key
       INNER JOIN [dbo].[d_mms_membership] Mbrs
         ON Mbrs.[membership_id] = FST.[membership_id]
       INNER JOIN [dbo].[d_mms_member] Mbr
         ON Mbr.[membership_id] = FST.[membership_id]
            AND Mbr.[member_active_flag] = 'Y'
            AND Mbr.[val_member_type_id] = 1
       INNER JOIN [dbo].[dim_date] DD_Post
         ON DD_Post.[dim_date_key] = FST.[post_dim_date_key]
  WHERE FST.[active_transaction_flag] = 'Y'
    AND FST.[voided_flag] = 'N'
    AND FST.[transaction_edited_flag] = 'N'
    AND FST.[reversal_flag] = 'N'
    AND FST.[refund_flag] = 'N'
    AND FST.[membership_adjustment_flag] = 'N'
    AND FST.[automated_refund_flag] = 'N'
    AND FST.[membership_id] > 0
    AND FST.[sales_dollar_amount] > 0
    AND Mbrs.val_membership_status_id <> 1
    AND DD_Post.[calendar_date] >= @StartingDate
    AND DD_Post.[calendar_date] <= @EndingDate
    AND DD_Post.[calendar_date] >= Mbr.[join_date]
  GROUP BY FST.[membership_id], DD_Post.[month_starting_date], Prd.[product_id];

END
