CREATE PROC [sandbox].[proc_mart_sw_d_mms_member_usage_summary] AS  -- @min_batch_id bigint, @max_batch_id bigint AS

BEGIN

DECLARE @QueryRunDate datetime, @PriorMonthEndingDate datetime, @BeginDate datetime  --, @EndDate datetime;
DECLARE @CalendarYearStartingDate datetime, @UsageTransferStartingDate datetime, @UsageTransferEndingDate datetime;
DECLARE @TwelveMonthStartingDate datetime, @TwelveMonthEndingDate datetime;

SET @QueryRunDate = DATEADD(DD, DATEDIFF(DD, 0, GETDATE()), 0);
SET @PriorMonthEndingDate = DATEADD(DD, -1, DATEADD(MM, DATEDIFF(MM, 0, @QueryRunDate), 0));
SET @BeginDate = DATEADD(MM, -11, DATEADD(MM, DATEDIFF(MM, 0, @PriorMonthEndingDate), 0));
--SET @EndDate = @PriorMonthEndingDate --DATEADD(DD, 0, DATEADD(DD, DATEDIFF(DD, 0, @PriorMonthEndingDate), 0));
SET @CalendarYearStartingDate = DATEADD(yy, 0, DATEADD(yy, DATEDIFF(yy, 0, @PriorMonthEndingDate), 0));
SET @TwelveMonthStartingDate = DATEADD(MM, -11, DATEADD(MM, DATEDIFF(MM, 0, @PriorMonthEndingDate), 0));
SET @TwelveMonthEndingDate = DATEADD(dd, -1, DATEADD(mm, 1, DATEADD(mm, DATEDIFF(mm, 0, @PriorMonthEndingDate), 0)));

if MONTH(@PriorMonthEndingDate) < 9
  SET @UsageTransferStartingDate = DATEADD(yy, -1, @CalendarYearStartingDate);
else
  SET @UsageTransferStartingDate = @CalendarYearStartingDate;

SET @UsageTransferEndingDate = CONVERT(datetime, STR(YEAR(@UsageTransferStartingDate)) + '-11-30');

if @UsageTransferStartingDate < @BeginDate
  SET @BeginDate = @UsageTransferStartingDate;

SELECT DM.[member_id]
     , DC.[club_id]
     , DM.[membership_id]
     , [fourteen_day]         = SUM(CASE WHEN DD.[calendar_date] >= DATEADD(DD, -14 , DATEADD(DD, DATEDIFF(DD, 0, DDReport.[calendar_date]),0)) THEN 1 ELSE 0 END)
     , [thirty_day]           = SUM(CASE WHEN DD.[calendar_date] >= DATEADD(DD, -30 , DATEADD(DD, DATEDIFF(DD, 0, DDReport.[calendar_date]),0)) THEN 1 ELSE 0 END)
     , [sixty_day]            = SUM(CASE WHEN DD.[calendar_date] >= DATEADD(DD, -60 , DATEADD(DD, DATEDIFF(DD, 0, DDReport.[calendar_date]),0)) THEN 1 ELSE 0 END)
     , [ninety_day]           = SUM(CASE WHEN DD.[calendar_date] >= DATEADD(DD, -90 , DATEADD(DD, DATEDIFF(DD, 0, DDReport.[calendar_date]),0)) THEN 1 ELSE 0 END)
     , [one_twenty_day]       = SUM(CASE WHEN DD.[calendar_date] >= DATEADD(DD, -120, DATEADD(DD, DATEDIFF(DD, 0, DDReport.[calendar_date]),0)) THEN 1 ELSE 0 END)
     , [one_eighty_day]       = SUM(CASE WHEN DD.[calendar_date] >= DATEADD(DD, -180, DATEADD(DD, DATEDIFF(DD, 0, DDReport.[calendar_date]),0)) THEN 1 ELSE 0 END)
     , [mtd]                  = SUM(CASE WHEN DD.[calendar_date] >= DATEADD(MM, DATEDIFF(MM, 0, DDReport.[calendar_date]), 0) THEN 1 ELSE 0 END)
     , [one_month]            = SUM(CASE WHEN DD.[calendar_date] BETWEEN DDOne.[month_starting_date] AND DDOne.[month_ending_date] THEN 1 ELSE 0 END)
     , [two_month]            = SUM(CASE WHEN DD.[calendar_date] BETWEEN DDTwo.[month_starting_date] AND DDTwo.[month_ending_date] THEN 1 ELSE 0 END)
     , [three_month]          = SUM(CASE WHEN DD.[calendar_date] BETWEEN DDThree.[month_starting_date] AND DDThree.[month_ending_date] THEN 1 ELSE 0 END)
     , [twelve_month_total]   = SUM(CASE WHEN DD.[calendar_date] BETWEEN @TwelveMonthStartingDate AND @TwelveMonthEndingDate THEN 1 ELSE 0 END)
     , [calendar_year_total]  = SUM(CASE WHEN DD.[calendar_date] BETWEEN @CalendarYearStartingDate AND @PriorMonthEndingDate THEN 1 ELSE 0 END)
     , [usage_transfer_total] = SUM(CASE WHEN DD.[calendar_date] BETWEEN @UsageTransferStartingDate AND @UsageTransferEndingDate THEN 1 ELSE 0 END)
     , [last_swipe_date]      = MAX(DD.[calendar_date])
     , [dv_load_date_time]    = MAX(FMU.[dv_load_date_time])
     , [dv_batch_id]          = MAX(FMU.[dv_batch_id])
  FROM [dbo].[fact_mms_member_usage] FMU
       INNER JOIN [dbo].[d_mms_member] DM
         ON DM.[dim_mms_member_key] = FMU.[dim_mms_checkin_member_key]
       INNER JOIN [dbo].[d_mms_club] DC
         ON DC.[dim_club_key] = FMU.[dim_club_key]
       INNER JOIN [dbo].[dim_date] DD
         ON DD.[dim_date_key] = FMU.[check_in_dim_date_key]
       CROSS JOIN [dbo].[dim_date] DDReport
       INNER JOIN [dbo].[dim_date] DDOne
         ON DDOne.[dim_date_key] = DDReport.[prior_month_starting_dim_date_key]
       INNER JOIN [dbo].[dim_date] DDTwo
         ON DDTwo.[dim_date_key] = DDOne.[prior_month_starting_dim_date_key]
       INNER JOIN [dbo].[dim_date] DDThree
         ON DDThree.[dim_date_key] = DDTwo.[prior_month_starting_dim_date_key]
  WHERE DDReport.[calendar_date] = @QueryRunDate
    AND DD.calendar_date >= @BeginDate
    AND DD.calendar_date < @QueryRunDate
  GROUP BY DM.[membership_id], DM.[member_id], DC.[club_id];

END
