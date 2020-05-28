CREATE PROC [dbo].[proc_dim_revenue_allocation_rule] @in_year [varchar](4) AS

begin
set xact_abort on
set nocount on

/*Declare @in_year [varchar](4)*/
set @in_year =  cast(year(getdate()) as varchar)
declare @dv_inserted_date_time datetime = getdate()
declare @dv_insert_user varchar(50) = suser_sname()

IF OBJECT_ID('tempdb.dbo.#etl_step1', 'U') IS NOT NULL DROP TABLE #etl_step1;

SELECT DRAR.Revenue_Allocation_Rule_Name,
        DRAR.Revenue_Allocation_Rule_Name + ' - ' +cast(year(getdate())+1 as varchar) Revenue_Allocation_Rule_Set,
/*      DimLocationKey,*/
        NextYearRevenuePostingDimDate.Month_Starting_Dim_Date_Key Revenue_Posting_Month_Starting_Dim_Date_Key,
        NextYearRevenuePostingDimDate.Month_Ending_Dim_Date_Key Revenue_Posting_Month_Ending_Dim_Date_Key,
        NextYearRevenuePostingDimDate.Four_Digit_Year_Dash_Two_Digit_Month Revenue_Posting_Month_Four_Digit_Year_Dash_Two_Digit_Month,
        NextYearEarliestTransactionDimDate.Dim_Date_Key Earliest_Transaction_Dim_Date_Key,
        NextYearRevenuePostingDimDate.Month_Ending_Dim_Date_Key Latest_Transaction_Dim_Date_Key,/*WATCH FOR LEAP YEAR 2020 -- 51 UDW records returns for'20200229'*/
        DRAR.Revenue_From_Late_Transaction_Flag,
        DRAR.Ratio,
        DRAR.Accumulated_Ratio,
        Convert(DateTime,Convert(Varchar,GetDate(),101),101) Effective_Date,
        'Dec 31, 9999' Expiration_Date,
        /*'Y' Active_Ind,*/
        -1 Batch_ID,
        DRAR.One_Off_Rule_Flag
        , DRAR.club_id
        , DRAR.dim_club_key
        , DRAR.dim_revenue_allocation_rule_id
        , @dv_inserted_date_time dv_inserted_date_time
        , @dv_insert_user dv_insert_user
into #etl_step1
FROM dim_revenue_allocation_rule DRAR
        JOIN Dim_Date RevenuePostingDimDate
        ON DRAR.Revenue_Posting_Month_Starting_Dim_Date_Key = RevenuePostingDimDate.Dim_Date_Key
        JOIN Dim_Date NextYearRevenuePostingDimDate
        ON dateadd(yy,1,RevenuePostingDimDate.Calendar_Date) = NextYearRevenuePostingDimDate.Calendar_Date
        JOIN Dim_Date EarliestTransactionDimDate
        ON DRAR.Earliest_Transaction_Dim_Date_Key = EarliestTransactionDimDate.Dim_Date_Key
        JOIN Dim_Date NextYearEarliestTransactionDimDate
        ON dateadd(yy,1,EarliestTransactionDimDate.Calendar_Date) = NextYearEarliestTransactionDimDate.Calendar_Date
        WHERE Revenue_Allocation_Rule_Name <> 'sale month activity'
        AND Revenue_Allocation_Rule_Set LIKE '% - '+ @in_year
 
/*select * from #etl_step1*/



begin tran 

DELETE dbo.dim_revenue_allocation_rule WHERE Revenue_Allocation_Rule_Name <> 'sale month activity' AND Revenue_Allocation_Rule_Set LIKE '% - '+ cast(@in_year+1 as varchar)

insert into dim_revenue_allocation_rule (
       Revenue_Allocation_Rule_Name,
       Revenue_Allocation_Rule_Set,
/*     DimLocationKey,*/
       Revenue_Posting_Month_Starting_Dim_Date_Key,
       Revenue_Posting_Month_Ending_Dim_Date_Key,
       Revenue_Posting_Month_Four_Digit_Year_Dash_Two_Digit_Month,
       Earliest_Transaction_Dim_Date_Key,
       Latest_Transaction_Dim_Date_Key,
       Revenue_From_Late_Transaction_Flag,
       Ratio,
       Accumulated_Ratio,
       Effective_Date,
       Expiration_Date,
      /* Active_Ind,*/
       dv_batch_id,
       One_Off_Rule_Flag
       , club_id
       , dim_club_key
       , dim_revenue_allocation_rule_id
       , dv_inserted_date_time
       , dv_insert_user
)
   SELECT Revenue_Allocation_Rule_Name,
              Revenue_Allocation_Rule_Set,
/*            Dim_Location_Key,*/
              Revenue_Posting_Month_Starting_Dim_Date_Key,
              Revenue_Posting_Month_Ending_Dim_Date_Key,
              Revenue_Posting_Month_Four_Digit_Year_Dash_Two_Digit_Month,
              Earliest_Transaction_Dim_Date_Key,
              Latest_Transaction_Dim_Date_Key,/*WATCH FOR LEAP YEAR 2020 -- 51 UDW records returns for'20200229'*/
              Revenue_From_Late_Transaction_Flag,
              Ratio,
              Accumulated_Ratio,
              Convert(DateTime,Convert(Varchar,GetDate(),101),101) Effective_Date,
              'Dec 31, 9999' Expiration_Date,
            /*  'Y' Active_Ind,*/
              -1 Batch_ID
              , One_Off_Rule_Flag
              , club_id
              , dim_club_key
              , dim_revenue_allocation_rule_id
              , dv_inserted_date_time
              , dv_insert_user
FROM #etl_step1

commit tran
 
end

