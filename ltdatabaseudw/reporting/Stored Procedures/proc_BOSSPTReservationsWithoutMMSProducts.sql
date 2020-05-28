CREATE PROC [reporting].[proc_BOSSPTReservationsWithoutMMSProducts] @StartDate [DATETIME],@EndDate [DATETIME],@DimLocationKeyList [VARCHAR](4000) AS

BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
   SET FMTONLY OFF
END

----- Sample Execution
 ---   Exec [reporting].[proc_BOSSPTReservationsWithoutMMSProducts] '10/01/2018','10/31/2018','151'

SET @StartDate = CASE WHEN @StartDate = 'Jan 1, 1900' 
                      THEN DATEADD(MONTH,DATEDIFF(MONTH,0,GETDATE()-1),0)    ----- returns 1st of yesterday's month
					  WHEN @StartDate = 'Dec 30, 1899'
					  THEN DATEADD(YEAR,DATEDIFF(YEAR,0,GETDATE()-1),0)      ----- returns 1st of yesterday's year
					  ELSE @StartDate END
SET @EndDate = CASE WHEN @EndDate = 'Jan 1, 1900' 
                    THEN CONVERT(DATETIME,CONVERT(VARCHAR,GETDATE()-1,101),101) 
					ELSE @EndDate END


DECLARE @StartDimDateKey INT,
        @EndtDimDateKey INT


SET @StartDimDateKey = (SELECT dim_date_key FROM [marketing].[v_dim_date] WHERE calendar_date = @StartDate)
SET @EndtDimDateKey = (SELECT dim_date_key FROM [marketing].[v_dim_date] WHERE calendar_date = @EndDate)

DECLARE @StartDimDate DATE = (Select calendar_date from [marketing].[v_dim_date] Where dim_date_key = @StartDimDateKey)
DECLARE @EndOfMonthDimDateKey INT = (SELECT month_ending_dim_date_key FROM [marketing].[v_dim_date] Where dim_date_key = @StartDimDateKey)
DECLARE @EarliestTransactionPOstDateKey INT = (select dim_date_key from  [marketing].[v_dim_date] where calendar_date = dateadd(month,-2,@StartDimDate))


--DECLARE @StartDimDateKey INT = '20191001'
--DECLARE @EndtDimDateKey INT  = '20191031' 
--DECLARE @DimLocationKeyList [VARCHAR](4000) = '239'


/* Parse the Passed Parameters */

 ----- Club Prompt ------  
IF OBJECT_ID('tempdb.dbo.#Clubs', 'U') IS NOT NULL  
  DROP TABLE #Clubs;

DECLARE @list_table VARCHAR(8000)
SET @list_table = 'club_list'

  EXEC marketing.proc_parse_pipe_list @DimLocationKeyList,@list_table


	
SELECT DimClub.dim_club_key AS DimClubKey, 
       DimClub.club_id, 
	   DimClub.club_name AS ClubName,
       DimClub.club_code,
	   DimClub.gl_club_id,
	   DimClub.local_currency_code AS LocalCurrencyCode,
	   MMSRegion.description AS MMSRegion,
	   PTRCLRegion.description AS PTRCLRegion,
	   MemberActivitiesRegion.description AS MemberActivitiesRegion
  INTO #Clubs   
  FROM [marketing].[v_dim_club] DimClub
  JOIN #club_list ClubKeyList
    ON ClubKeyList.Item = DimClub.club_id
	  OR ClubKeyList.Item = -1
  JOIN [marketing].[v_dim_description]  MMSRegion
   ON MMSRegion.dim_description_key = DimClub.region_dim_description_key 
  JOIN [marketing].[v_dim_description]  PTRCLRegion
   ON PTRCLRegion.dim_description_key = DimClub.pt_rcl_area_dim_description_key
  JOIN [marketing].[v_dim_description]  MemberActivitiesRegion
   ON MemberActivitiesRegion.dim_description_key = DimClub.member_activities_region_dim_description_key
WHERE DimClub.club_id Not In (-1,99,100)
  AND DimClub.club_id < 900
  AND DimClub.club_type = 'Club'
  AND (DimClub.club_close_dim_date_key in('-997','-998','-999')  OR DimClub.club_close_dim_date_key > @StartDimDateKey)  
GROUP BY DimClub.dim_club_key, 
       DimClub.club_id, 
	   DimClub.club_name,
       DimClub.club_code,
	   DimClub.gl_club_id,
	   DimClub.local_currency_code,
	   MMSRegion.description,
	   PTRCLRegion.description,
	   MemberActivitiesRegion.description



IF OBJECT_ID('tempdb.dbo.#BOSSReservation', 'U') IS NOT NULL
  DROP TABLE #BOSSReservation;
IF OBJECT_ID('tempdb.dbo.#BOSSMemberKeys', 'U') IS NOT NULL
  DROP TABLE #BOSSMemberKeys;
IF OBJECT_ID('tempdb.dbo.#BOSSMemberKeysMMSProducts', 'U') IS NOT NULL
  DROP TABLE #BOSSMemberKeysMMSProducts;
  IF OBJECT_ID('tempdb.dbo.#RecurrentProduct', 'U') IS NOT NULL
  DROP TABLE #RecurrentProduct;
IF OBJECT_ID('tempdb.dbo.#Finaltable', 'U') IS NOT NULL
  DROP TABLE #Finaltable;


/* Query to show all BOSS Reservations for the Selected Club and date range */
select
c.club_code,
c.club_name,
member.customer_name,
member.dim_mms_member_key,
boss.reservation_id,
boss.start_dim_date_key,
boss.end_dim_date_key,
bossp.product_description boss_product,
bossp.product_line boss_product_line,
bossp.sku boss_product_sku,
bossp.upc_code boss_upc_code,
Product.product_id,
Product.product_description mms_product,
Product.dim_mms_product_key
--fbdr.meeting_dim_date_key
INTO #BOSSReservation
from
marketing.v_dim_boss_reservation boss 
JOIN marketing.v_dim_boss_product bossp on boss.dim_boss_product_key = bossp.dim_boss_product_key
JOIN [marketing].[v_fact_boss_daily_roster] fbdr on boss.dim_boss_reservation_key = fbdr.dim_boss_reservation_key
JOIN marketing.v_dim_club c on boss.dim_club_key = c.dim_club_key
JOIN [marketing].v_dim_mms_member member ON fbdr.dim_mms_member_key = member.dim_mms_member_key
JOIN [marketing].[v_dim_mms_product] Product ON boss.dim_mms_product_key = Product.dim_mms_product_key
JOIN #Clubs  clist on c.dim_club_key = clist.DimClubKey
where fbdr.meeting_dim_date_key between @StartDimDateKey and @EndOfMonthDimDateKey
and bossp.sku like 'PT%'
and bossp.sku <> 'PT LEAD GENERATION'
group by c.club_code,
c.club_name,
member.customer_name,
member.dim_mms_member_key,
boss.reservation_id,
boss.start_dim_date_key,
boss.end_dim_date_key,
bossp.product_description,
bossp.product_line,
bossp.sku,
bossp.upc_code,
Product.product_id,
Product.product_description,
Product.dim_mms_product_key
;


/* Find the distinct list of Rostered Members and the MMS Product IDs associated with the Bookings they are part of */
Select dim_mms_member_key, dim_mms_product_key
INTO #BOSSMemberKeysMMSProducts
FROM #BOSSReservation
GROUP BY dim_mms_member_key,dim_mms_product_key

/* Find If the Member had bought the same MMS Product as associated in the Bookings that they are part of in BOSS */
Select 
@EarliestTransactionPOstDateKey as EarliestTransactionPOstDateKey,
c.club_code,
c.club_name,
member.customer_name, 
member.dim_mms_member_key,
Product.product_id,
Product.product_description,
Product.dim_mms_product_key,
TranItem.pos_flag,
TranItem.post_dim_date_key,
TranItem.sales_dollar_amount,
TranItem.item_lt_bucks_amount,
TranItem.sales_quantity
INTO #RecurrentProduct 
from [marketing].[v_fact_mms_transaction_item] TranItem
  JOIN [marketing].[v_dim_mms_product] Product ON TranItem.dim_mms_product_key = Product.dim_mms_product_key
  JOIN [marketing].[v_dim_club] c on TranItem.dim_club_key = c.dim_club_key
  JOIN [marketing].v_dim_mms_member member ON TranItem.dim_mms_member_key = member.dim_mms_member_key
  JOIN #BOSSMemberKeysMMSProducts BOSS ON TranItem.dim_mms_member_key = BOSS.dim_mms_member_key and TranItem.dim_mms_product_key = BOSS.dim_mms_product_key
Where 
1=1
AND Product.Reporting_division = 'Personal Training'
AND TranItem.post_dim_date_key < @EndOfMonthDimDateKey
AND TranItem.post_dim_date_key >= @EarliestTransactionPOstDateKey
;



--select * from #BOSSReservation order by customer_name, start_dim_date_key


--select * from #RecurrentProduct order by customer_name, post_dim_date_key

/* Query to find Reservations associated with Members who have not bought the associated MMS Product in Last 2 Months */
select a.* from #BOSSReservation a
where concat(a.dim_mms_member_key,a.dim_mms_product_key)  not in (select concat(p.dim_mms_member_key,p.dim_mms_product_key) from #RecurrentProduct p)
order by customer_name, start_dim_date_key


 
  END
