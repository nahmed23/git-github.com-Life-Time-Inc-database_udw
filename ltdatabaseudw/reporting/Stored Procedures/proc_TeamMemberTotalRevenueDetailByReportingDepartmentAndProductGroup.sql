CREATE PROC [reporting].[proc_TeamMemberTotalRevenueDetailByReportingDepartmentAndProductGroup] @StartFourDigitYearDashTwoDigitMonth [Char](7),@EndFourDigitYearDashTwoDigitMonth [Char](7),@DepartmentMinDimReportingHierarchyKeyList [Varchar](8000),@DimEmployeeIDList [Varchar](4000) AS                    
BEGIN                     
SET XACT_ABORT ON                    
SET NOCOUNT ON                    
                    
IF 1=0 BEGIN                    
       SET FMTONLY OFF                    
     END                    
                  
                     
--DECLARE @StartFourDigitYearDashTwoDigitMonth Char(7) = '2018-02'                    
--DECLARE @EndFourDigitYearDashTwoDigitMonth Char(7) = '2018-02'                    
--DECLARE @DepartmentMinDimReportingHierarchyKeyList Varchar(8000) = 'All Departments'                    
--DECLARE @DimEmployeeIDList Varchar(4000) = '-998'                    

    


-- Use map_utc_time_zone_conversion to determine correct 'current' time, factoring in daylight savings / non daylight savings                    

DECLARE @ReportRunDateTime VARCHAR(21)  
                  
    SET @ReportRunDateTime = 
(SELECT Replace(Substring(convert(varchar,dateadd(hh,-1 * offset ,getdate()) ,100),1,6)                    
        +', '+Substring(convert(varchar,dateadd(hh,-1 * offset ,getdate()) ,100),8,10)                    
        +' '+Substring(convert(varchar,dateadd(hh,-1 * offset ,getdate()) ,100),18,2),'  ',' ')                     
        get_date_varchar 
   FROM map_utc_time_zone_conversion                    
  WHERE getdate() 
between utc_start_date_time 
    and utc_end_date_time 
	and description = 'central time')                    
                    
                    
DECLARE @StartMonthStartingDimDateKey INT  
                  
 SELECT @StartMonthStartingDimDateKey = DimDate.Month_Starting_Dim_Date_Key                    
   FROM [marketing].[v_dim_date] DimDate                    
  WHERE DimDate.Four_Digit_Year_Dash_Two_Digit_Month = @StartFourDigitYearDashTwoDigitMonth                    
    AND DimDate.Day_Number_In_Month = 1                    
                    
DECLARE @EndMonthStartingDimDateKey INT,                    
        @EndMonthEndingDimDateKey INT,                    
        @EndMonthEndingDate DATETIME          
                         
 SELECT @EndMonthStartingDimDateKey = DimDate.Month_Starting_Dim_Date_Key,                    
        @EndMonthEndingDimDateKey = DimDate.month_ending_dim_date_key,                    
        @EndMonthEndingDate = DimDate.month_ending_date               
   FROM [marketing].[v_dim_date] DimDate                    
  WHERE DimDate.Four_Digit_Year_Dash_Two_Digit_Month = @EndFourDigitYearDashTwoDigitMonth                    
    AND DimDate.Day_Number_In_Month = 1   
  
	Exec [reporting].[proc_DimReportingHierarchy_history] 
		 'N/A',
		 'N/A',                    
		 @DepartmentMinDimReportingHierarchyKeyList,
		 'N/A',
		 @StartMonthStartingDimDateKey,
		 @EndMonthEndingDimDateKey                     
   
     IF OBJECT_ID('tempdb.dbo.#DimReportingHierarchy_Prelim', 'U') IS NOT NULL               
       DROP TABLE #DimReportingHierarchy_Prelim;           
                      
      SELECT DimReportingHierarchyKey,
             DivisionName,              
             SubdivisionName,          
             DepartmentName,          
             ProductGroupName,          
             RegionType,          
             ReportRegionType,          
             CASE WHEN ProductGroupName IN('Weight Loss Challenges','90 Day Weight Loss')          
                  THEN 'Y'
                  ELSE 'N'
                   END PTDeferredRevenueProductGroupFlag     
       INTO #DimReportingHierarchy_Prelim                  
       FROM #OuterOutputTable       
	  
	                      
     IF OBJECT_ID('tempdb.dbo.#DepartmentGrouping', 'U') IS NOT NULL               
       DROP TABLE #DepartmentGrouping;                
                    
      SELECT MIN(DimReportingHierarchyKey) AS DepartmentMinDimReportingHierarchyKey,               
             DivisionName,         
             SubdivisionName,     
             DepartmentName     
        INTO #DepartmentGrouping               
		FROM #DimReportingHierarchy_Prelim               
	GROUP BY DivisionName,                   
             SubdivisionName,     
             DepartmentName     
                    
     IF OBJECT_ID('tempdb.dbo.#DimReportingHierarchy', 'U') IS NOT NULL               
       DROP TABLE #DimReportingHierarchy;                
                    
     SELECT  Prelim.DimReportingHierarchyKey,                 
             DeptGroup.DepartmentMinDimReportingHierarchyKey,          
             Prelim.DivisionName,              
             Prelim.SubdivisionName,          
             Prelim.DepartmentName,          
             Prelim.ProductGroupName,          
             Prelim.RegionType,          
             Prelim.ReportRegionType,          
             Prelim.PTDeferredRevenueProductGroupFlag          
		INTO #DimReportingHierarchy               
		FROM #DimReportingHierarchy_Prelim  Prelim               
		JOIN #DepartmentGrouping  DeptGroup               
		  ON Prelim.DivisionName = DeptGroup.DivisionName               
         AND Prelim.SubdivisionName = DeptGroup.SubdivisionName               
         AND Prelim.DepartmentName = DeptGroup.DepartmentName                                 

IF OBJECT_ID('tempdb.dbo.#DimEmployeeKeyList', 'U') IS NOT NULL                    
  DROP TABLE #DimEmployeeKeyList;                       
                    
DECLARE @list_table VARCHAR(100)                    
    SET @list_table = 'Employee_ID_list'                    
                    
   EXEC marketing.proc_parse_pipe_list @DimEmployeeIDList,@list_table                    
                    
SELECT  DISTINCT DimEmployee.dim_employee_key  AS DimEmployeeKey,
        DimEmployee.employee_id DimEmployeeID                    
  INTO #DimEmployeeKeyList                    
  FROM #Employee_ID_list EmployeeIDList  
  JOIN [marketing].[v_dim_Employee] DimEmployee
    ON EmployeeIDList.Item = DimEmployee.employee_id
                    
DECLARE @LocalCurrencyFlag CHAR(1),
        @ReportCurrencyCode VARCHAR(15)
 SELECT @LocalCurrencyFlag = CASE WHEN Count(*) = 1 
                                  THEN 'Y' 
								  ELSE 'N' 
								   END,
        @ReportCurrencyCode = CASE WHEN Count(*) = 1 
		                           THEN MIN(CurrencyDimEmployeeLocation.LocalCurrencyCode) 
								   ELSE 'USD' 
								    END
   FROM ( SELECT DISTINCT DimClub.Local_Currency_Code LocalCurrencyCode
			FROM #DimEmployeeKeyList
			JOIN marketing.v_Dim_Employee DimEmployee 
			  ON #DimEmployeeKeyList.DimEmployeeKey = DimEmployee.Dim_Employee_Key
            JOIN marketing.v_dim_club Dimclub 
		      ON DimEmployee.Dim_club_Key = DimClub.Dim_club_Key ) CurrencyDimEmployeeLocation

	                    
  ------ Created to set parameters for deferred E-comm sales of 60 Day challenge products                    
  ------ Rule set that challenge starts in the 2nd month of each quarter and if sales are made in the 1st month of the quarter                    
  ------ revenue is deferred to the 2nd month   
                   
DECLARE @FirstOfReportRangeDimDateKey INT                    
DECLARE @EndOfReportRangeDimDateKey   INT                    
    SET @FirstOfReportRangeDimDateKey = (SELECT MIN(dim_date_key) 
	                                       FROM [marketing].[v_dim_date]                     
                                          WHERE Four_Digit_Year_Dash_Two_Digit_Month = @StartFourDigitYearDashTwoDigitMonth)                    
    SET @EndOfReportRangeDimDateKey =   (SELECT MAX(dim_date_key) 
	                                       FROM [marketing].[v_dim_date]                     
                                          WHERE Four_Digit_Year_Dash_Two_Digit_Month = @EndFourDigitYearDashTwoDigitMonth)                    
                    
DECLARE @EComm60DayChallengeRevenueStartMonthStartDimDateKey INT        
            
  ---- When the requested month is the 2nd month of the quarter, set the start date to the prior month  
                    
	SET @EComm60DayChallengeRevenueStartMonthStartDimDateKey =  
	    (SELECT CASE 
				WHEN (SELECT Month_Number_In_Year                     
						FROM [marketing].[v_dim_date]                     
						WHERE dim_date_key = @FirstOfReportRangeDimDateKey) 
					in (2,5,8,11)
				THEN (SELECT Prior_Month_Starting_Dim_Date_Key     
						FROM [marketing].[v_dim_date]      
						WHERE dim_date_key = @FirstOfReportRangeDimDateKey)     
				ELSE (SELECT Month_Starting_Dim_Date_Key                    
						FROM [marketing].[v_dim_date]                     
						WHERE dim_date_key = @FirstOfReportRangeDimDateKey)
				 END     
          FROM [marketing].[v_dim_date]                    
         WHERE dim_date_key = @FirstOfReportRangeDimDateKey)  ---- to limit result set to one record                    
                    
                    
DECLARE @EComm60DayChallengeRevenueEndMonthEndDimDateKey INT      
              
  ---- When the requested month is the 1st month of the quarter, set the end date to the prior month 
                     
    SET @EComm60DayChallengeRevenueEndMonthEndDimDateKey = 
	(SELECT CASE 
	        WHEN (SELECT Month_Number_In_Year                     
                    FROM [marketing].[v_dim_date]                     
                   WHERE dim_date_key = @EndOfReportRangeDimDateKey) 
			  in (1,4,7,10)
            THEN (SELECT Prior_Month_Ending_Dim_Date_Key     
                    FROM [marketing].[v_dim_date]      
                   WHERE dim_date_key = @EndOfReportRangeDimDateKey)     
            ELSE (SELECT month_ending_dim_date_key                    
                    FROM [marketing].[v_dim_date]                     
                   WHERE dim_date_key = @EndOfReportRangeDimDateKey)
             END      
            FROM [marketing].[v_dim_date]                    
           WHERE dim_date_key = @FirstOfReportRangeDimDateKey)  ---- to limit result set to one record                    
                
				
			SELECT  ( CASE
					  WHEN FactAllocatedRevenue.sales_source = 'Cafe'                    
					  THEN DimReportingHierarchy_Cafe.DepartmentName           
					  WHEN FactAllocatedRevenue.sales_source = 'Hybris'     
					  THEN DimReportingHierarchy_Hybris.DepartmentName     
					  WHEN FactAllocatedRevenue.sales_source = 'HealthCheckUSA'       
					  THEN DimReportingHierarchy_HealthCheckUSA.DepartmentName     
					  WHEN FactAllocatedRevenue.sales_source = 'MMS'     
					  THEN DimReportingHierarchy_MMS.DepartmentName     
					  WHEN FactAllocatedRevenue.sales_source = 'Magento'         
					  THEN DimReportingHierarchy_Magento.DepartmentName                    
					   END  )   RevenueReportingDepartmentName,     
					( CASE 
					  WHEN FactAllocatedRevenue.sales_source = 'Cafe'                    
					  THEN DimReportingHierarchy_Cafe.ProductGroupName           
					  WHEN FactAllocatedRevenue.sales_source = 'Hybris'     
					  THEN DimReportingHierarchy_Hybris.ProductGroupName     
					  WHEN FactAllocatedRevenue.sales_source = 'HealthCheckUSA'       
					  THEN DimReportingHierarchy_HealthCheckUSA.ProductGroupName     
					  WHEN FactAllocatedRevenue.sales_source = 'MMS'     
					  THEN DimReportingHierarchy_MMS.ProductGroupName     
					  WHEN FactAllocatedRevenue.sales_source = 'Magento'         
					  THEN DimReportingHierarchy_Magento.ProductGroupName                    
					  END ) RevenueProductGroup,     
     			(DimDate.Standard_Date_Name) TransactionPostingDate,                    
				(CASE WHEN DimLocation.Club_Code != '' 
					  THEN DimLocation.Club_Code 
					  ELSE DimLocation.Club_Name 
					   END) ClubCode,                    
				(DimCustomer.Member_ID) MemberID,                    
				(DimCustomer.customer_name_last_first) MemberName,                    
   				( CASE WHEN FactAllocatedRevenue.sales_source = 'Cafe'                    
					   THEN DimCafeProduct.menu_item_name          
					   WHEN FactAllocatedRevenue.sales_source = 'Hybris'     
					   THEN DimHybrisProduct.name     
					   WHEN FactAllocatedRevenue.sales_source = 'HealthCheckUSA'       
					   THEN DimHealthCheckUSAProduct.Product_Description     
					   WHEN FactAllocatedRevenue.sales_source = 'MMS'     
					   THEN DimMMSProduct.Product_Description     
					   WHEN FactAllocatedRevenue.sales_source = 'Magento'         
					   THEN DimMagentoProduct.product_name                    
						END ) ProductDescription,     
				FactAllocatedRevenue.sales_source			SalesSource,               
				(FactAllocatedRevenue.transaction_amount)	LocalCurrencySaleAmount,                    
   				(FactAllocatedRevenue.allocated_quantity)	SaleQuantity,                    
				(FactAllocatedRevenue.allocated_amount)		LocalCurrencyRevenueAmount,                      
				(FactAllocatedRevenue.allocated_amount)		ReportCurrencyRevenueAmount,                    
  				(FactAllocatedRevenue.Allocated_Quantity)	RevenueQuantity,                    
     			(DimLocation.Local_Currency_Code)			LocalCurrencyCode,                    
				'Local Currency'							ReportCurrencyCode,                    
				(DimEmployee.Last_Name 
				+ ', ' 
				+ DimEmployee.First_Name 
				+ ' - ' 
				+ EmployeeDimLocation.Club_Name)			EmployeeNameLastFirstDashClubName,                    
				NULL										RevenueReportingDepartmentNameCommaList,                           
				@ReportRunDateTime							ReportRunDateTime,                    
				(EmployeeDimLocation.Club_Name)				EmployeeHomeClub,                    
				(DimEmployee.Last_Name)						EmployeeLastName,                    
				(DimEmployee.First_Name)					EmployeeFirstName,                    
				(FactAllocatedRevenue.transaction_dim_date_key) TransactionPostDimDateKey,                    
				(DimCustomer.Last_Name)						MemberLastName,                    
				(DimCustomer.First_Name)					MemberFirstName,                    
				NULL										SoldNotServicedFlag,        
				0											LocalCurrencyCorporateTransferAmount,                    
				0											CorporateTransferAmount,                    
				NULL										HeaderDivisionList,                    
				NULL										HeaderSubDivisionList               
		   FROM [marketing].[v_fact_combined_allocated_transaction_item] FactAllocatedRevenue                    
      LEFT JOIN [marketing].[v_dim_cafe_product_history] DimCafeProduct                    
             ON FactAllocatedRevenue.dim_product_key = DimCafeProduct.dim_cafe_product_key                    
			AND FactAllocatedRevenue.sales_source = 'Cafe'               
		    AND DimCafeProduct.effective_date_time <= @EndMonthEndingDate               
		    AND DimCafeProduct.expiration_date_time > @EndMonthEndingDate               
	  LEFT JOIN [marketing].[v_dim_hybris_product_history] DimHybrisProduct                    
             ON FactAllocatedRevenue.dim_product_key = DimHybrisProduct.dim_hybris_product_key                    
			AND FactAllocatedRevenue.sales_source = 'Hybris'               
			AND DimHybrisProduct.effective_date_time <= @EndMonthEndingDate               
			AND DimHybrisProduct.expiration_date_time > @EndMonthEndingDate               
	  LEFT JOIN [marketing].[v_dim_healthcheckusa_product_history] DimHealthCheckUSAProduct                    
		     ON FactAllocatedRevenue.dim_product_key = DimHealthCheckUSAProduct.dim_healthcheckusa_product_key                    
			AND FactAllocatedRevenue.sales_source = 'HealthCheckUSA'               
			AND DimHealthCheckUSAProduct.effective_date_time <= @EndMonthEndingDate               
			AND DimHealthCheckUSAProduct.expiration_date_time > @EndMonthEndingDate               
	  LEFT JOIN [marketing].[v_dim_mms_product_history] DimMMSProduct                    
		     ON FactAllocatedRevenue.dim_product_key = DimMMSProduct.dim_mms_product_key                    
		    AND FactAllocatedRevenue.sales_source = 'MMS'               
			AND DimMMSProduct.effective_date_time <= @EndMonthEndingDate               
			AND DimMMSProduct.expiration_date_time > @EndMonthEndingDate               
	  LEFT JOIN [marketing].[v_dim_magento_product_history] DimMagentoProduct                         
			 ON FactAllocatedRevenue.dim_product_key = DimMagentoProduct.dim_magento_product_key                    
			AND FactAllocatedRevenue.sales_source = 'Magento'               
			AND DimMagentoProduct.effective_date_time <= @EndMonthEndingDate               
			AND DimMagentoProduct.expiration_date_time > @EndMonthEndingDate               
	  LEFT JOIN #DimReportingHierarchy DimReportingHierarchy_Cafe                    
		     ON DimCafeProduct.dim_reporting_hierarchy_key = DimReportingHierarchy_Cafe.DimReportingHierarchyKey                     
	  LEFT JOIN #DimReportingHierarchy DimReportingHierarchy_Hybris                    
		     ON DimHybrisProduct.dim_reporting_hierarchy_key = DimReportingHierarchy_Hybris.DimReportingHierarchyKey                    
		    AND DimReportingHierarchy_Hybris.PTDeferredRevenueProductGroupFlag = 'N'                  
	  LEFT JOIN #DimReportingHierarchy DimReportingHierarchy_HealthCheckUSA                    
		     ON DimHealthCheckUSAProduct.dim_reporting_hierarchy_key = DimReportingHierarchy_HealthCheckUSA.DimReportingHierarchyKey                    
		    AND DimReportingHierarchy_HealthCheckUSA.PTDeferredRevenueProductGroupFlag = 'N'                 
	  LEFT JOIN #DimReportingHierarchy DimReportingHierarchy_MMS                    
		     ON DimMMSProduct.dim_reporting_hierarchy_key = DimReportingHierarchy_MMS.DimReportingHierarchyKey                     
	  LEFT JOIN #DimReportingHierarchy DimReportingHierarchy_Magento                    
		     ON DimMagentoProduct.dim_reporting_hierarchy_key = DimReportingHierarchy_Magento.DimReportingHierarchyKey                    
            AND DimReportingHierarchy_Magento.PTDeferredRevenueProductGroupFlag = 'N'                   
		   JOIN #DimEmployeeKeyList                     
			 ON FactAllocatedRevenue.primary_sales_dim_employee_key = #DimEmployeeKeyList.DimEmployeeKey                    
		   JOIN [marketing].[v_dim_date] DimDate                    
			 ON FactAllocatedRevenue.transaction_dim_date_key = DimDate.dim_date_key                    
		   JOIN [marketing].[v_dim_club] DimLocation                    
			 ON FactAllocatedRevenue.allocated_dim_club_key = DimLocation.dim_club_key                    
		   JOIN [marketing].[v_dim_mms_member] DimCustomer                    
			 ON FactAllocatedRevenue.dim_mms_member_key = DimCustomer.dim_mms_member_key                    
		   JOIN [marketing].[v_dim_employee] DimEmployee                    
		   	 ON FactAllocatedRevenue.primary_sales_dim_employee_key = DimEmployee.dim_employee_key                    
		   JOIN [marketing].[v_dim_club] EmployeeDimLocation                    
			 ON DimEmployee.Dim_Club_Key = EmployeeDimLocation.Dim_Club_Key                    
		  WHERE FactAllocatedRevenue.allocated_month_starting_dim_date_key >= @StartMonthStartingDimDateKey
			AND FactAllocatedRevenue.allocated_month_starting_dim_date_key <= @EndMonthStartingDimDateKey


      UNION ALL

      SELECT (CASE WHEN FactAllocatedRevenue.sales_source = 'Hybris'                    
                   THEN DimReportingHierarchy_Hybris.DepartmentName     
                   WHEN FactAllocatedRevenue.sales_source = 'HealthCheckUSA'       
                   THEN DimReportingHierarchy_HealthCheckUSA.DepartmentName     
			   	   WHEN FactAllocatedRevenue.sales_source = 'Magento'         
				   THEN DimReportingHierarchy_Magento.DepartmentName                    
				    END  ) RevenueReportingDepartmentName,     
             (CASE WHEN FactAllocatedRevenue.sales_source = 'Hybris'                    
                   THEN DimReportingHierarchy_Hybris.ProductGroupName     
                   WHEN FactAllocatedRevenue.sales_source = 'HealthCheckUSA'       
                   THEN DimReportingHierarchy_HealthCheckUSA.ProductGroupName     
                   WHEN FactAllocatedRevenue.sales_source = 'Magento'         
                   THEN DimReportingHierarchy_Magento.ProductGroupName                    
                   END) RevenueProductGroup,     
			(DimDate.Standard_Date_Name) TransactionPostingDate,                    
			(CASE WHEN DimLocation.Club_Code != '' 
			      THEN DimLocation.Club_Code 
				  ELSE DimLocation.Club_Name 
				   END) ClubCode,                    
			(DimCustomer.Member_ID) MemberID,                    
			(DimCustomer.customer_name_last_first) MemberName,                    
			(CASE WHEN FactAllocatedRevenue.sales_source = 'Hybris'                    
				  THEN DimHybrisProduct.name     
				  WHEN FactAllocatedRevenue.sales_source = 'HealthCheckUSA'       
				  THEN DimHealthCheckUSAProduct.Product_Description     
				  WHEN FactAllocatedRevenue.sales_source = 'Magento'         
				  THEN DimMagentoProduct.product_name                    
				   END) ProductDescription,     
			FactAllocatedRevenue.sales_source                  SalesSource,                         
		   (FactAllocatedRevenue.transaction_amount)           LocalCurrencySaleAmount,                    
		   (FactAllocatedRevenue.allocated_quantity)           SaleQuantity,                           
    	   (FactAllocatedRevenue.allocated_amount)             LocalCurrencyRevenueAmount,                      
      	   (FactAllocatedRevenue.allocated_amount)             ReportCurrencyRevenueAmount,                    
     	   (FactAllocatedRevenue.Allocated_Quantity)           RevenueQuantity,                    
		   (DimLocation.Local_Currency_Code)                   LocalCurrencyCode,                    
		   'Local Currency'                                    ReportCurrencyCode,                    
		   (DimEmployee.Last_Name 
		   + ', ' 
		   + DimEmployee.First_Name 
		   + ' - ' 
		   + EmployeeDimLocation.Club_Name)                    EmployeeNameLastFirstDashClubName,                    
		   NULL                                                RevenueReportingDepartmentNameCommaList,                           
		   @ReportRunDateTime                                  ReportRunDateTime,                    
		   (EmployeeDimLocation.Club_Name)                     EmployeeHomeClub,                    
		   (DimEmployee.Last_Name)                             EmployeeLastName,                    
		   (DimEmployee.First_Name)                            EmployeeFirstName,                    
		   (FactAllocatedRevenue.transaction_dim_date_key)     TransactionPostDimDateKey,                    
		   (DimCustomer.Last_Name)                             MemberLastName,                    
		   (DimCustomer.First_Name)                            MemberFirstName,                    
		   NULL                                                SoldNotServicedFlag,                    
		   0                                                   LocalCurrencyCorporateTransferAmount,                    
		   0                                                   CorporateTransferAmount,                    
		   NULL AS                                             HeaderDivisionList,                          
		   NULL AS                                             HeaderSubDivisionList                    
	  FROM [marketing].[v_fact_combined_allocated_transaction_item] FactAllocatedRevenue                    
 LEFT JOIN [marketing].[v_dim_hybris_product_history] DimHybrisProduct                    
		ON FactAllocatedRevenue.dim_product_key = DimHybrisProduct.dim_hybris_product_key                    
       AND FactAllocatedRevenue.sales_source = 'Hybris'               
       AND DimHybrisProduct.effective_date_time <= @EndMonthEndingDate               
       AND DimHybrisProduct.expiration_date_time > @EndMonthEndingDate               
 LEFT JOIN [marketing].[v_dim_healthcheckusa_product_history] DimHealthCheckUSAProduct                    
        ON FactAllocatedRevenue.dim_product_key = DimHealthCheckUSAProduct.dim_healthcheckusa_product_key                    
       AND FactAllocatedRevenue.sales_source = 'HealthCheckUSA'               
       AND DimHealthCheckUSAProduct.effective_date_time <= @EndMonthEndingDate               
       AND DimHealthCheckUSAProduct.expiration_date_time > @EndMonthEndingDate               
 LEFT JOIN [marketing].[v_dim_magento_product_history] DimMagentoProduct                         
        ON FactAllocatedRevenue.dim_product_key = DimMagentoProduct.dim_magento_product_key                    
       AND FactAllocatedRevenue.sales_source = 'Magento'               
       AND DimMagentoProduct.effective_date_time <= @EndMonthEndingDate               
       AND DimMagentoProduct.expiration_date_time > @EndMonthEndingDate                  
 LEFT JOIN #DimReportingHierarchy DimReportingHierarchy_Hybris                    
        ON DimHybrisProduct.dim_reporting_hierarchy_key = DimReportingHierarchy_Hybris.DimReportingHierarchyKey                          
 LEFT JOIN #DimReportingHierarchy DimReportingHierarchy_HealthCheckUSA                    
        ON DimHealthCheckUSAProduct.dim_reporting_hierarchy_key = DimReportingHierarchy_HealthCheckUSA.DimReportingHierarchyKey                    
 LEFT JOIN #DimReportingHierarchy DimReportingHierarchy_Magento                    
        ON DimMagentoProduct.dim_reporting_hierarchy_key = DimReportingHierarchy_Magento.DimReportingHierarchyKey                    
      JOIN #DimEmployeeKeyList                     
		ON FactAllocatedRevenue.primary_sales_dim_employee_key = #DimEmployeeKeyList.DimEmployeeKey                    
	  JOIN [marketing].[v_dim_date] DimDate                    
		ON FactAllocatedRevenue.transaction_dim_date_key = DimDate.dim_date_key                    
	  JOIN [marketing].[v_dim_club] DimLocation                    
		ON FactAllocatedRevenue.allocated_dim_club_key = DimLocation.dim_club_key                    
	  JOIN [marketing].[v_dim_mms_member] DimCustomer                    
		ON FactAllocatedRevenue.dim_mms_member_key = DimCustomer.dim_mms_member_key                    
	  JOIN [marketing].[v_dim_employee] DimEmployee                    
		ON FactAllocatedRevenue.primary_sales_dim_employee_key = DimEmployee.dim_employee_key                    
	  JOIN [marketing].[v_dim_club] EmployeeDimLocation                    
		ON DimEmployee.Dim_Club_Key = EmployeeDimLocation.Dim_Club_Key                    
	 WHERE (FactAllocatedRevenue.allocated_month_starting_dim_date_key >= @EComm60DayChallengeRevenueStartMonthStartDimDateKey                    
	   AND FactAllocatedRevenue.allocated_month_starting_dim_date_key  <= @EComm60DayChallengeRevenueEndMonthEndDimDateKey)                    
	   AND FactAllocatedRevenue.sales_source in('Hybris','HealthCheckUSA','Magento')                    
	   AND ( DimReportingHierarchy_Hybris.PTDeferredRevenueProductGroupFlag = 'Y'                
	    OR DimReportingHierarchy_HealthCheckUSA.PTDeferredRevenueProductGroupFlag = 'Y'
	    OR DimReportingHierarchy_Magento.PTDeferredRevenueProductGroupFlag = 'Y'  )

DROP TABLE #DimEmployeeKeyList
DROP TABLE #DimReportingHierarchy

END


