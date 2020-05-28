CREATE VIEW [reporting].[v_corp_membership_spend] AS SELECT
MM.membership_id
,MM.member_id
,MM.dim_mms_membership_key
,MM.dim_mms_member_key
,FMMS.total_spend_amount
,ISNULL(TotalDues.TotalLTDues,0) 'total_dues_amount'
,FMMS.Total_spend_amount - ISNULL(TotalDues.TotalLTDues,0) as 'amount_spend_minus_dues'
,FMMS.[last_12_month_spend_amount] 'last_12_month_spend_amount'
,ISNULL(TotalLast12MonthDues.LT12MonthDues,0) 'last_12_month_dues_amount'
,FMMS.[last_12_month_spend_amount] - ISNULL(TotalLast12MonthDues.LT12MonthDues,0) as 'last_12_month_spend_minus_dues'
,MM.[customer_name]
,MM.[join_date]
,(CAST(CONVERT(CHAR(8),getdate(),112) AS INT) -  CAST(CONVERT(CHAR(8),MM.[join_date_key], 112) AS INT)) / 10000 'membership_tenure'
,corpCodes.[company_name]
,corpCodes.usage_report_member_type
,MM.[description_member]
,ClubLocation.state 'home_club_state'
,ClubLocation.club_name 'home_club'
,ClubLocation.marketing_club_level 'club_membership_level'
,Membership.[membership_type]
,Membership.[membership_status]
,MM.[gender_abbreviation] 'primary_member_gender'
,MM.[member_active_flag] 'primary_member_active_flag'


,(CAST(CONVERT(CHAR(8),getdate(),112) AS INT) - CAST(CONVERT(CHAR(8), MM.[date_of_birth], 112) AS INT)) / 10000 'primary_member_age'
,CASE WHEN (CAST(CONVERT(CHAR(8),getdate(),112) AS INT) - CAST(CONVERT(CHAR(8), MM.[date_of_birth], 112) AS INT)) / 10000 < 20 THEN '< 20'
		WHEN (CAST(CONVERT(CHAR(8),getdate(),112) AS INT) - CAST(CONVERT(CHAR(8), MM.[date_of_birth], 112) AS INT)) / 10000 > 19 
			  AND (CAST(CONVERT(CHAR(8),getdate(),112) AS INT) - CAST(CONVERT(CHAR(8), MM.[date_of_birth], 112) AS INT)) / 10000 < 30 THEN '20-29'
		WHEN (CAST(CONVERT(CHAR(8),getdate(),112) AS INT) - CAST(CONVERT(CHAR(8), MM.[date_of_birth], 112) AS INT)) / 10000 > 29
			  AND (CAST(CONVERT(CHAR(8),getdate(),112) AS INT) - CAST(CONVERT(CHAR(8), MM.[date_of_birth], 112) AS INT)) / 10000 < 40 THEN '30-39'
		WHEN (CAST(CONVERT(CHAR(8),getdate(),112) AS INT) - CAST(CONVERT(CHAR(8), MM.[date_of_birth], 112) AS INT)) / 10000 > 39
			  AND (CAST(CONVERT(CHAR(8),getdate(),112) AS INT) - CAST(CONVERT(CHAR(8), MM.[date_of_birth], 112) AS INT)) / 10000 < 50 THEN '40-49'
		WHEN (CAST(CONVERT(CHAR(8),getdate(),112) AS INT) - CAST(CONVERT(CHAR(8), MM.[date_of_birth], 112) AS INT)) / 10000 > 49 
			  AND (CAST(CONVERT(CHAR(8),getdate(),112) AS INT) - CAST(CONVERT(CHAR(8), MM.[date_of_birth], 112) AS INT)) / 10000 < 60 THEN '50-59'
		WHEN (CAST(CONVERT(CHAR(8),getdate(),112) AS INT) - CAST(CONVERT(CHAR(8), MM.[date_of_birth], 112) AS INT)) / 10000 > 59
			  AND (CAST(CONVERT(CHAR(8),getdate(),112) AS INT) - CAST(CONVERT(CHAR(8), MM.[date_of_birth], 112) AS INT)) / 10000 < 70 THEN '60-69'
		ELSE '70 +' END 'primary_member_age_segment'
	
,CASE WHEN SUBSTRING(CONVERT(char(8),MM.Date_Of_Birth, 112), 1,4) < 1946 THEN 'Silent Generation (Born 1945 and before)'
	  WHEN SUBSTRING(CONVERT(char(8),MM.Date_Of_Birth, 112), 1,4) > 1945 
			AND SUBSTRING(CONVERT(char(8),MM.Date_Of_Birth, 112), 1,4) < 1965 THEN 'Baby Boomers (Born 1946 to 1964)'
	  WHEN SUBSTRING(CONVERT(char(8),MM.Date_Of_Birth, 112), 1,4) > 1964
			AND SUBSTRING(CONVERT(char(8),MM.Date_Of_Birth, 112), 1,4) < 1981 THEN 'Generation X (Born 1965 to 1980)'
	  WHEN SUBSTRING(CONVERT(char(8),MM.Date_Of_Birth, 112), 1,4) > 1980 
			AND SUBSTRING(CONVERT(char(8),MM.Date_Of_Birth, 112), 1,4) < 1997 THEN 'Millennials (Born 1981 to 1996)'
	  ELSE 'Post-Millennials (Born 1997 and onward)' END 'primary_member_age_generation'

,CASE WHEN SUBSTRING(CONVERT(char(8),MM.Date_Of_Birth, 112), 1,4) < 1946 THEN 1
	  WHEN SUBSTRING(CONVERT(char(8),MM.Date_Of_Birth, 112), 1,4) > 1945 
			AND SUBSTRING(CONVERT(char(8),MM.Date_Of_Birth, 112), 1,4) < 1965 THEN 2
	  WHEN SUBSTRING(CONVERT(char(8),MM.Date_Of_Birth, 112), 1,4) > 1964
			AND SUBSTRING(CONVERT(char(8),MM.Date_Of_Birth, 112), 1,4) < 1981 THEN 3
	  WHEN SUBSTRING(CONVERT(char(8),MM.Date_Of_Birth, 112), 1,4) > 1980 
			AND SUBSTRING(CONVERT(char(8),MM.Date_Of_Birth, 112), 1,4) < 1997 THEN 4
	  ELSE 5 END 'primary_member_age_generation_sort'
, 'Medicare' As 'spend_bucket'
,membership.membership_activation_date

FROM [marketing].[v_fact_mms_member_spend] FMMS
JOIN [marketing].[v_dim_mms_member] MM
	ON FMMS.[dim_mms_member_key] = MM.[dim_mms_member_key]
JOIN [marketing].[v_dim_mms_membership] Membership
	ON FMMS.[dim_mms_membership_key] =  Membership.[dim_mms_membership_key]

JOIN [marketing].[v_fact_mms_member_reimbursement_program] memberReimb
	ON MM.[dim_mms_member_key] = memberReimb.[dim_mms_member_key] AND memberReimb.termination_date >= GETDATE()

JOIN [marketing].[v_dim_mms_company] corpCodes
	ON memberReimb.[dim_mms_company_key] = corpCodes.[dim_mms_company_key]
JOIN [marketing].[v_dim_club] ClubLocation
	ON Membership.[home_dim_club_key] = ClubLocation.[Dim_Club_Key]
LEFT JOIN	(   --Calculating Monthly Dues All Time
		SELECT TI.dim_mms_member_key
		,SUM(TI.sales_dollar_amount) TotalLTDues
		,COUNT(TI.dim_mms_member_key) AllDuesAssessed 
		FROM marketing.v_fact_mms_transaction_item TI
		JOIN [marketing].[v_dim_mms_transaction_reason] DimTransactionReason 
		ON TI.dim_mms_transaction_reason_key =   DimTransactionReason.dim_mms_transaction_reason_key
		WHERE DimTransactionReason.reason_code_id = 28 
		GROUP BY TI.dim_mms_member_key
		) TotalDues ON TotalDues.dim_mms_member_key=FMMS.dim_mms_member_key
LEFT JOIN	(  --Calculating Monthly Dues within the Past 12 months
		SELECT TI.dim_mms_member_key
		,ISNULL(SUM(TI.sales_dollar_amount),0) LT12MonthDues
		,ISNULL(COUNT(TI.dim_mms_member_key),0) Last12MonthDuesAssessed
		FROM marketing.v_fact_mms_transaction_item TI
		JOIN [marketing].[v_dim_mms_transaction_reason] DimTransactionReason 
		ON TI.dim_mms_transaction_reason_key =   DimTransactionReason.dim_mms_transaction_reason_key
		LEFT JOIN marketing.v_dim_date D
		ON TI.post_dim_date_key = D.dim_date_key
		WHERE DimTransactionReason.reason_code_id = 28 
		AND DATEDIFF(MM,D.calendar_date,GETDATE()) < 12
		GROUP BY TI.dim_mms_member_key
		) TotalLast12MonthDues ON TotalLast12MonthDues.dim_mms_member_key=FMMS.dim_mms_member_key


WHERE corpCodes.corporate_code in ('73405', '75949', '87379', '84452')
 AND MM.[description_member] = 'Primary'
 AND MM.Last_Name <> 'House Account'
 AND membership.membership_status = 'Active'


UNION



----------------------------------------------Other Reimbursement Programs Membership Spend------------------------------------------

SELECT
MM.membership_id
,MM.member_id
,MM.dim_mms_membership_key
,MM.dim_mms_member_key
,FMMS.total_spend_amount
,ISNULL(TotalDues.TotalLTDues,0) 'total_dues_amount'
,FMMS.Total_spend_amount - ISNULL(TotalDues.TotalLTDues,0) as 'amount_spend_minus_dues'
,FMMS.[last_12_month_spend_amount] 'last_12_month_spend_amount'
,ISNULL(TotalLast12MonthDues.LT12MonthDues,0) 'last_12_month_dues_amount'
,FMMS.[last_12_month_spend_amount] - ISNULL(TotalLast12MonthDues.LT12MonthDues,0) as 'last_12_month_spend_minus_dues'
,MM.[customer_name]
,MM.[join_date]
,(CAST(CONVERT(CHAR(8),getdate(),112) AS INT) -  CAST(CONVERT(CHAR(8),MM.[join_date_key], 112) AS INT)) / 10000 'membership_tenure'
,corpCodes.[company_name]
,corpCodes.usage_report_member_type
,MM.[description_member]
,ClubLocation.state 'home_club_state'
,ClubLocation.club_name 'home_club'
,ClubLocation.marketing_club_level 'club_membership_level'
,Membership.[membership_type]
,Membership.[membership_status]
,MM.[gender_abbreviation] 'primary_member_gender'
,MM.[member_active_flag] 'primary_member_active_flag'

,(CAST(CONVERT(CHAR(8),getdate(),112) AS INT) - CAST(CONVERT(CHAR(8), MM.[date_of_birth], 112) AS INT)) / 10000 'primary_member_age'
,CASE WHEN (CAST(CONVERT(CHAR(8),getdate(),112) AS INT) - CAST(CONVERT(CHAR(8), MM.[date_of_birth], 112) AS INT)) / 10000 < 20 THEN '< 20'
		WHEN (CAST(CONVERT(CHAR(8),getdate(),112) AS INT) - CAST(CONVERT(CHAR(8), MM.[date_of_birth], 112) AS INT)) / 10000 > 19 
			  AND (CAST(CONVERT(CHAR(8),getdate(),112) AS INT) - CAST(CONVERT(CHAR(8), MM.[date_of_birth], 112) AS INT)) / 10000 < 30 THEN '20-29'
		WHEN (CAST(CONVERT(CHAR(8),getdate(),112) AS INT) - CAST(CONVERT(CHAR(8), MM.[date_of_birth], 112) AS INT)) / 10000 > 29
			  AND (CAST(CONVERT(CHAR(8),getdate(),112) AS INT) - CAST(CONVERT(CHAR(8), MM.[date_of_birth], 112) AS INT)) / 10000 < 40 THEN '30-39'
		WHEN (CAST(CONVERT(CHAR(8),getdate(),112) AS INT) - CAST(CONVERT(CHAR(8), MM.[date_of_birth], 112) AS INT)) / 10000 > 39
			  AND (CAST(CONVERT(CHAR(8),getdate(),112) AS INT) - CAST(CONVERT(CHAR(8), MM.[date_of_birth], 112) AS INT)) / 10000 < 50 THEN '40-49'
		WHEN (CAST(CONVERT(CHAR(8),getdate(),112) AS INT) - CAST(CONVERT(CHAR(8), MM.[date_of_birth], 112) AS INT)) / 10000 > 49 
			  AND (CAST(CONVERT(CHAR(8),getdate(),112) AS INT) - CAST(CONVERT(CHAR(8), MM.[date_of_birth], 112) AS INT)) / 10000 < 60 THEN '50-59'
		WHEN (CAST(CONVERT(CHAR(8),getdate(),112) AS INT) - CAST(CONVERT(CHAR(8), MM.[date_of_birth], 112) AS INT)) / 10000 > 59
			  AND (CAST(CONVERT(CHAR(8),getdate(),112) AS INT) - CAST(CONVERT(CHAR(8), MM.[date_of_birth], 112) AS INT)) / 10000 < 70 THEN '60-69'
		ELSE '70 +' END 'primary_member_age_segment'
	
,CASE WHEN SUBSTRING(CONVERT(char(8),MM.Date_Of_Birth, 112), 1,4) < 1946 THEN 'Silent Generation (Born 1945 and before)'
	  WHEN SUBSTRING(CONVERT(char(8),MM.Date_Of_Birth, 112), 1,4) > 1945 
			AND SUBSTRING(CONVERT(char(8),MM.Date_Of_Birth, 112), 1,4) < 1965 THEN 'Baby Boomers (Born 1946 to 1964)'
	  WHEN SUBSTRING(CONVERT(char(8),MM.Date_Of_Birth, 112), 1,4) > 1964
			AND SUBSTRING(CONVERT(char(8),MM.Date_Of_Birth, 112), 1,4) < 1981 THEN 'Generation X (Born 1965 to 1980)'
	  WHEN SUBSTRING(CONVERT(char(8),MM.Date_Of_Birth, 112), 1,4) > 1980 
			AND SUBSTRING(CONVERT(char(8),MM.Date_Of_Birth, 112), 1,4) < 1997 THEN 'Millennials (Born 1981 to 1996)'
	  ELSE 'Post-Millennials (Born 1997 and onward)' END 'primary_member_age_generation'

,CASE WHEN SUBSTRING(CONVERT(char(8),MM.Date_Of_Birth, 112), 1,4) < 1946 THEN 1
	  WHEN SUBSTRING(CONVERT(char(8),MM.Date_Of_Birth, 112), 1,4) > 1945 
			AND SUBSTRING(CONVERT(char(8),MM.Date_Of_Birth, 112), 1,4) < 1965 THEN 2
	  WHEN SUBSTRING(CONVERT(char(8),MM.Date_Of_Birth, 112), 1,4) > 1964
			AND SUBSTRING(CONVERT(char(8),MM.Date_Of_Birth, 112), 1,4) < 1981 THEN 3
	  WHEN SUBSTRING(CONVERT(char(8),MM.Date_Of_Birth, 112), 1,4) > 1980 
			AND SUBSTRING(CONVERT(char(8),MM.Date_Of_Birth, 112), 1,4) < 1997 THEN 4
	  ELSE 5 END 'primary_member_age_generation_sort'
,'Other' As 'SpendBucket'
,membership.membership_activation_date

FROM [marketing].[v_fact_mms_member_spend] FMMS
JOIN [marketing].[v_dim_mms_member] MM 
	ON FMMS.[dim_mms_member_key] = MM.[dim_mms_member_key]
JOIN [marketing].[v_dim_mms_membership] Membership
	ON FMMS.[dim_mms_membership_key] =  Membership.[dim_mms_membership_key]
JOIN [marketing].[v_fact_mms_member_reimbursement_program] memberReimb
	ON MM.[dim_mms_member_key] = memberReimb.[dim_mms_member_key] AND memberReimb.termination_date >= GETDATE()
JOIN [marketing].[v_dim_mms_company] corpCodes
	ON memberReimb.[dim_mms_company_key] = corpCodes.[dim_mms_company_key]
JOIN [marketing].[v_dim_club] ClubLocation
	ON membership.[home_dim_club_key] = ClubLocation.[Dim_Club_Key]
LEFT JOIN	(   --Calculating Monthly Dues All Time
		SELECT TI.dim_mms_member_key
		,SUM(TI.sales_dollar_amount) TotalLTDues
		,COUNT(TI.dim_mms_member_key) AllDuesAssessed 
		FROM marketing.v_fact_mms_transaction_item TI
		JOIN [marketing].[v_dim_mms_transaction_reason] DimTransactionReason 
		ON TI.dim_mms_transaction_reason_key =   DimTransactionReason.dim_mms_transaction_reason_key
		WHERE DimTransactionReason.reason_code_id = 28 
		GROUP BY TI.dim_mms_member_key
		) TotalDues ON TotalDues.dim_mms_member_key=FMMS.dim_mms_member_key
LEFT JOIN	(  --Calculating Monthly Dues within the Past 12 months
		SELECT TI.dim_mms_member_key
		,ISNULL(SUM(TI.sales_dollar_amount),0) LT12MonthDues
		,ISNULL(COUNT(TI.dim_mms_member_key),0) Last12MonthDuesAssessed
		FROM marketing.v_fact_mms_transaction_item TI
		JOIN [marketing].[v_dim_mms_transaction_reason] DimTransactionReason 
		ON TI.dim_mms_transaction_reason_key =   DimTransactionReason.dim_mms_transaction_reason_key
		LEFT JOIN marketing.v_dim_date D
		ON TI.post_dim_date_key = D.dim_date_key
		WHERE DimTransactionReason.reason_code_id = 28 
		AND DATEDIFF(MM,D.calendar_date,GETDATE()) < 12
		GROUP BY TI.dim_mms_member_key
		) TotalLast12MonthDues ON TotalLast12MonthDues.dim_mms_member_key=FMMS.dim_mms_member_key

WHERE
	  MM.description_member = 'Primary'
	  AND MM.Last_Name <> 'House Account'
	AND MM.dim_mms_membership_key NOT IN ('-997','-998','-999')
	AND membership.membership_status = 'Active'
	AND corpCodes.corporate_code  NOT IN ('73405', '75949', '87379', '84452','88486', '85899','89330')

UNION

 -----------------------------------------------------------TPA Membership Spend------------------------------------------
 
SELECT
MM.membership_id
,MM.member_id
,MM.dim_mms_membership_key
,MM.dim_mms_member_key
,FMMS.total_spend_amount
,ISNULL(TotalDues.TotalLTDues,0) 'total_dues_amount'
,FMMS.Total_spend_amount - ISNULL(TotalDues.TotalLTDues,0) as 'amount_spend_minus_dues'
,FMMS.[last_12_month_spend_amount] 'last_12_month_spend_amount'
,ISNULL(TotalLast12MonthDues.LT12MonthDues,0) 'last_12_month_dues_amount'
,FMMS.[last_12_month_spend_amount] - ISNULL(TotalLast12MonthDues.LT12MonthDues,0) as 'last_12_month_spend_minus_dues'
,MM.[customer_name]
,MM.[join_date]
,(CAST(CONVERT(CHAR(8),getdate(),112) AS INT) -  CAST(CONVERT(CHAR(8),MM.[join_date_key], 112) AS INT)) / 10000 'membership_tenure'
,corpCodes.[company_name]
,corpCodes.usage_report_member_type
,MM.[description_member]
,ClubLocation.state 'home_club_state'
,ClubLocation.club_name 'home_club'
,ClubLocation.marketing_club_level 'club_membership_level'
,Membership.[membership_type]
,Membership.[membership_status]
,MM.[gender_abbreviation] 'primary_member_gender'
,MM.[member_active_flag] 'primary_member_active_flag'

,(CAST(CONVERT(CHAR(8),getdate(),112) AS INT) - CAST(CONVERT(CHAR(8), MM.[date_of_birth], 112) AS INT)) / 10000 'primary_member_age'
,CASE WHEN (CAST(CONVERT(CHAR(8),getdate(),112) AS INT) - CAST(CONVERT(CHAR(8), MM.[date_of_birth], 112) AS INT)) / 10000 < 20 THEN '< 20'
		WHEN (CAST(CONVERT(CHAR(8),getdate(),112) AS INT) - CAST(CONVERT(CHAR(8), MM.[date_of_birth], 112) AS INT)) / 10000 > 19 
			  AND (CAST(CONVERT(CHAR(8),getdate(),112) AS INT) - CAST(CONVERT(CHAR(8), MM.[date_of_birth], 112) AS INT)) / 10000 < 30 THEN '20-29'
		WHEN (CAST(CONVERT(CHAR(8),getdate(),112) AS INT) - CAST(CONVERT(CHAR(8), MM.[date_of_birth], 112) AS INT)) / 10000 > 29
			  AND (CAST(CONVERT(CHAR(8),getdate(),112) AS INT) - CAST(CONVERT(CHAR(8), MM.[date_of_birth], 112) AS INT)) / 10000 < 40 THEN '30-39'
		WHEN (CAST(CONVERT(CHAR(8),getdate(),112) AS INT) - CAST(CONVERT(CHAR(8), MM.[date_of_birth], 112) AS INT)) / 10000 > 39
			  AND (CAST(CONVERT(CHAR(8),getdate(),112) AS INT) - CAST(CONVERT(CHAR(8), MM.[date_of_birth], 112) AS INT)) / 10000 < 50 THEN '40-49'
		WHEN (CAST(CONVERT(CHAR(8),getdate(),112) AS INT) - CAST(CONVERT(CHAR(8), MM.[date_of_birth], 112) AS INT)) / 10000 > 49 
			  AND (CAST(CONVERT(CHAR(8),getdate(),112) AS INT) - CAST(CONVERT(CHAR(8), MM.[date_of_birth], 112) AS INT)) / 10000 < 60 THEN '50-59'
		WHEN (CAST(CONVERT(CHAR(8),getdate(),112) AS INT) - CAST(CONVERT(CHAR(8), MM.[date_of_birth], 112) AS INT)) / 10000 > 59
			  AND (CAST(CONVERT(CHAR(8),getdate(),112) AS INT) - CAST(CONVERT(CHAR(8), MM.[date_of_birth], 112) AS INT)) / 10000 < 70 THEN '60-69'
		ELSE '70 +' END 'primary_member_age_segment'
	
,CASE WHEN SUBSTRING(CONVERT(char(8),MM.Date_Of_Birth, 112), 1,4) < 1946 THEN 'Silent Generation (Born 1945 and before)'
	  WHEN SUBSTRING(CONVERT(char(8),MM.Date_Of_Birth, 112), 1,4) > 1945 
			AND SUBSTRING(CONVERT(char(8),MM.Date_Of_Birth, 112), 1,4) < 1965 THEN 'Baby Boomers (Born 1946 to 1964)'
	  WHEN SUBSTRING(CONVERT(char(8),MM.Date_Of_Birth, 112), 1,4) > 1964
			AND SUBSTRING(CONVERT(char(8),MM.Date_Of_Birth, 112), 1,4) < 1981 THEN 'Generation X (Born 1965 to 1980)'
	  WHEN SUBSTRING(CONVERT(char(8),MM.Date_Of_Birth, 112), 1,4) > 1980 
			AND SUBSTRING(CONVERT(char(8),MM.Date_Of_Birth, 112), 1,4) < 1997 THEN 'Millennials (Born 1981 to 1996)'
	  ELSE 'Post-Millennials (Born 1997 and onward)' END 'primary_member_age_generation'

,CASE WHEN SUBSTRING(CONVERT(char(8),MM.Date_Of_Birth, 112), 1,4) < 1946 THEN 1
	  WHEN SUBSTRING(CONVERT(char(8),MM.Date_Of_Birth, 112), 1,4) > 1945 
			AND SUBSTRING(CONVERT(char(8),MM.Date_Of_Birth, 112), 1,4) < 1965 THEN 2
	  WHEN SUBSTRING(CONVERT(char(8),MM.Date_Of_Birth, 112), 1,4) > 1964
			AND SUBSTRING(CONVERT(char(8),MM.Date_Of_Birth, 112), 1,4) < 1981 THEN 3
	  WHEN SUBSTRING(CONVERT(char(8),MM.Date_Of_Birth, 112), 1,4) > 1980 
			AND SUBSTRING(CONVERT(char(8),MM.Date_Of_Birth, 112), 1,4) < 1997 THEN 4
	  ELSE 5 END 'primary_member_age_generation_sort'
,'TPA' As 'SpendBucket'
,membership.membership_activation_date
	  
FROM [marketing].[v_fact_mms_member_spend] FMMS
JOIN [marketing].[v_dim_mms_member] MM 
	ON FMMS.[dim_mms_member_key] = MM.[dim_mms_member_key]
JOIN [marketing].[v_dim_mms_membership] Membership
	ON FMMS.[dim_mms_membership_key] =  Membership.[dim_mms_membership_key]

JOIN [marketing].[v_fact_mms_member_reimbursement_program] memberReimb
	ON MM.[dim_mms_member_key] = memberReimb.[dim_mms_member_key] AND memberReimb.termination_date >= GETDATE()

JOIN [marketing].[v_dim_mms_company] corpCodes
	ON memberReimb.[dim_mms_company_key] = corpCodes.[dim_mms_company_key]
JOIN [marketing].[v_dim_club] ClubLocation
	ON Membership.[home_dim_club_key] = ClubLocation.[Dim_Club_Key]
LEFT JOIN	(   --Calculating Monthly Dues All Time
		SELECT TI.dim_mms_member_key
		,SUM(TI.sales_dollar_amount) TotalLTDues
		,COUNT(TI.dim_mms_member_key) AllDuesAssessed 
		FROM marketing.v_fact_mms_transaction_item TI
		JOIN [marketing].[v_dim_mms_transaction_reason] DimTransactionReason 
		ON TI.dim_mms_transaction_reason_key =   DimTransactionReason.dim_mms_transaction_reason_key
		WHERE DimTransactionReason.reason_code_id = 28 
		GROUP BY TI.dim_mms_member_key
		) TotalDues ON TotalDues.dim_mms_member_key=FMMS.dim_mms_member_key
LEFT JOIN	(  --Calculating Monthly Dues within the Past 12 months
		SELECT TI.dim_mms_member_key
		,ISNULL(SUM(TI.sales_dollar_amount),0) LT12MonthDues
		,ISNULL(COUNT(TI.dim_mms_member_key),0) Last12MonthDuesAssessed
		FROM marketing.v_fact_mms_transaction_item TI
		JOIN [marketing].[v_dim_mms_transaction_reason] DimTransactionReason 
		ON TI.dim_mms_transaction_reason_key =   DimTransactionReason.dim_mms_transaction_reason_key
		LEFT JOIN marketing.v_dim_date D
		ON TI.post_dim_date_key = D.dim_date_key
		WHERE DimTransactionReason.reason_code_id = 28 
		AND DATEDIFF(MM,D.calendar_date,GETDATE()) < 12
		GROUP BY TI.dim_mms_member_key
		) TotalLast12MonthDues ON TotalLast12MonthDues.dim_mms_member_key=FMMS.dim_mms_member_key

WHERE corpCodes.corporate_code in ('88486', '85899')
 AND MM.[description_member] = 'Primary'
 AND MM.Last_Name <> 'House Account'
 AND MM.dim_mms_membership_key NOT IN ('-997','-998','-999')
 AND membership.membership_status = 'Active'



UNION
----------------------------------------------Regular Membeshp Spend------------------------------------------

SELECT
MM.membership_id
,MM.member_id
,MM.dim_mms_membership_key
,MM.dim_mms_member_key
,FMMS.total_spend_amount
,ISNULL(TotalDues.TotalLTDues,0) 'total_dues_amount'
,FMMS.Total_spend_amount - ISNULL(TotalDues.TotalLTDues,0) as 'amount_spend_minus_dues'
,FMMS.[last_12_month_spend_amount] 'last_12_month_spend_amount'
,ISNULL(TotalLast12MonthDues.LT12MonthDues,0) 'last_12_month_dues_amount'
,FMMS.[last_12_month_spend_amount] - ISNULL(TotalLast12MonthDues.LT12MonthDues,0) as 'last_12_month_spend_minus_dues'
,MM.[customer_name]
,MM.[join_date]
,(CAST(CONVERT(CHAR(8),getdate(),112) AS INT) -  CAST(CONVERT(CHAR(8),MM.[join_date_key], 112) AS INT)) / 10000 'membership_tenure'
,NULL AS 'company_name'
,NULL AS 'usage_report_member_type'
,MM.[description_member]
,ClubLocation.state 'home_club_state'
,ClubLocation.club_name 'home_club'
,ClubLocation.marketing_club_level 'club_membership_level'
,Membership.[membership_type]
,Membership.[membership_status]
,MM.[gender_abbreviation] 'primary_member_gender'
,MM.[member_active_flag] 'primary_member_active_flag'

,(CAST(CONVERT(CHAR(8),getdate(),112) AS INT) - CAST(CONVERT(CHAR(8), MM.[date_of_birth], 112) AS INT)) / 10000 'primary_member_age'
,CASE WHEN (CAST(CONVERT(CHAR(8),getdate(),112) AS INT) - CAST(CONVERT(CHAR(8), MM.[date_of_birth], 112) AS INT)) / 10000 < 20 THEN '< 20'
		WHEN (CAST(CONVERT(CHAR(8),getdate(),112) AS INT) - CAST(CONVERT(CHAR(8), MM.[date_of_birth], 112) AS INT)) / 10000 > 19 
			  AND (CAST(CONVERT(CHAR(8),getdate(),112) AS INT) - CAST(CONVERT(CHAR(8), MM.[date_of_birth], 112) AS INT)) / 10000 < 30 THEN '20-29'
		WHEN (CAST(CONVERT(CHAR(8),getdate(),112) AS INT) - CAST(CONVERT(CHAR(8), MM.[date_of_birth], 112) AS INT)) / 10000 > 29
			  AND (CAST(CONVERT(CHAR(8),getdate(),112) AS INT) - CAST(CONVERT(CHAR(8), MM.[date_of_birth], 112) AS INT)) / 10000 < 40 THEN '30-39'
		WHEN (CAST(CONVERT(CHAR(8),getdate(),112) AS INT) - CAST(CONVERT(CHAR(8), MM.[date_of_birth], 112) AS INT)) / 10000 > 39
			  AND (CAST(CONVERT(CHAR(8),getdate(),112) AS INT) - CAST(CONVERT(CHAR(8), MM.[date_of_birth], 112) AS INT)) / 10000 < 50 THEN '40-49'
		WHEN (CAST(CONVERT(CHAR(8),getdate(),112) AS INT) - CAST(CONVERT(CHAR(8), MM.[date_of_birth], 112) AS INT)) / 10000 > 49 
			  AND (CAST(CONVERT(CHAR(8),getdate(),112) AS INT) - CAST(CONVERT(CHAR(8), MM.[date_of_birth], 112) AS INT)) / 10000 < 60 THEN '50-59'
		WHEN (CAST(CONVERT(CHAR(8),getdate(),112) AS INT) - CAST(CONVERT(CHAR(8), MM.[date_of_birth], 112) AS INT)) / 10000 > 59
			  AND (CAST(CONVERT(CHAR(8),getdate(),112) AS INT) - CAST(CONVERT(CHAR(8), MM.[date_of_birth], 112) AS INT)) / 10000 < 70 THEN '60-69'
		ELSE '70 +' END 'primary_member_age_segment'
	
,CASE WHEN SUBSTRING(CONVERT(char(8),MM.Date_Of_Birth, 112), 1,4) < 1946 THEN 'Silent Generation (Born 1945 and before)'
	  WHEN SUBSTRING(CONVERT(char(8),MM.Date_Of_Birth, 112), 1,4) > 1945 
			AND SUBSTRING(CONVERT(char(8),MM.Date_Of_Birth, 112), 1,4) < 1965 THEN 'Baby Boomers (Born 1946 to 1964)'
	  WHEN SUBSTRING(CONVERT(char(8),MM.Date_Of_Birth, 112), 1,4) > 1964
			AND SUBSTRING(CONVERT(char(8),MM.Date_Of_Birth, 112), 1,4) < 1981 THEN 'Generation X (Born 1965 to 1980)'
	  WHEN SUBSTRING(CONVERT(char(8),MM.Date_Of_Birth, 112), 1,4) > 1980 
			AND SUBSTRING(CONVERT(char(8),MM.Date_Of_Birth, 112), 1,4) < 1997 THEN 'Millennials (Born 1981 to 1996)'
	  ELSE 'Post-Millennials (Born 1997 and onward)' END 'primary_member_age_generation'

,CASE WHEN SUBSTRING(CONVERT(char(8),MM.Date_Of_Birth, 112), 1,4) < 1946 THEN 1
	  WHEN SUBSTRING(CONVERT(char(8),MM.Date_Of_Birth, 112), 1,4) > 1945 
			AND SUBSTRING(CONVERT(char(8),MM.Date_Of_Birth, 112), 1,4) < 1965 THEN 2
	  WHEN SUBSTRING(CONVERT(char(8),MM.Date_Of_Birth, 112), 1,4) > 1964
			AND SUBSTRING(CONVERT(char(8),MM.Date_Of_Birth, 112), 1,4) < 1981 THEN 3
	  WHEN SUBSTRING(CONVERT(char(8),MM.Date_Of_Birth, 112), 1,4) > 1980 
			AND SUBSTRING(CONVERT(char(8),MM.Date_Of_Birth, 112), 1,4) < 1997 THEN 4
	  ELSE 5 END 'primary_member_age_generation_sort'
,'Regular' As 'SpendBucket'
,membership.membership_activation_date

FROM [marketing].[v_fact_mms_member_spend] FMMS
JOIN [marketing].[v_dim_mms_member] MM 
	ON FMMS.[dim_mms_member_key] = MM.[dim_mms_member_key]
JOIN [marketing].[v_dim_mms_membership] Membership
	ON FMMS.[dim_mms_membership_key] =  Membership.[dim_mms_membership_key]
LEFT JOIN [marketing].[v_fact_mms_member_reimbursement_program] memberReimb
	ON MM.[dim_mms_member_key] = memberReimb.[dim_mms_member_key] AND memberReimb.termination_date <= GETDATE() 
LEFT JOIN	(   --Calculating Monthly Dues All Time
		SELECT TI.dim_mms_member_key
		,SUM(TI.sales_dollar_amount) TotalLTDues
		,COUNT(TI.dim_mms_member_key) AllDuesAssessed 
		FROM marketing.v_fact_mms_transaction_item TI
		JOIN [marketing].[v_dim_mms_transaction_reason] DimTransactionReason 
		ON TI.dim_mms_transaction_reason_key =   DimTransactionReason.dim_mms_transaction_reason_key
		WHERE DimTransactionReason.reason_code_id = 28 
		GROUP BY TI.dim_mms_member_key
		) TotalDues ON TotalDues.dim_mms_member_key=FMMS.dim_mms_member_key
LEFT JOIN	(  --Calculating Monthly Dues within the Past 12 months
		SELECT TI.dim_mms_member_key
		,ISNULL(SUM(TI.sales_dollar_amount),0) LT12MonthDues
		,ISNULL(COUNT(TI.dim_mms_member_key),0) Last12MonthDuesAssessed
		FROM marketing.v_fact_mms_transaction_item TI
		JOIN [marketing].[v_dim_mms_transaction_reason] DimTransactionReason 
		ON TI.dim_mms_transaction_reason_key =   DimTransactionReason.dim_mms_transaction_reason_key
		LEFT JOIN marketing.v_dim_date D
		ON TI.post_dim_date_key = D.dim_date_key
		WHERE DimTransactionReason.reason_code_id = 28 
		AND DATEDIFF(MM,D.calendar_date,GETDATE()) < 12
		GROUP BY TI.dim_mms_member_key
		) TotalLast12MonthDues ON TotalLast12MonthDues.dim_mms_member_key=FMMS.dim_mms_member_key


JOIN [marketing].[v_dim_club] ClubLocation
	ON membership.[home_dim_club_key] = ClubLocation.[Dim_Club_Key]
JOIN marketing.v_dim_mms_membership_type MT
	ON MT.dim_mms_membership_type_key=Membership.dim_mms_membership_type_key
JOIN marketing.v_dim_mms_product P
	ON P.Product_ID=MT.Product_id
JOIN marketing.v_dim_mms_membership_type_attribute MTA
	ON MTA.dim_mms_membership_type_key=MT.dim_mms_membership_type_key

WHERE
	MM.Last_Name <> 'House Account'
	AND MM.dim_mms_membership_key NOT IN ('-997','-998','-999')
	AND MTA.val_membership_type_attribute_id IN (5,6,7,8,9,11,14,22,25,26,27,67)
	AND membership.membership_status = 'Active'
	AND MM.dim_mms_membership_key NOT IN (	SELECT dim_mms_membership_key 
											FROM  marketing.v_fact_mms_member_reimbursement_program
											WHERE termination_date >= getdate()
											);