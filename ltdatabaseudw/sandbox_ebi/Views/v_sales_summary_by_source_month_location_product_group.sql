CREATE VIEW [sandbox_ebi].[v_sales_summary_by_source_month_location_product_group] AS SELECT 'Cafe' as sales_source,
      Club.region AS mms_region_description,
      Club.club_id,
	  Club.club_name,
      CAST(OrderDate.month_starting_date AS date) month_starting_date,
	  Product.reporting_department,
	  Product.reporting_product_group,
      SUM(CafeSales.item_sales_dollar_amount_excluding_tax) sales_amount,
	  Club.state_abbreviation,
	  Club.area,
	  Club.club_re_open_date
FROM [dbo].[fact_cafe_sales_transaction_item] CafeSales
JOIN [dbo].[dim_date]  OrderDate
  ON CafeSales.order_close_dim_date_key = OrderDate.dim_date_key
JOIN [marketing].[v_dim_cafe_product] Product
  ON CafeSales.dim_cafe_product_key = Product.dim_cafe_product_key
JOIN [sandbox_ebi].[v_location] Club
  ON CafeSales.dim_club_key = Club.dim_club_key
WHERE CafeSales.item_voided_flag = 'N'       
AND (CafeSales.order_void_flag = 'N'       -------------------- Special handling of refunds in Cafe application - refunds have a order_void_flag = 'Y'
     OR CafeSales.order_refund_flag = 'Y')
GROUP BY Club.region,
      Club.club_id,
	  Club.club_name,
      CAST(OrderDate.month_starting_date AS date),
	  Product.reporting_product_group,
      Product.reporting_department,
	  Club.state_abbreviation,
	  Club.area,
	  Club.club_re_open_date

UNION ALL


SELECT 'Spa' as sales_source,
       Club.region AS mms_region_description,
       CAST(CASE WHEN Ticket.store_number = 123  THEN 15
			WHEN Ticket.store_number = 127  THEN 173
			ELSE Ticket.store_number  END AS INT) club_id,
	   Club.club_name,
	   CAST(TicketDate.month_starting_date AS date) month_starting_date,
	   CASE WHEN Ticket.service_amount <> 0 THEN 'Service'
			ELSE 'Product' END reporting_department,
       CASE WHEN Ticket.service_amount <> 0 THEN isnull(DimService.category,'')
			ELSE 'Product' END reporting_product_group,
       SUM(Ticket.product_amount + Ticket.service_amount) as sales_amount,
	   Club.state_abbreviation,
	   Club.area,
	   Club.club_re_open_date
  FROM [marketing].[v_fact_spabiz_ticket_item] Ticket
  JOIN [marketing].[v_dim_spabiz_service] DimService
    ON Ticket.dim_spabiz_service_key = DimService.dim_spabiz_service_key
  JOIN [dbo].[dim_date]  TicketDate
    ON CAST(Ticket.ticket_item_date_time AS DATE) = TicketDate.calendar_date
  JOIN [sandbox_ebi].[v_location] Club
    ON Club.club_id = (CASE WHEN Ticket.store_number = 123  THEN 15
			                WHEN Ticket.store_number = 127  THEN 173
			                ELSE Ticket.store_number  END)
 WHERE Ticket.status_id = 1
GROUP BY  Club.region,
       CAST(CASE WHEN Ticket.store_number = 123  THEN 15
			WHEN Ticket.store_number = 127  THEN 173
			ELSE Ticket.store_number  END AS INT),
	   Club.club_name,
	   CAST(TicketDate.month_starting_date AS date),
	   CASE WHEN Ticket.service_amount <> 0 THEN 'Service'
			ELSE 'Product' END,
       CASE WHEN Ticket.service_amount <> 0 THEN isnull(DimService.category,'')
			ELSE 'Product' END,
	  Club.state_abbreviation,
	  Club.area,
	  Club.club_re_open_date

UNION ALL


SELECT 'HealthCheckUSA' as sales_source,
       Club.region AS mms_region_description,
	   Club.club_id,
	   Club.club_name,
	   CAST(TranDate.month_starting_date AS date) month_starting_date,
	   Product.reporting_department,
	   Product.reporting_product_group,
       SUM(HCUSASales.sales_amount) sales_amount,
	   Club.state_abbreviation,
	   Club.area,
	   Club.club_re_open_date
FROM [dbo].[fact_healthcheckusa_allocated_transaction_item] HCUSASales
JOIN [dbo].[dim_date]  TranDate
  ON HCUSASales.transaction_post_dim_date_key = TranDate.dim_date_key
JOIN [marketing].[v_dim_healthcheckusa_product] Product
  ON HCUSASales.dim_healthcheckusa_product_key = Product.dim_healthcheckusa_product_key
JOIN [sandbox_ebi].[v_location] Club
  ON HCUSASales.transaction_reporting_dim_club_key = Club.dim_club_key
GROUP BY Club.region,
       Club.club_id,
	   Club.club_name,
	   CAST(TranDate.month_starting_date AS date),
	   Product.reporting_department,
	   Product.reporting_product_group,
	   Club.state_abbreviation,
	   Club.area,
	   Club.club_re_open_date

UNION ALL


SELECT 'Hybris' as sales_source,
       Club.region AS mms_region_description,
       Club.club_id,
	   Club.club_name,
	   CAST(SettlementDate.month_starting_date AS date) month_starting_date,
	   Product.reporting_department,
	   Product.reporting_product_group,
       SUM(HybrisSales.transaction_amount_gross) sales_amount,
	   Club.state_abbreviation,
	   Club.area,
	   Club.club_re_open_date
FROM [dbo].[fact_hybris_transaction_item] HybrisSales
JOIN [dbo].[dim_date]  SettlementDate
  ON HybrisSales.settlement_dim_date_key = SettlementDate.dim_date_key
JOIN [marketing].[v_dim_hybris_product] Product
  ON HybrisSales.dim_hybris_product_key = Product.dim_hybris_product_key
JOIN [sandbox_ebi].[v_location] Club
  ON HybrisSales.transaction_reporting_dim_club_key = Club.dim_club_key
GROUP BY Club.region,
       Club.club_id,
	   Club.club_name,
	   CAST(SettlementDate.month_starting_date AS date),
	   Product.reporting_department,
	   Product.reporting_product_group,
	   Club.state_abbreviation,
	   Club.area,
	   Club.club_re_open_date

UNION ALL

SELECT 'Magento' as sales_source,
       Club.region AS mms_region_description,
       Club.club_id,
	   Club.club_name,
	   CAST(TranDate.month_starting_date AS date) month_starting_date,
	   Product.reporting_department,
	   Product.reporting_product_group,
	   SUM(MagentoSales.transaction_item_amount - MagentoSales.transaction_discount_amount) AS sales_amount,
	   Club.state_abbreviation,
	   Club.area,
	   Club.club_re_open_date
FROM [dbo].[fact_magento_tran_item] MagentoSales
JOIN [dbo].[dim_date]  TranDate
  ON MagentoSales.order_dim_date_key = TranDate.dim_date_key
JOIN [marketing].[v_dim_magento_product] Product
  ON MagentoSales.dim_magento_product_key = Product.dim_magento_product_key
JOIN [sandbox_ebi].[v_location] Club
  ON MagentoSales.transaction_reporting_dim_club_key = Club.dim_club_key
GROUP BY Club.region,
       Club.club_id,
	   Club.club_name,
	   CAST(TranDate.month_starting_date AS date),
	   Product.reporting_department,
	   Product.reporting_product_group,
	   Club.state_abbreviation,
	   Club.area,
	   Club.club_re_open_date

UNION ALL


SELECT 'MMS' as sales_source,
       Club.region AS mms_region_description,
       Club.club_id,
	   Club.club_name,
	   CAST(TranDate.month_starting_date AS date) month_starting_date,
	   Product.reporting_department,
	   Product.reporting_product_group,
       SUM(MSSSales.sales_dollar_amount) sales_amount,
	   Club.state_abbreviation,
	   Club.area,
	   Club.club_re_open_date
FROM [dbo].[fact_mms_sales_transaction_item] MSSSales
LEFT JOIN [dbo].[fact_mms_sales_transaction_item_automated_refund] Refund
    ON MSSSales.fact_mms_sales_transaction_item_key = Refund.refund_fact_mms_sales_transaction_item_key
JOIN [dbo].[dim_date] TranDate
  ON MSSSales.post_dim_date_key = TranDate.dim_date_key
JOIN [marketing].[v_dim_mms_product] Product
  ON MSSSales.dim_mms_product_key = Product.dim_mms_product_key
JOIN [sandbox_ebi].[v_location] Club
  ON MSSSales.transaction_reporting_dim_club_key = Club.dim_club_key
JOIN [sandbox_ebi].[v_location] TransactionClub
  ON MSSSales.dim_club_key = TransactionClub.dim_club_key
WHERE MSSSales.voided_flag = 'N'
AND (Refund.refund_fact_mms_sales_transaction_item_key IS NULL           
      OR (Refund.refund_fact_mms_sales_transaction_item_key IS NOT NULL    
          AND TransactionClub.club_id <> 13)) 
GROUP BY  Club.region,
       Club.club_id,
	   Club.club_name,
	   CAST(TranDate.month_starting_date AS date),
	   Product.reporting_department,
	   Product.reporting_product_group,
	   Club.state_abbreviation,
	   Club.area,
	   Club.club_re_open_date

UNION ALL

  ---- Union with MMS refunds assigned to Corporate Internal
  ---- Must trace back to original sale club
SELECT 'MMS' as sales_source,
       Club.region AS mms_region_description,
       Club.club_id,
	   Club.club_name,
	   CAST(TranDate.month_starting_date AS date) month_starting_date,
	   Product.reporting_department,
	   Product.reporting_product_group,
       SUM(MSSSales.sales_dollar_amount) sales_amount,
	   Club.state_abbreviation,
	   Club.area,
	   Club.club_re_open_date
FROM [dbo].[fact_mms_sales_transaction_item] MSSSales
JOIN [dbo].[fact_mms_sales_transaction_item_automated_refund] Refund
    ON MSSSales.fact_mms_sales_transaction_item_key = Refund.refund_fact_mms_sales_transaction_item_key 
JOIN [sandbox_ebi].[v_location] Club
  ON Refund.original_transaction_reporting_dim_club_key = Club.dim_club_key
JOIN [sandbox_ebi].[v_location] TransactionClub
  ON MSSSales.dim_club_key = TransactionClub.dim_club_key
JOIN [dbo].[dim_date]  TranDate
  ON MSSSales.post_dim_date_key = TranDate.dim_date_key
JOIN [marketing].[v_dim_mms_product] Product
  ON MSSSales.dim_mms_product_key = Product.dim_mms_product_key
WHERE MSSSales.voided_flag = 'N'
AND TransactionClub.club_id = 13
GROUP BY  Club.region,
       Club.club_id,
	   Club.club_name,
	   CAST(TranDate.month_starting_date AS date),
	   Product.reporting_department,
	   Product.reporting_product_group,
	   Club.state_abbreviation,
	   Club.area,
	   Club.club_re_open_date;