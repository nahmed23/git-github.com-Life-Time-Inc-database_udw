CREATE VIEW [sandbox].[v_mart_sw_qualified_sales_promotion]
AS SELECT d_mms_qualified_sales_promotion.[qualified_sales_promotion_id]
       , d_mms_qualified_sales_promotion.[val_qualified_sales_promotion_type_id]
       , d_mms_qualified_sales_promotion.[sales_promotion_id]
       , d_mms_qualified_sales_promotion.[promotion_name]
       , d_mms_qualified_sales_promotion.[description]
       , [qualified_sales_promotion_description]      = d_mms_qualified_sales_promotion.[description]
       , [qualified_sales_promotion_type_description] = r_mms_val_qualified_sales_promotion_type.[description]
       , d_mms_qualified_sales_promotion.[dim_mms_qualified_sales_promotion_key]
       , d_mms_qualified_sales_promotion.[dim_mms_sales_promotion_key]
       , d_mms_qualified_sales_promotion.[dim_mms_val_qualified_sales_promotion_type_key]
    FROM [sandbox].[v_mart_mms_qualified_sales_promotion] d_mms_qualified_sales_promotion
         INNER JOIN [sandbox].[v_mart_mms_val_qualified_sales_promotion_type] r_mms_val_qualified_sales_promotion_type
           ON r_mms_val_qualified_sales_promotion_type.[val_qualified_sales_promotion_type_id] = d_mms_qualified_sales_promotion.[val_qualified_sales_promotion_type_id];