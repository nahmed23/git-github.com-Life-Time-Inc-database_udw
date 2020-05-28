CREATE VIEW [sandbox].[v_mart_sw_membership_type_product_ltwl]
AS SELECT [membership_type_id]           = d_mms_membership_type.[membership_type_id]
       , [product_id]                   = d_mms_membership_type.[product_id]
       , [membership_type_display_name] = d_mms_membership_type.[display_name]
       , [dim_mms_membership_type_key]  = d_mms_membership_type.[dim_mms_membership_type_key]
       , [dim_mms_product_key]          = d_mms_membership_type.[dim_mms_product_key]
    FROM [dbo].[d_mms_membership_type] d_mms_membership_type
    WHERE d_mms_membership_type.[membership_type_id] = 9729;