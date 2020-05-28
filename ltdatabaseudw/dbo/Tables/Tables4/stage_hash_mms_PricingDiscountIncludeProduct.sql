CREATE TABLE [dbo].[stage_hash_mms_PricingDiscountIncludeProduct] (
    [stage_hash_mms_PricingDiscountIncludeProduct_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                         CHAR (32)       NOT NULL,
    [PricingDiscountIncludeProductID]                 INT             NULL,
    [PricingDiscountID]                               INT             NULL,
    [ProductID]                                       INT             NULL,
    [TriggerQuantity]                                 INT             NULL,
    [DiscountedProductID]                             INT             NULL,
    [DiscountUseLimit]                                INT             NULL,
    [InsertedDateTime]                                DATETIME        NULL,
    [UpdatedDateTime]                                 DATETIME        NULL,
    [OverrideDiscountTypeID]                          TINYINT         NULL,
    [OverrideDiscountValue]                           DECIMAL (10, 2) NULL,
    [OverrideSalesCommissionPercent]                  DECIMAL (6, 2)  NULL,
    [OverrideServiceCommissionPercent]                DECIMAL (6, 2)  NULL,
    [BundleProductFlag]                               BIT             NULL,
    [dv_load_date_time]                               DATETIME        NOT NULL,
    [dv_inserted_date_time]                           DATETIME        NOT NULL,
    [dv_insert_user]                                  VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                            DATETIME        NULL,
    [dv_update_user]                                  VARCHAR (50)    NULL,
    [dv_batch_id]                                     BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

