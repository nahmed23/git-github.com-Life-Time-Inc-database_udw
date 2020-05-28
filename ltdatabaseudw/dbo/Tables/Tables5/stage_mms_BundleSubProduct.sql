CREATE TABLE [dbo].[stage_mms_BundleSubProduct] (
    [stage_mms_BundleSubProduct_id]     BIGINT       NOT NULL,
    [BundleSubProductID]                INT          NULL,
    [BundleProductID]                   INT          NULL,
    [SubProductID]                      INT          NULL,
    [BundleProductGroupNumber]          INT          NULL,
    [Quantity]                          INT          NULL,
    [GLAccountNumber]                   VARCHAR (5)  NULL,
    [GLSubAccountNumber]                VARCHAR (7)  NULL,
    [GLOverRideClubID]                  INT          NULL,
    [ValGLGroupID]                      TINYINT      NULL,
    [InsertedDateTime]                  DATETIME     NULL,
    [UpdatedDateTime]                   DATETIME     NULL,
    [WorkdayAccount]                    VARCHAR (6)  NULL,
    [WorkdayCostCenter]                 VARCHAR (6)  NULL,
    [WorkdayOffering]                   VARCHAR (10) NULL,
    [WorkdayOverRideRegion]             VARCHAR (4)  NULL,
    [WorkdayRevenueProductGroupAccount] VARCHAR (6)  NULL,
    [dv_batch_id]                       BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

