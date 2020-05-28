CREATE TABLE [sandbox_ebi].[exerp_mapping] (
    [ProductID]                INT          NOT NULL,
    [Department]               VARCHAR (21) NULL,
    [Product]                  VARCHAR (57) NOT NULL,
    [New_Exerp_Product]        VARCHAR (50) NOT NULL,
    [MMS_New_ProductID]        VARCHAR (5)  NULL,
    [Global_Name_Clipcard]     VARCHAR (30) NULL,
    [Global_name_Subscription] VARCHAR (30) NULL,
    [DeductType]               VARCHAR (8)  NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

