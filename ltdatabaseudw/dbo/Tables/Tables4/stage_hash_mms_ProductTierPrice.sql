CREATE TABLE [dbo].[stage_hash_mms_ProductTierPrice] (
    [stage_hash_mms_ProductTierPrice_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                            CHAR (32)       NOT NULL,
    [ProductTierPriceID]                 INT             NULL,
    [ProductTierID]                      INT             NULL,
    [Price]                              DECIMAL (26, 6) NULL,
    [ValMembershipTypeGroupID]           TINYINT         NULL,
    [InsertedDateTime]                   DATETIME        NULL,
    [UpdatedDateTime]                    DATETIME        NULL,
    [ValCardLevelID]                     INT             NULL,
    [dv_load_date_time]                  DATETIME        NOT NULL,
    [dv_batch_id]                        BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

