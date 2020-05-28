CREATE TABLE [dbo].[stage_hash_mms_LTFResource] (
    [stage_hash_mms_LTFResource_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                       CHAR (32)    NOT NULL,
    [LTFResourceID]                 INT          NULL,
    [Identifier]                    VARCHAR (50) NULL,
    [Name]                          VARCHAR (50) NULL,
    [ValResourceTypeID]             INT          NULL,
    [InsertedDateTime]              DATETIME     NULL,
    [UpdatedDateTime]               DATETIME     NULL,
    [dv_load_date_time]             DATETIME     NOT NULL,
    [dv_batch_id]                   BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

