CREATE TABLE [dbo].[stage_mms_ValMIPItem] (
    [stage_mms_ValMIPItem_id] BIGINT       NOT NULL,
    [ValMIPItemID]            SMALLINT     NULL,
    [Description]             VARCHAR (50) NULL,
    [SortOrder]               SMALLINT     NULL,
    [InsertedDateTime]        DATETIME     NULL,
    [UpdatedDateTime]         DATETIME     NULL,
    [dv_batch_id]             BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([dv_batch_id]));

