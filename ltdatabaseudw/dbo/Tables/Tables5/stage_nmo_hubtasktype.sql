CREATE TABLE [dbo].[stage_nmo_hubtasktype] (
    [stage_nmo_hubtasktype_id] BIGINT         NOT NULL,
    [id]                       INT            NULL,
    [title]                    NVARCHAR (200) NULL,
    [description]              NVARCHAR (255) NULL,
    [createdDate]              DATETIME       NULL,
    [updateddate]              DATETIME       NULL,
    [dv_batch_id]              BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

