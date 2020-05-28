CREATE TABLE [dbo].[stage_nmo_hubtasknotes] (
    [stage_nmo_hubtasknotes_id] BIGINT         NOT NULL,
    [id]                        INT            NULL,
    [hubtaskid]                 INT            NULL,
    [title]                     NVARCHAR (255) NULL,
    [description]               VARCHAR (8000) NULL,
    [creatorpartyid]            INT            NULL,
    [creatorname]               NVARCHAR (60)  NULL,
    [createddate]               DATETIME       NULL,
    [updateddate]               DATETIME       NULL,
    [dv_batch_id]               BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

