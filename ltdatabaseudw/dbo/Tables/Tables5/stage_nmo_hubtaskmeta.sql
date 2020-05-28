CREATE TABLE [dbo].[stage_nmo_hubtaskmeta] (
    [stage_nmo_hubtaskmeta_id] BIGINT         NOT NULL,
    [id]                       INT            NULL,
    [hubtaskid]                INT            NULL,
    [metakey]                  NVARCHAR (200) NULL,
    [metadescription]          NVARCHAR (255) NULL,
    [createddate]              DATETIME       NULL,
    [updateddate]              DATETIME       NULL,
    [dv_batch_id]              BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

