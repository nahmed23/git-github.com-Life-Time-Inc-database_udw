CREATE TABLE [dbo].[stage_nmo_hubtaskinterest] (
    [stage_nmo_hubtaskinterest_id] BIGINT         NOT NULL,
    [id]                           INT            NULL,
    [title]                        NVARCHAR (255) NULL,
    [hubtaskdepartmentid]          INT            NULL,
    [activationdate]               DATETIME       NULL,
    [expirationdate]               DATETIME       NULL,
    [createddate]                  DATETIME       NULL,
    [updateddate]                  DATETIME       NULL,
    [dv_batch_id]                  BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

