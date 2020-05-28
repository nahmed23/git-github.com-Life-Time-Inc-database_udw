CREATE TABLE [dbo].[stage_nmo_hubtaskdepartment] (
    [stage_nmo_hubtaskdepartment_id] BIGINT         NOT NULL,
    [id]                             INT            NULL,
    [title]                          NVARCHAR (200) NULL,
    [activationdate]                 DATETIME       NULL,
    [expirationdate]                 DATETIME       NULL,
    [createddate]                    DATETIME       NULL,
    [updateddate]                    DATETIME       NULL,
    [dv_batch_id]                    BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

