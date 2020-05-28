﻿CREATE TABLE [dbo].[stage_hash_nmo_hubtaskdepartment] (
    [stage_hash_nmo_hubtaskdepartment_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                             CHAR (32)      NOT NULL,
    [id]                                  INT            NULL,
    [title]                               NVARCHAR (200) NULL,
    [activationdate]                      DATETIME       NULL,
    [expirationdate]                      DATETIME       NULL,
    [createddate]                         DATETIME       NULL,
    [updateddate]                         DATETIME       NULL,
    [dv_load_date_time]                   DATETIME       NOT NULL,
    [dv_batch_id]                         BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

