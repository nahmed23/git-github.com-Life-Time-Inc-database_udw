﻿CREATE TABLE [dbo].[stage_hybris_currencieslp] (
    [stage_hybris_currencieslp_id] BIGINT         NOT NULL,
    [ITEMPK]                       BIGINT         NULL,
    [ITEMTYPEPK]                   BIGINT         NULL,
    [LANGPK]                       BIGINT         NULL,
    [p_name]                       NVARCHAR (255) NULL,
    [createdTS]                    DATETIME       NULL,
    [modifiedTS]                   DATETIME       NULL,
    [dv_batch_id]                  BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

