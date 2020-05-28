CREATE TABLE [dbo].[stage_hybris_categorieslp] (
    [stage_hybris_categorieslp_id] BIGINT         NOT NULL,
    [itempk]                       BIGINT         NULL,
    [itemtypepk]                   BIGINT         NULL,
    [langpk]                       BIGINT         NULL,
    [p_description]                VARCHAR (8000) NULL,
    [p_name]                       NVARCHAR (255) NULL,
    [jan_one]                      DATETIME       NULL,
    [dv_batch_id]                  BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

