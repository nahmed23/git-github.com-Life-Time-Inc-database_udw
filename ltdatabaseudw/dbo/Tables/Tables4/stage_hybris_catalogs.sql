CREATE TABLE [dbo].[stage_hybris_catalogs] (
    [stage_hybris_catalogs_id] BIGINT         NOT NULL,
    [hjmpTS]                   BIGINT         NULL,
    [createdTS]                DATETIME       NULL,
    [modifiedTS]               DATETIME       NULL,
    [TypePkString]             BIGINT         NULL,
    [OwnerPkString]            BIGINT         NULL,
    [PK]                       BIGINT         NULL,
    [p_id]                     NVARCHAR (200) NULL,
    [p_activecatalogversion]   BIGINT         NULL,
    [p_defaultcatalog]         TINYINT        NULL,
    [p_supplier]               BIGINT         NULL,
    [p_buyer]                  BIGINT         NULL,
    [p_previewurltemplate]     NVARCHAR (255) NULL,
    [aCLTS]                    BIGINT         NULL,
    [propTS]                   BIGINT         NULL,
    [dv_batch_id]              BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

