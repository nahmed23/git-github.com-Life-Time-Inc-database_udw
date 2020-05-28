CREATE TABLE [dbo].[stage_lt_bucks_Categories] (
    [stage_lt_bucks_Categories_id] BIGINT         NOT NULL,
    [category_id]                  INT            NULL,
    [category_catalog]             INT            NULL,
    [category_name]                NVARCHAR (50)  NULL,
    [category_desc]                VARCHAR (1000) NULL,
    [category_parent]              INT            NULL,
    [category_group]               INT            NULL,
    [category_active]              BIT            NULL,
    [category_image]               INT            NULL,
    [category_type]                INT            NULL,
    [category_conversion]          NVARCHAR (50)  NULL,
    [category_isdeleted]           BIT            NULL,
    [category_last_user]           INT            NULL,
    [LastModifiedTimestamp]        DATETIME       NULL,
    [dv_batch_id]                  BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

