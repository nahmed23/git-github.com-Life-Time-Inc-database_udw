CREATE TABLE [dbo].[stage_hash_lt_bucks_Categories] (
    [stage_hash_lt_bucks_Categories_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                           CHAR (32)      NOT NULL,
    [category_id]                       INT            NULL,
    [category_catalog]                  INT            NULL,
    [category_name]                     NVARCHAR (50)  NULL,
    [category_desc]                     VARCHAR (1000) NULL,
    [category_parent]                   INT            NULL,
    [category_group]                    INT            NULL,
    [category_active]                   BIT            NULL,
    [category_image]                    INT            NULL,
    [category_type]                     INT            NULL,
    [category_conversion]               NVARCHAR (50)  NULL,
    [category_isdeleted]                BIT            NULL,
    [category_last_user]                INT            NULL,
    [LastModifiedTimestamp]             DATETIME       NULL,
    [dv_load_date_time]                 DATETIME       NOT NULL,
    [dv_inserted_date_time]             DATETIME       NOT NULL,
    [dv_insert_user]                    VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]              DATETIME       NULL,
    [dv_update_user]                    VARCHAR (50)   NULL,
    [dv_batch_id]                       BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

