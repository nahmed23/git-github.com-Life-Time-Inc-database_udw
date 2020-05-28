CREATE TABLE [dbo].[stage_hash_hybris_currencieslp] (
    [stage_hash_hybris_currencieslp_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                           CHAR (32)      NOT NULL,
    [ITEMPK]                            BIGINT         NULL,
    [ITEMTYPEPK]                        BIGINT         NULL,
    [LANGPK]                            BIGINT         NULL,
    [p_name]                            NVARCHAR (255) NULL,
    [createdTS]                         DATETIME       NULL,
    [modifiedTS]                        DATETIME       NULL,
    [dv_load_date_time]                 DATETIME       NOT NULL,
    [dv_inserted_date_time]             DATETIME       NOT NULL,
    [dv_insert_user]                    VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]              DATETIME       NULL,
    [dv_update_user]                    VARCHAR (50)   NULL,
    [dv_batch_id]                       BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

