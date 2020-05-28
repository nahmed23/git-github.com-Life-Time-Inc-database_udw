CREATE TABLE [dbo].[dim_exerp_activity_tagging] (
    [dim_exerp_activity_tagging_id]  BIGINT        IDENTITY (1, 1) NOT NULL,
    [dim_boss_product_key]           CHAR (32)     NULL,
    [dim_exerp_activity_key]         CHAR (32)     NULL,
    [dim_exerp_activity_tagging_key] CHAR (32)     NULL,
    [tag_name]                       VARCHAR (255) NULL,
    [tag_type]                       VARCHAR (255) NULL,
    [taggable_id]                    INT           NULL,
    [taggings_id]                    INT           NULL,
    [tags_id]                        INT           NULL,
    [dv_load_date_time]              DATETIME      NULL,
    [dv_load_end_date_time]          DATETIME      NULL,
    [dv_batch_id]                    BIGINT        NOT NULL,
    [dv_inserted_date_time]          DATETIME      NOT NULL,
    [dv_insert_user]                 VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]           DATETIME      NULL,
    [dv_update_user]                 VARCHAR (50)  NULL
)
WITH (HEAP, DISTRIBUTION = HASH([dim_exerp_activity_tagging_key]));

