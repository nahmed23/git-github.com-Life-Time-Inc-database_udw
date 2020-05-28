CREATE TABLE [dbo].[dim_description] (
    [dim_description_id]      BIGINT        IDENTITY (1, 1) NOT NULL,
    [dim_description_key]     VARCHAR (255) NULL,
    [source_object]           VARCHAR (255) NULL,
    [source_bk_hash]          VARCHAR (32)  NULL,
    [abbreviated_description] VARCHAR (25)  NULL,
    [description]             VARCHAR (100) NULL,
    [dv_load_date_time]       DATETIME      NULL,
    [dv_load_end_date_time]   DATETIME      NULL,
    [dv_batch_id]             BIGINT        NOT NULL,
    [dv_inserted_date_time]   DATETIME      NOT NULL,
    [dv_insert_user]          VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]    DATETIME      NULL,
    [dv_update_user]          VARCHAR (50)  NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([dim_description_key]));

