CREATE TABLE [dbo].[s_loc_attribute] (
    [s_loc_attribute_id]     BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                CHAR (32)     NOT NULL,
    [attribute_id]           BIGINT        NULL,
    [attribute_value]        VARCHAR (100) NULL,
    [udw_source_name]        VARCHAR (100) NULL,
    [created_date_time]      DATETIME      NULL,
    [created_by]             VARCHAR (100) NULL,
    [last_updated_date_time] DATETIME      NULL,
    [last_updated_by]        VARCHAR (100) NULL,
    [deleted_date_time]      DATETIME      NULL,
    [deleted_by]             VARCHAR (100) NULL,
    [managed_by_udw]         BIT           NULL,
    [dv_load_date_time]      DATETIME      NOT NULL,
    [dv_r_load_source_id]    BIGINT        NOT NULL,
    [dv_inserted_date_time]  DATETIME      NOT NULL,
    [dv_insert_user]         VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]   DATETIME      NULL,
    [dv_update_user]         VARCHAR (50)  NULL,
    [dv_hash]                CHAR (32)     NOT NULL,
    [dv_deleted]             BIT           DEFAULT ((0)) NOT NULL,
    [dv_batch_id]            BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

