CREATE TABLE [dbo].[s_mms_ltf_key_owner] (
    [s_mms_ltf_key_owner_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                CHAR (32)     NOT NULL,
    [ltf_key_owner_id]       INT           NULL,
    [key_priority]           INT           NULL,
    [from_date]              DATETIME      NULL,
    [thru_date]              DATETIME      NULL,
    [from_time]              VARCHAR (15)  NULL,
    [thru_time]              VARCHAR (15)  NULL,
    [usage_count]            INT           NULL,
    [usage_limit]            INT           NULL,
    [inserted_date_time]     DATETIME      NULL,
    [updated_date_time]      DATETIME      NULL,
    [display_name]           VARCHAR (200) NULL,
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

