CREATE TABLE [dbo].[stage_hash_boss_interest] (
    [stage_hash_boss_interest_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                     CHAR (32)    NOT NULL,
    [id]                          INT          NULL,
    [short_desc]                  CHAR (15)    NULL,
    [long_desc]                   CHAR (50)    NULL,
    [dummy_modified_date_time]    DATETIME     NULL,
    [dv_load_date_time]           DATETIME     NOT NULL,
    [dv_inserted_date_time]       DATETIME     NOT NULL,
    [dv_insert_user]              VARCHAR (50) NOT NULL,
    [dv_updated_date_time]        DATETIME     NULL,
    [dv_update_user]              VARCHAR (50) NULL,
    [dv_batch_id]                 BIGINT       NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));

