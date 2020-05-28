CREATE TABLE [dbo].[l_athlinks_api_vw_master_event] (
    [l_athlinks_api_vw_master_event_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                           CHAR (32)    NOT NULL,
    [master_id]                         INT          NULL,
    [company_id]                        INT          NULL,
    [next_race_id]                      INT          NULL,
    [prev_race_id]                      INT          NULL,
    [dv_load_date_time]                 DATETIME     NOT NULL,
    [dv_r_load_source_id]               BIGINT       NOT NULL,
    [dv_inserted_date_time]             DATETIME     NOT NULL,
    [dv_insert_user]                    VARCHAR (50) NOT NULL,
    [dv_updated_date_time]              DATETIME     NULL,
    [dv_update_user]                    VARCHAR (50) NULL,
    [dv_hash]                           CHAR (32)    NOT NULL,
    [dv_deleted]                        BIT          DEFAULT ((0)) NOT NULL,
    [dv_batch_id]                       BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

