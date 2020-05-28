CREATE TABLE [dbo].[s_ig_it_cfg_terminal_master_1] (
    [s_ig_it_cfg_terminal_master_1_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                          CHAR (32)    NOT NULL,
    [term_id]                          INT          NULL,
    [row_version]                      BINARY (8)   NULL,
    [last_ping_utc_date_time]          DATETIME     NULL,
    [dummy_modified_date_time]         DATETIME     NULL,
    [dv_load_date_time]                DATETIME     NOT NULL,
    [dv_r_load_source_id]              BIGINT       NOT NULL,
    [dv_inserted_date_time]            DATETIME     NOT NULL,
    [dv_insert_user]                   VARCHAR (50) NOT NULL,
    [dv_updated_date_time]             DATETIME     NULL,
    [dv_update_user]                   VARCHAR (50) NULL,
    [dv_hash]                          CHAR (32)    NOT NULL,
    [dv_deleted]                       BIT          DEFAULT ((0)) NOT NULL,
    [dv_batch_id]                      BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

