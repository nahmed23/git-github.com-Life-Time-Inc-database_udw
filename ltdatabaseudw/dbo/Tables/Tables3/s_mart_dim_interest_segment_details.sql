CREATE TABLE [dbo].[s_mart_dim_interest_segment_details] (
    [s_mart_dim_interest_segment_details_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                CHAR (32)    NOT NULL,
    [dim_interest_segment_details_id]        INT          NULL,
    [interest_id]                            INT          NULL,
    [interest_name]                          CHAR (50)    NULL,
    [row_add_date]                           DATETIME     NULL,
    [active_flag]                            INT          NULL,
    [interest_display_name]                  CHAR (50)    NULL,
    [dv_load_date_time]                      DATETIME     NOT NULL,
    [dv_r_load_source_id]                    BIGINT       NOT NULL,
    [dv_inserted_date_time]                  DATETIME     NOT NULL,
    [dv_insert_user]                         VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                   DATETIME     NULL,
    [dv_update_user]                         VARCHAR (50) NULL,
    [dv_hash]                                CHAR (32)    NOT NULL,
    [dv_deleted]                             BIT          DEFAULT ((0)) NOT NULL,
    [dv_batch_id]                            BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

