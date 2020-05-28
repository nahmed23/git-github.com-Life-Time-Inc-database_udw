CREATE TABLE [dbo].[stage_hash_mart_seg_member_juniors_on_account] (
    [stage_hash_mart_seg_member_juniors_on_account_id] BIGINT     IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                          CHAR (32)  NOT NULL,
    [juniors_on_account_segment_id]                    INT        NULL,
    [juniors_on_account]                               CHAR (255) NULL,
    [dummy_modified_date_time]                         DATETIME   NULL,
    [dv_load_date_time]                                DATETIME   NOT NULL,
    [dv_batch_id]                                      BIGINT     NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

