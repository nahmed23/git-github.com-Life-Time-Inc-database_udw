CREATE TABLE [dbo].[stage_hash_mart_fact_member_interests] (
    [stage_hash_mart_fact_member_interests_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                  CHAR (32)       NOT NULL,
    [fact_member_interests_id]                 INT             NULL,
    [member_id]                                DECIMAL (26, 6) NULL,
    [interest_id]                              DECIMAL (26, 6) NULL,
    [interest_confidence]                      DECIMAL (26, 6) NULL,
    [row_add_date]                             DATETIME        NULL,
    [active_flag]                              INT             NULL,
    [row_deactivation_date]                    DATETIME        NULL,
    [dv_load_date_time]                        DATETIME        NOT NULL,
    [dv_batch_id]                              BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

