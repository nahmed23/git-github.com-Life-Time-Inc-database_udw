CREATE TABLE [dbo].[stage_hash_mart_fact_seg_membership_term_risk] (
    [stage_hash_mart_fact_seg_membership_term_risk_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                          CHAR (32)       NOT NULL,
    [fact_seg_membership_term_risk]                    INT             NULL,
    [membership_id]                                    DECIMAL (26, 6) NULL,
    [term_risk_segment]                                DECIMAL (26, 6) NULL,
    [row_add_date]                                     DATETIME        NULL,
    [active_flag]                                      INT             NULL,
    [row_deactivation_date]                            DATETIME        NULL,
    [dv_load_date_time]                                DATETIME        NOT NULL,
    [dv_batch_id]                                      BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

