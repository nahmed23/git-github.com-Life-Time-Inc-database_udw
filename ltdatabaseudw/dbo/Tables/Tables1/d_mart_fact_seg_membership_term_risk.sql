CREATE TABLE [dbo].[d_mart_fact_seg_membership_term_risk] (
    [d_mart_fact_seg_membership_term_risk_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                 CHAR (32)       NOT NULL,
    [fact_seg_membership_term_risk_key]       VARCHAR (32)    NULL,
    [fact_seg_membership_term_risk_id]        INT             NULL,
    [active_flag]                             CHAR (1)        NULL,
    [dim_mms_membership_key]                  VARCHAR (32)    NULL,
    [membership_id]                           DECIMAL (26, 6) NULL,
    [row_add_date]                            DATETIME        NULL,
    [row_add_dim_date_key]                    VARCHAR (8)     NULL,
    [row_add_dim_time_key]                    INT             NULL,
    [row_deactivation_date]                   DATETIME        NULL,
    [row_deactivation_dim_date_key]           VARCHAR (8)     NULL,
    [row_deactivation_dim_time_key]           INT             NULL,
    [term_risk_segment]                       DECIMAL (26, 6) NULL,
    [p_mart_fact_seg_membership_term_risk_id] BIGINT          NOT NULL,
    [deleted_flag]                            INT             NULL,
    [dv_load_date_time]                       DATETIME        NULL,
    [dv_load_end_date_time]                   DATETIME        NULL,
    [dv_batch_id]                             BIGINT          NOT NULL,
    [dv_inserted_date_time]                   DATETIME        NOT NULL,
    [dv_insert_user]                          VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                    DATETIME        NULL,
    [dv_update_user]                          VARCHAR (50)    NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

