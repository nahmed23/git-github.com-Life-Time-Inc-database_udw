﻿CREATE TABLE [dbo].[stage_hash_mart_dim_seg_membership_term_risk] (
    [stage_hash_mart_dim_seg_membership_term_risk_id] BIGINT     IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                         CHAR (32)  NOT NULL,
    [dim_seg_term_risk_id]                            INT        NULL,
    [term_risk_segment]                               INT        NULL,
    [term_risk]                                       CHAR (255) NULL,
    [row_add_date]                                    DATETIME   NULL,
    [active_flag]                                     INT        NULL,
    [dv_load_date_time]                               DATETIME   NOT NULL,
    [dv_batch_id]                                     BIGINT     NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

