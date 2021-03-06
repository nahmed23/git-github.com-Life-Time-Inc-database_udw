﻿CREATE TABLE [dbo].[p_mart_fact_seg_membership_term_risk] (
    [p_mart_fact_seg_membership_term_risk_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                 CHAR (32)    NOT NULL,
    [fact_seg_membership_term_risk_id]        INT          NULL,
    [l_mart_fact_seg_membership_term_risk_id] BIGINT       NULL,
    [s_mart_fact_seg_membership_term_risk_id] BIGINT       NULL,
    [dv_load_date_time]                       DATETIME     NOT NULL,
    [dv_load_end_date_time]                   DATETIME     NOT NULL,
    [dv_greatest_satellite_date_time]         DATETIME     NULL,
    [dv_next_greatest_satellite_date_time]    DATETIME     NULL,
    [dv_first_in_key_series]                  INT          NULL,
    [dv_inserted_date_time]                   DATETIME     NOT NULL,
    [dv_insert_user]                          VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                    DATETIME     NULL,
    [dv_update_user]                          VARCHAR (50) NULL,
    [dv_batch_id]                             BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

