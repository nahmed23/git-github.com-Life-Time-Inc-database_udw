﻿CREATE TABLE [dbo].[p_mdm_golden_record_customer_linkage] (
    [p_mdm_golden_record_customer_linkage_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                 CHAR (32)     NOT NULL,
    [load_date_time]                          DATETIME      NULL,
    [row_number]                              INT           NULL,
    [source_code]                             VARCHAR (128) NULL,
    [source_id]                               VARCHAR (128) NULL,
    [event_date_time]                         DATETIME      NULL,
    [event_type]                              VARCHAR (128) NULL,
    [s_mdm_golden_record_customer_linkage_id] BIGINT        NULL,
    [l_mdm_golden_record_customer_linkage_id] BIGINT        NULL,
    [dv_greatest_satellite_date_time]         DATETIME      NULL,
    [dv_next_greatest_satellite_date_time]    DATETIME      NULL,
    [dv_load_date_time]                       DATETIME      NOT NULL,
    [dv_load_end_date_time]                   DATETIME      NOT NULL,
    [dv_batch_id]                             BIGINT        NOT NULL,
    [dv_inserted_date_time]                   DATETIME      NOT NULL,
    [dv_insert_user]                          VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]                    DATETIME      NULL,
    [dv_update_user]                          VARCHAR (50)  NULL,
    [dv_first_in_key_series]                  BIT           NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_p_mdm_golden_record_customer_linkage]
    ON [dbo].[p_mdm_golden_record_customer_linkage]([bk_hash] ASC, [p_mdm_golden_record_customer_linkage_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[p_mdm_golden_record_customer_linkage]([dv_batch_id] ASC);

