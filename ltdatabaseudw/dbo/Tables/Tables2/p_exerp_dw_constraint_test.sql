﻿CREATE TABLE [dbo].[p_exerp_dw_constraint_test] (
    [p_exerp_dw_constraint_test_id]        BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                              CHAR (32)      NOT NULL,
    [test_number]                          VARCHAR (4000) NULL,
    [s_exerp_dw_constraint_test_id]        BIGINT         NULL,
    [dv_load_date_time]                    DATETIME       NOT NULL,
    [dv_load_end_date_time]                DATETIME       NOT NULL,
    [dv_greatest_satellite_date_time]      DATETIME       NULL,
    [dv_next_greatest_satellite_date_time] DATETIME       NULL,
    [dv_first_in_key_series]               INT            NULL,
    [dv_inserted_date_time]                DATETIME       NOT NULL,
    [dv_insert_user]                       VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]                 DATETIME       NULL,
    [dv_update_user]                       VARCHAR (50)   NULL,
    [dv_batch_id]                          BIGINT         NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[p_exerp_dw_constraint_test]([dv_batch_id] ASC);

