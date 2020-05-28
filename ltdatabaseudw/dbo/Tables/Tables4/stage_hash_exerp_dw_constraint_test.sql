﻿CREATE TABLE [dbo].[stage_hash_exerp_dw_constraint_test] (
    [stage_hash_exerp_dw_constraint_test_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                CHAR (32)      NOT NULL,
    [test_number]                            VARCHAR (4000) NULL,
    [table_1]                                VARCHAR (4000) NULL,
    [table_2]                                VARCHAR (4000) NULL,
    [foreign_key]                            VARCHAR (4000) NULL,
    [primary_key]                            VARCHAR (4000) NULL,
    [nullable]                               BIT            NULL,
    [relationship]                           VARCHAR (4000) NULL,
    [extra_con]                              VARCHAR (4000) NULL,
    [test_query]                             VARCHAR (4000) NULL,
    [dummy_modified_date_time]               DATETIME       NULL,
    [dv_load_date_time]                      DATETIME       NOT NULL,
    [dv_inserted_date_time]                  DATETIME       NOT NULL,
    [dv_insert_user]                         VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]                   DATETIME       NULL,
    [dv_update_user]                         VARCHAR (50)   NULL,
    [dv_batch_id]                            BIGINT         NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));

