﻿CREATE TABLE [dbo].[d_ec_plans] (
    [d_ec_plans_id]                           BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                 CHAR (32)       NOT NULL,
    [dim_trainerize_plan_key]                 VARCHAR (32)    NULL,
    [plan_id]                                 INT             NULL,
    [coach_d_ltfeb_ltf_user_identity_bk_hash] VARCHAR (32)    NULL,
    [coach_party_id]                          INT             NULL,
    [created_dim_date_key]                    VARCHAR (8)     NULL,
    [d_ltfeb_ltf_user_identity_bk_hash]       VARCHAR (32)    NULL,
    [dim_trainerize_program_key]              VARCHAR (32)    NULL,
    [duration]                                NVARCHAR (50)   NULL,
    [duration_type]                           INT             NULL,
    [end_dim_date_key]                        VARCHAR (8)     NULL,
    [end_dim_time_key]                        INT             NULL,
    [party_id]                                INT             NULL,
    [plan_name]                               NVARCHAR (4000) NULL,
    [source_id]                               NVARCHAR (50)   NULL,
    [source_type]                             INT             NULL,
    [start_dim_date_key]                      VARCHAR (8)     NULL,
    [start_dim_time_key]                      INT             NULL,
    [updated_dim_date_key]                    VARCHAR (8)     NULL,
    [p_ec_plans_id]                           BIGINT          NOT NULL,
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
