CREATE TABLE [dbo].[stage_hash_exerp_debt_case] (
    [stage_hash_exerp_debt_case_id] BIGINT           IDENTITY (1, 1) NOT NULL,
    [bk_hash]                       CHAR (32)        NOT NULL,
    [id]                            VARCHAR (4000)   NULL,
    [center_id]                     INT              NULL,
    [person_id]                     VARCHAR (4000)   NULL,
    [company_id]                    VARCHAR (4000)   NULL,
    [start_datetime]                DATETIME         NULL,
    [amount]                        NUMERIC (18, 10) NULL,
    [closed]                        BIT              NULL,
    [closed_datetime]               DATETIME         NULL,
    [current_step]                  INT              NULL,
    [ets]                           BIGINT           NULL,
    [dummy_modified_date_time]      DATETIME         NULL,
    [dv_load_date_time]             DATETIME         NOT NULL,
    [dv_inserted_date_time]         DATETIME         NOT NULL,
    [dv_insert_user]                VARCHAR (50)     NOT NULL,
    [dv_updated_date_time]          DATETIME         NULL,
    [dv_update_user]                VARCHAR (50)     NULL,
    [dv_batch_id]                   BIGINT           NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));

