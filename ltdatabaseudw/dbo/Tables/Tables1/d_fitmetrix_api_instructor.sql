CREATE TABLE [dbo].[d_fitmetrix_api_instructor] (
    [d_fitmetrix_api_instructor_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                       CHAR (32)     NOT NULL,
    [dim_fitmetrix_instructor_key]  CHAR (32)     NULL,
    [instructor_id]                 INT           NULL,
    [dim_employee_key]              CHAR (32)     NULL,
    [dim_fitmetrix_location_key]    CHAR (32)     NULL,
    [email]                         VARCHAR (255) NULL,
    [gender]                        VARCHAR (255) NULL,
    [name]                          VARCHAR (255) NULL,
    [p_fitmetrix_api_instructor_id] BIGINT        NOT NULL,
    [deleted_flag]                  INT           NULL,
    [dv_load_date_time]             DATETIME      NULL,
    [dv_load_end_date_time]         DATETIME      NULL,
    [dv_batch_id]                   BIGINT        NOT NULL,
    [dv_inserted_date_time]         DATETIME      NOT NULL,
    [dv_insert_user]                VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]          DATETIME      NULL,
    [dv_update_user]                VARCHAR (50)  NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_fitmetrix_api_instructor]([dv_batch_id] ASC);

