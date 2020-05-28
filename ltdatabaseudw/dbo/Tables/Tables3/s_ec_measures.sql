CREATE TABLE [dbo].[s_ec_measures] (
    [s_ec_measures_id]                  BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                           CHAR (32)       NOT NULL,
    [measures_id]                       VARCHAR (36)    NULL,
    [slug]                              NVARCHAR (100)  NULL,
    [title]                             NVARCHAR (100)  NULL,
    [tags]                              NVARCHAR (4000) NULL,
    [description]                       NVARCHAR (500)  NULL,
    [unit]                              VARCHAR (100)   NULL,
    [measure_value_type]                INT             NULL,
    [extended_metadata]                 NVARCHAR (4000) NULL,
    [gender]                            NVARCHAR (10)   NULL,
    [optimum_range_male]                NVARCHAR (100)  NULL,
    [optimum_range_female]              NVARCHAR (100)  NULL,
    [diagonostic_range_male]            NVARCHAR (100)  NULL,
    [diagonostic_range_female]          NVARCHAR (100)  NULL,
    [created_by]                        INT             NULL,
    [created_date]                      DATETIME        NULL,
    [modified_by]                       INT             NULL,
    [modified_date]                     DATETIME        NULL,
    [measurement_type]                  INT             NULL,
    [measurement_instructions_location] NVARCHAR (200)  NULL,
    [dv_load_date_time]                 DATETIME        NOT NULL,
    [dv_r_load_source_id]               BIGINT          NOT NULL,
    [dv_inserted_date_time]             DATETIME        NOT NULL,
    [dv_insert_user]                    VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]              DATETIME        NULL,
    [dv_update_user]                    VARCHAR (50)    NULL,
    [dv_hash]                           CHAR (32)       NOT NULL,
    [dv_batch_id]                       BIGINT          NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_ec_measures]([dv_batch_id] ASC);

