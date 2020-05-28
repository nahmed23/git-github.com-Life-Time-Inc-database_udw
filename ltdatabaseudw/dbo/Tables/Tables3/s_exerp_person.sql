CREATE TABLE [dbo].[s_exerp_person] (
    [s_exerp_person_id]      BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                CHAR (32)      NOT NULL,
    [person_id]              VARCHAR (4000) NULL,
    [ets]                    BIGINT         NULL,
    [can_sms]                BIT            NULL,
    [can_email]              BIT            NULL,
    [employee_title]         VARCHAR (4000) NULL,
    [staff_external_id]      VARCHAR (4000) NULL,
    [state]                  VARCHAR (4000) NULL,
    [county]                 VARCHAR (4000) NULL,
    [company_id]             VARCHAR (4000) NULL,
    [payer_person_id]        VARCHAR (4000) NULL,
    [person_status]          VARCHAR (4000) NULL,
    [person_type]            VARCHAR (4000) NULL,
    [gender]                 VARCHAR (4000) NULL,
    [city]                   VARCHAR (4000) NULL,
    [postal_code]            VARCHAR (4000) NULL,
    [country_id]             VARCHAR (4000) NULL,
    [title]                  VARCHAR (4000) NULL,
    [duplicate_of_person_id] VARCHAR (4000) NULL,
    [creation_date]          DATETIME       NULL,
    [date_of_birth]          DATETIME       NULL,
    [dv_load_date_time]      DATETIME       NOT NULL,
    [dv_r_load_source_id]    BIGINT         NOT NULL,
    [dv_inserted_date_time]  DATETIME       NOT NULL,
    [dv_insert_user]         VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]   DATETIME       NULL,
    [dv_update_user]         VARCHAR (50)   NULL,
    [dv_hash]                CHAR (32)      NOT NULL,
    [dv_deleted]             BIT            DEFAULT ((0)) NOT NULL,
    [dv_batch_id]            BIGINT         NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_exerp_person]([dv_batch_id] ASC);

