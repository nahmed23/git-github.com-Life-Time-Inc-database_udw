CREATE TABLE [dbo].[stage_exerp_person] (
    [stage_exerp_person_id]  BIGINT         NOT NULL,
    [id]                     VARCHAR (4000) NULL,
    [center_id]              INT            NULL,
    [home_center_person_id]  INT            NULL,
    [home_center_id]         INT            NULL,
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
    [dv_batch_id]            BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

