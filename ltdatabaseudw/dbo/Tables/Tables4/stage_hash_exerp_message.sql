CREATE TABLE [dbo].[stage_hash_exerp_message] (
    [stage_hash_exerp_message_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                     CHAR (32)      NOT NULL,
    [id]                          VARCHAR (4000) NULL,
    [person_id]                   VARCHAR (4000) NULL,
    [company_id]                  VARCHAR (4000) NULL,
    [creation_datetime]           DATETIME       NULL,
    [delivery_datetime]           DATETIME       NULL,
    [delivery_method]             VARCHAR (4000) NULL,
    [delivered_by_person_id]      VARCHAR (4000) NULL,
    [template_id]                 INT            NULL,
    [type]                        VARCHAR (4000) NULL,
    [ref_type]                    VARCHAR (4000) NULL,
    [ref_id]                      VARCHAR (4000) NULL,
    [subject]                     VARCHAR (4000) NULL,
    [from_person_id]              VARCHAR (4000) NULL,
    [channel]                     VARCHAR (4000) NULL,
    [message_category]            VARCHAR (4000) NULL,
    [center_id]                   INT            NULL,
    [ets]                         BIGINT         NULL,
    [dv_load_date_time]           DATETIME       NOT NULL,
    [dv_updated_date_time]        DATETIME       NULL,
    [dv_update_user]              VARCHAR (50)   NULL,
    [dv_batch_id]                 BIGINT         NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));

