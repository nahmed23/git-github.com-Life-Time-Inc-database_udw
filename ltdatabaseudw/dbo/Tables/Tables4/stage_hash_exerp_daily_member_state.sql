CREATE TABLE [dbo].[stage_hash_exerp_daily_member_state] (
    [stage_hash_exerp_daily_member_state_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                CHAR (32)      NOT NULL,
    [id]                                     INT            NULL,
    [person_id]                              VARCHAR (4000) NULL,
    [center_id]                              INT            NULL,
    [home_center_person_id]                  INT            NULL,
    [date]                                   DATETIME       NULL,
    [entry_datetime]                         DATETIME       NULL,
    [change]                                 VARCHAR (4000) NULL,
    [member_number_delta]                    INT            NULL,
    [extra_number_delta]                     INT            NULL,
    [secondary_member_number_delta]          INT            NULL,
    [cancel_datetime]                        DATETIME       NULL,
    [ets]                                    BIGINT         NULL,
    [dummy_modified_date_time]               DATETIME       NULL,
    [dv_load_date_time]                      DATETIME       NOT NULL,
    [dv_updated_date_time]                   DATETIME       NULL,
    [dv_update_user]                         VARCHAR (50)   NULL,
    [dv_batch_id]                            BIGINT         NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));

