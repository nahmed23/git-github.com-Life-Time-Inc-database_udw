CREATE TABLE [dbo].[d_fitmetrix_api_appointment_id_statistics] (
    [d_fitmetrix_api_appointment_id_statistics_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                      CHAR (32)     NOT NULL,
    [fact_fitmetrix_appointment_detail_key]        CHAR (32)     NULL,
    [profile_appointment_id]                       INT           NULL,
    [appointment_name]                             VARCHAR (255) NULL,
    [checked_in_flag]                              CHAR (1)      NULL,
    [created_dim_date_key]                         CHAR (8)      NULL,
    [created_dim_time_key]                         INT           NULL,
    [dim_fitmetrix_appointment_key]                CHAR (32)     NULL,
    [dim_mms_member_key]                           CHAR (32)     NULL,
    [email_address]                                VARCHAR (255) NULL,
    [first_name]                                   VARCHAR (255) NULL,
    [last_name]                                    VARCHAR (255) NULL,
    [spot_number]                                  INT           NULL,
    [start_dim_date_key]                           CHAR (8)      NULL,
    [start_dim_time_key]                           INT           NULL,
    [total_points]                                 INT           NULL,
    [waitlist_dim_date_key]                        CHAR (8)      NULL,
    [waitlist_dim_time_key]                        INT           NULL,
    [waitlist_flag]                                CHAR (1)      NULL,
    [waitlist_position]                            INT           NULL,
    [p_fitmetrix_api_appointment_id_statistics_id] BIGINT        NOT NULL,
    [deleted_flag]                                 INT           NULL,
    [dv_load_date_time]                            DATETIME      NULL,
    [dv_load_end_date_time]                        DATETIME      NULL,
    [dv_batch_id]                                  BIGINT        NOT NULL,
    [dv_inserted_date_time]                        DATETIME      NOT NULL,
    [dv_insert_user]                               VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]                         DATETIME      NULL,
    [dv_update_user]                               VARCHAR (50)  NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_fitmetrix_api_appointment_id_statistics]([dv_batch_id] ASC);

