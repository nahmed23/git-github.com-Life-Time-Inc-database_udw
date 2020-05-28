CREATE TABLE [dbo].[s_fitmetrix_api_instructor] (
    [s_fitmetrix_api_instructor_id]     BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                           CHAR (32)     NOT NULL,
    [instructor_id]                     INT           NULL,
    [first_name]                        VARCHAR (255) NULL,
    [last_name]                         VARCHAR (255) NULL,
    [description]                       VARCHAR (255) NULL,
    [image]                             VARCHAR (255) NULL,
    [email]                             VARCHAR (255) NULL,
    [street_1]                          VARCHAR (255) NULL,
    [street_2]                          VARCHAR (255) NULL,
    [city]                              VARCHAR (255) NULL,
    [state]                             VARCHAR (255) NULL,
    [zip]                               VARCHAR (255) NULL,
    [country]                           VARCHAR (255) NULL,
    [bio]                               VARCHAR (255) NULL,
    [home_phone]                        VARCHAR (255) NULL,
    [work_phone]                        VARCHAR (255) NULL,
    [gender]                            VARCHAR (255) NULL,
    [quote]                             VARCHAR (255) NULL,
    [show_online]                       VARCHAR (255) NULL,
    [active]                            VARCHAR (255) NULL,
    [display_order]                     INT           NULL,
    [facebook_url]                      VARCHAR (255) NULL,
    [twitter_url]                       VARCHAR (255) NULL,
    [instagram_url]                     VARCHAR (255) NULL,
    [sound_cloud_url]                   VARCHAR (255) NULL,
    [spotify_url]                       VARCHAR (255) NULL,
    [deleted]                           VARCHAR (255) NULL,
    [extra_field_1]                     VARCHAR (255) NULL,
    [extra_field_2]                     VARCHAR (255) NULL,
    [extra_field_3]                     VARCHAR (255) NULL,
    [receive_cancel_and_register_email] VARCHAR (255) NULL,
    [dummy_modified_date_time]          DATETIME      NULL,
    [dv_load_date_time]                 DATETIME      NOT NULL,
    [dv_r_load_source_id]               BIGINT        NOT NULL,
    [dv_inserted_date_time]             DATETIME      NOT NULL,
    [dv_insert_user]                    VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]              DATETIME      NULL,
    [dv_update_user]                    VARCHAR (50)  NULL,
    [dv_hash]                           CHAR (32)     NOT NULL,
    [dv_batch_id]                       BIGINT        NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_fitmetrix_api_instructor]([dv_batch_id] ASC);

