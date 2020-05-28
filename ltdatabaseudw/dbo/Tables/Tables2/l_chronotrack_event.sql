CREATE TABLE [dbo].[l_chronotrack_event] (
    [l_chronotrack_event_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                CHAR (32)      NOT NULL,
    [event_id]               BIGINT         NULL,
    [event_group_id]         BIGINT         NULL,
    [location_id]            BIGINT         NULL,
    [organizer_id]           BIGINT         NULL,
    [timer_id]               BIGINT         NULL,
    [currency_id]            NCHAR (3)      NULL,
    [payment_location_id]    BIGINT         NULL,
    [last_yrs_event_id]      BIGINT         NULL,
    [parent_event_id]        BIGINT         NULL,
    [external_id]            NVARCHAR (255) NULL,
    [online_payee_id]        BIGINT         NULL,
    [series_id]              BIGINT         NULL,
    [language_id]            NCHAR (2)      NULL,
    [onsite_payee_id]        BIGINT         NULL,
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
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

