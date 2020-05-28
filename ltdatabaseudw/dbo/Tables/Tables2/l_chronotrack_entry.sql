CREATE TABLE [dbo].[l_chronotrack_entry] (
    [l_chronotrack_entry_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                CHAR (32)      NOT NULL,
    [entry_id]               BIGINT         NULL,
    [race_id]                BIGINT         NULL,
    [trans_id]               BIGINT         NULL,
    [team_id]                BIGINT         NULL,
    [athlete_id]             BIGINT         NULL,
    [wave_id]                BIGINT         NULL,
    [prefered_bracket_id]    BIGINT         NULL,
    [primary_bracket_id]     BIGINT         NULL,
    [external_id]            NVARCHAR (255) NULL,
    [reg_option_id]          BIGINT         NULL,
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

