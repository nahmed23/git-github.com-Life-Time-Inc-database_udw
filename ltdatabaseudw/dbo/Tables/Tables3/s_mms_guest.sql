CREATE TABLE [dbo].[s_mms_guest] (
    [s_mms_guest_id]        BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]               CHAR (32)    NOT NULL,
    [guest_id]              INT          NULL,
    [card_number]           VARCHAR (25) NULL,
    [first_name]            VARCHAR (50) NULL,
    [middle_name]           VARCHAR (50) NULL,
    [last_name]             VARCHAR (50) NULL,
    [address_line1]         VARCHAR (50) NULL,
    [address_line2]         VARCHAR (50) NULL,
    [city]                  VARCHAR (50) NULL,
    [state]                 VARCHAR (15) NULL,
    [zip]                   VARCHAR (11) NULL,
    [inserted_date_time]    DATETIME     NULL,
    [updated_date_time]     DATETIME     NULL,
    [dv_load_date_time]     DATETIME     NOT NULL,
    [dv_batch_id]           BIGINT       NOT NULL,
    [dv_r_load_source_id]   BIGINT       NOT NULL,
    [dv_inserted_date_time] DATETIME     NOT NULL,
    [dv_insert_user]        VARCHAR (50) NOT NULL,
    [dv_updated_date_time]  DATETIME     NULL,
    [dv_update_user]        VARCHAR (50) NULL,
    [dv_hash]               CHAR (32)    NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_mms_guest]
    ON [dbo].[s_mms_guest]([bk_hash] ASC, [s_mms_guest_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_mms_guest]([dv_batch_id] ASC);

