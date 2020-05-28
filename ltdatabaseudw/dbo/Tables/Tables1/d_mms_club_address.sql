CREATE TABLE [dbo].[d_mms_club_address] (
    [d_mms_club_address_id]       BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                     CHAR (32)       NOT NULL,
    [dim_mms_club_address_key]    CHAR (32)       NULL,
    [club_address_id]             INT             NULL,
    [address_line_1]              VARCHAR (50)    NULL,
    [address_line_2]              VARCHAR (50)    NULL,
    [city]                        VARCHAR (50)    NULL,
    [club_id]                     INT             NULL,
    [country_dim_description_key] VARCHAR (532)   NULL,
    [dim_club_key]                CHAR (32)       NULL,
    [latitude]                    DECIMAL (20, 9) NULL,
    [longitude]                   DECIMAL (20, 9) NULL,
    [postal_code]                 VARCHAR (11)    NULL,
    [state_dim_description_key]   VARCHAR (532)   NULL,
    [val_address_type_id]         INT             NULL,
    [p_mms_club_address_id]       BIGINT          NOT NULL,
    [dv_load_date_time]           DATETIME        NULL,
    [dv_load_end_date_time]       DATETIME        NULL,
    [dv_batch_id]                 BIGINT          NOT NULL,
    [dv_inserted_date_time]       DATETIME        NOT NULL,
    [dv_insert_user]              VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]        DATETIME        NULL,
    [dv_update_user]              VARCHAR (50)    NULL
)
WITH (HEAP, DISTRIBUTION = REPLICATE);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_mms_club_address]([dv_batch_id] ASC);

