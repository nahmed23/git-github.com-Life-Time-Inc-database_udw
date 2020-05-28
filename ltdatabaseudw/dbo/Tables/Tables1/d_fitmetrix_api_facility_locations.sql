CREATE TABLE [dbo].[d_fitmetrix_api_facility_locations] (
    [d_fitmetrix_api_facility_locations_id]  BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                CHAR (32)     NOT NULL,
    [dim_fitmetrix_location_key]             CHAR (32)     NULL,
    [facility_location_id]                   INT           NULL,
    [address_city]                           VARCHAR (256) NULL,
    [address_country_abbreviation]           VARCHAR (256) NULL,
    [address_line_1]                         VARCHAR (256) NULL,
    [address_line_2]                         VARCHAR (256) NULL,
    [address_postal_code]                    VARCHAR (256) NULL,
    [address_state_or_province_abbreviation] VARCHAR (256) NULL,
    [dim_club_key]                           CHAR (32)     NULL,
    [email_from_name]                        VARCHAR (256) NULL,
    [latitude]                               VARCHAR (256) NULL,
    [location_name]                          VARCHAR (256) NULL,
    [longitude]                              VARCHAR (256) NULL,
    [phone]                                  VARCHAR (256) NULL,
    [p_fitmetrix_api_facility_locations_id]  BIGINT        NOT NULL,
    [deleted_flag]                           INT           NULL,
    [dv_load_date_time]                      DATETIME      NULL,
    [dv_load_end_date_time]                  DATETIME      NULL,
    [dv_batch_id]                            BIGINT        NOT NULL,
    [dv_inserted_date_time]                  DATETIME      NOT NULL,
    [dv_insert_user]                         VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]                   DATETIME      NULL,
    [dv_update_user]                         VARCHAR (50)  NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_fitmetrix_api_facility_locations]([dv_batch_id] ASC);

