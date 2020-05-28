CREATE TABLE [dbo].[stage_hash_mms_ClubAddress] (
    [stage_hash_mms_ClubAddress_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                       CHAR (32)       NOT NULL,
    [ClubAddressID]                 INT             NULL,
    [ClubID]                        INT             NULL,
    [AddressLine1]                  VARCHAR (50)    NULL,
    [AddressLine2]                  VARCHAR (50)    NULL,
    [City]                          VARCHAR (50)    NULL,
    [ValAddressTypeID]              INT             NULL,
    [Zip]                           VARCHAR (11)    NULL,
    [InsertedDateTime]              DATETIME        NULL,
    [ValCountryID]                  INT             NULL,
    [ValStateID]                    INT             NULL,
    [UpdatedDateTime]               DATETIME        NULL,
    [Latitude]                      DECIMAL (20, 9) NULL,
    [Longitude]                     DECIMAL (20, 9) NULL,
    [MapCenterLatitude]             DECIMAL (10, 4) NULL,
    [MapCenterLongitude]            DECIMAL (10, 4) NULL,
    [MapZoomLevel]                  INT             NULL,
    [dv_load_date_time]             DATETIME        NOT NULL,
    [dv_inserted_date_time]         DATETIME        NOT NULL,
    [dv_insert_user]                VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]          DATETIME        NULL,
    [dv_update_user]                VARCHAR (50)    NULL,
    [dv_batch_id]                   BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

