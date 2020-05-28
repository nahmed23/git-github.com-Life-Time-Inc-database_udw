CREATE TABLE [dbo].[stage_hash_mms_PaymentRefundContact] (
    [stage_hash_mms_PaymentRefundContact_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                CHAR (32)    NOT NULL,
    [PaymentRefundContactID]                 INT          NULL,
    [FirstName]                              VARCHAR (50) NULL,
    [LastName]                               VARCHAR (50) NULL,
    [MiddleInit]                             CHAR (1)     NULL,
    [PhoneAreaCode]                          VARCHAR (3)  NULL,
    [PhoneNumber]                            VARCHAR (7)  NULL,
    [AddressLine1]                           VARCHAR (50) NULL,
    [AddressLine2]                           VARCHAR (50) NULL,
    [City]                                   VARCHAR (50) NULL,
    [Zip]                                    VARCHAR (11) NULL,
    [ValCountryID]                           TINYINT      NULL,
    [ValStateID]                             SMALLINT     NULL,
    [PaymentRefundID]                        INT          NULL,
    [InsertedDateTime]                       DATETIME     NULL,
    [UpdatedDateTime]                        DATETIME     NULL,
    [dv_load_date_time]                      DATETIME     NOT NULL,
    [dv_inserted_date_time]                  DATETIME     NOT NULL,
    [dv_insert_user]                         VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                   DATETIME     NULL,
    [dv_update_user]                         VARCHAR (50) NULL,
    [dv_batch_id]                            BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

