CREATE TABLE [dbo].[dim_mms_membership_history_partial_20190916] (
    [eom_date]             VARCHAR (13)  NOT NULL,
    [membership_id]        INT           NULL,
    [effective_date_time]  DATETIME      NULL,
    [expiration_date_time] DATETIME      NULL,
    [product_id]           INT           NULL,
    [membership_status]    VARCHAR (100) NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = ROUND_ROBIN);

