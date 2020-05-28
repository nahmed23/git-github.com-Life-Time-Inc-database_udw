﻿CREATE TABLE [sandbox_ebi].[fact_exerp_participation] (
    [fact_exerp_participation_key]         VARCHAR (32)   NULL,
    [participation_id]                     VARCHAR (4000) NULL,
    [booking_dim_date_key]                 CHAR (8)       NULL,
    [booking_dim_time_key]                 CHAR (8)       NULL,
    [billable_flag]                        VARCHAR (1)    NOT NULL,
    [booking_cancelled_flag]               VARCHAR (1)    NOT NULL,
    [participation_cancelled_flag]         VARCHAR (1)    NOT NULL,
    [participation_cancelled_no_show_flag] VARCHAR (1)    NOT NULL,
    [cancel_dim_date_key]                  CHAR (8)       NULL,
    [cancel_dim_time_key]                  CHAR (8)       NULL,
    [cancel_interface_type]                VARCHAR (4000) NULL,
    [cancel_reason]                        VARCHAR (4000) NULL,
    [creation_dim_date_key]                CHAR (8)       NULL,
    [creation_dim_time_key]                CHAR (8)       NULL,
    [dim_club_key]                         CHAR (32)      NULL,
    [dim_exerp_booking_key]                CHAR (32)      NULL,
    [dim_exerp_activity_key]               CHAR (32)      NULL,
    [dim_exerp_product_key]                CHAR (32)      NULL,
    [dim_mms_product_key]                  VARCHAR (32)   NULL,
    [clipcard_flag]                        VARCHAR (1)    NOT NULL,
    [subscription_flag]                    VARCHAR (1)    NOT NULL,
    [dim_exerp_clipcard_key]               VARCHAR (32)   NULL,
    [dim_exerp_subscription_key]           VARCHAR (32)   NULL,
    [dim_exerp_subscription_period_key]    VARCHAR (32)   NULL,
    [dim_mms_member_key]                   VARCHAR (32)   NULL,
    [ets]                                  BIGINT         NULL,
    [participated_flag]                    VARCHAR (1)    NOT NULL,
    [participation_state]                  VARCHAR (4000) NULL,
    [show_up_dim_date_key]                 CHAR (8)       NULL,
    [show_up_dim_time_key]                 CHAR (8)       NULL,
    [show_up_interface_type]               VARCHAR (4000) NULL,
    [show_up_using_card_flag]              CHAR (1)       NULL,
    [user_interface_type]                  VARCHAR (4000) NULL,
    [was_on_waiting_list_flag]             CHAR (1)       NULL,
    [seat_obtained_dim_date_key]           VARCHAR (8)    NULL,
    [seat_obtained_dim_time_key]           INT            NULL,
    [participant_number]                   INT            NULL,
    [seat_id]                              VARCHAR (4000) NULL,
    [seat_state]                           VARCHAR (4000) NULL,
    [dv_load_date_time]                    DATETIME       NULL,
    [dv_load_end_date_time]                DATETIME       NULL,
    [dv_batch_id]                          BIGINT         NULL,
    [fact_exerp_transaction_log_key]       VARCHAR (4000) NULL,
    [future_booking_flag]                  INT            NOT NULL,
    [class_capacity]                       INT            NULL,
    [waitlist_flag]                        VARCHAR (1)    NOT NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = ROUND_ROBIN);

