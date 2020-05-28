CREATE TABLE [dbo].[stage_exerp_participation] (
    [stage_exerp_participation_id] BIGINT         NOT NULL,
    [id]                           VARCHAR (4000) NULL,
    [booking_id]                   VARCHAR (4000) NULL,
    [center_id]                    INT            NULL,
    [person_id]                    VARCHAR (4000) NULL,
    [creation_datetime]            DATETIME       NULL,
    [state]                        VARCHAR (4000) NULL,
    [user_interface_type]          VARCHAR (4000) NULL,
    [show_up_datetime]             DATETIME       NULL,
    [show_up_interface_type]       VARCHAR (4000) NULL,
    [showup_using_card]            BIT            NULL,
    [cancel_datetime]              DATETIME       NULL,
    [cancel_interface_type]        VARCHAR (4000) NULL,
    [cancel_reason]                VARCHAR (4000) NULL,
    [was_on_waiting_list]          BIT            NULL,
    [ets]                          BIGINT         NULL,
    [seat_obtained_datetime]       DATETIME       NULL,
    [participant_number]           INT            NULL,
    [seat_id]                      VARCHAR (4000) NULL,
    [seat_state]                   VARCHAR (4000) NULL,
    [dv_batch_id]                  BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

