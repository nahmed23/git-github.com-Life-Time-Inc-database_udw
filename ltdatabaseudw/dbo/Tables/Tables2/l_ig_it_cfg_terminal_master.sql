CREATE TABLE [dbo].[l_ig_it_cfg_terminal_master] (
    [l_ig_it_cfg_terminal_master_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                        CHAR (32)    NOT NULL,
    [term_id]                        INT          NULL,
    [term_grp_id]                    INT          NULL,
    [term_printer_grp_id]            INT          NULL,
    [term_service_grp_id]            INT          NULL,
    [term_option_grp_id]             INT          NULL,
    [primary_profit_center_id]       INT          NULL,
    [alt_rcpt_term_id]               INT          NULL,
    [profile_id]                     INT          NULL,
    [alt_bargun_term_id]             INT          NULL,
    [dv_load_date_time]              DATETIME     NOT NULL,
    [dv_r_load_source_id]            BIGINT       NOT NULL,
    [dv_inserted_date_time]          DATETIME     NOT NULL,
    [dv_insert_user]                 VARCHAR (50) NOT NULL,
    [dv_updated_date_time]           DATETIME     NULL,
    [dv_update_user]                 VARCHAR (50) NULL,
    [dv_hash]                        CHAR (32)    NOT NULL,
    [dv_deleted]                     BIT          DEFAULT ((0)) NOT NULL,
    [dv_batch_id]                    BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

