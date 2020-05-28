CREATE TABLE [sandbox_ebi].[b_res] (
    [request_to_on_hold] INT  NULL,
    [date_time]          DATE NULL,
    [club_id]            INT  NULL,
    [move_to_hold]       INT  NULL,
    [membership_add]     INT  NULL,
    [move_from_hold]     INT  NULL,
    [membership_cancel]  INT  NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = ROUND_ROBIN);

