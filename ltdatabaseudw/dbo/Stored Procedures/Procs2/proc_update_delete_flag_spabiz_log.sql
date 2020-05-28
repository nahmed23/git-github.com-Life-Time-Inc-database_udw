CREATE PROC [dbo].[proc_update_delete_flag_spabiz_log] @dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--deletes
if object_id('tempdb..#deletes') is not null drop table #deletes
create table dbo.#deletes with(distribution=hash(ap_data_id_hash)) as
select min(p.dv_load_date_time) min_dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l.ap_data_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l.store_number as varchar(500)),'z#@$k%&P'))),2) ap_data_id_hash
from p_spabiz_log p
join s_spabiz_log s on p.s_spabiz_log_id = s.s_spabiz_log_id and p.bk_hash = s.bk_hash
join l_spabiz_log l on p.l_spabiz_log_id = l.l_spabiz_log_id and p.bk_hash = l.bk_hash
where p.dv_batch_id >= @dv_batch_id
and s.action in (3,6,54)
group by convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l.ap_data_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l.store_number as varchar(500)),'z#@$k%&P'))),2)

declare @u varchar(500) = suser_sname()

update h_spabiz_ap_data
set dv_deleted = 1,
    dv_updated_date_time = getdate(),
    dv_update_user = @u
where bk_hash in (select ap_data_id_hash from #deletes)

end
