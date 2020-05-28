CREATE PROC [dbo].[proc_etl_mms_pt_credit_card_terminal] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_mms_PTCreditCardTerminal

set @insert_date_time = getdate()
insert into dbo.stage_hash_mms_PTCreditCardTerminal (
       bk_hash,
       PTCreditCardTerminalID,
       Name,
       Description,
       ValPTCreditCardClientNumberID,
       MerchantNumber,
       TerminalNumber,
       ClubID,
       ValCreditCardTerminalLocationID,
       InsertedDateTime,
       UpdatedDateTime,
       DrawerID,
       TerminalAreaID,
       TerminalStatus,
       EmployeeID,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(PTCreditCardTerminalID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       PTCreditCardTerminalID,
       Name,
       Description,
       ValPTCreditCardClientNumberID,
       MerchantNumber,
       TerminalNumber,
       ClubID,
       ValCreditCardTerminalLocationID,
       InsertedDateTime,
       UpdatedDateTime,
       DrawerID,
       TerminalAreaID,
       TerminalStatus,
       EmployeeID,
       isnull(cast(stage_mms_PTCreditCardTerminal.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_mms_PTCreditCardTerminal
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_mms_pt_credit_card_terminal @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_mms_pt_credit_card_terminal (
       bk_hash,
       pt_credit_card_terminal_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_mms_PTCreditCardTerminal.bk_hash,
       stage_hash_mms_PTCreditCardTerminal.PTCreditCardTerminalID pt_credit_card_terminal_id,
       isnull(cast(stage_hash_mms_PTCreditCardTerminal.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       2,
       @insert_date_time,
       @user
  from stage_hash_mms_PTCreditCardTerminal
  left join h_mms_pt_credit_card_terminal
    on stage_hash_mms_PTCreditCardTerminal.bk_hash = h_mms_pt_credit_card_terminal.bk_hash
 where h_mms_pt_credit_card_terminal_id is null
   and stage_hash_mms_PTCreditCardTerminal.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_mms_pt_credit_card_terminal
if object_id('tempdb..#l_mms_pt_credit_card_terminal_inserts') is not null drop table #l_mms_pt_credit_card_terminal_inserts
create table #l_mms_pt_credit_card_terminal_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_PTCreditCardTerminal.bk_hash,
       stage_hash_mms_PTCreditCardTerminal.PTCreditCardTerminalID pt_credit_card_terminal_id,
       stage_hash_mms_PTCreditCardTerminal.ValPTCreditCardClientNumberID val_pt_credit_card_client_number_id,
       stage_hash_mms_PTCreditCardTerminal.MerchantNumber merchant_number,
       stage_hash_mms_PTCreditCardTerminal.TerminalNumber terminal_number,
       stage_hash_mms_PTCreditCardTerminal.ClubID club_id,
       stage_hash_mms_PTCreditCardTerminal.ValCreditCardTerminalLocationID val_credit_card_terminal_location_id,
       stage_hash_mms_PTCreditCardTerminal.DrawerID drawer_id,
       stage_hash_mms_PTCreditCardTerminal.TerminalAreaID terminal_area_id,
       stage_hash_mms_PTCreditCardTerminal.EmployeeID employee_id,
       stage_hash_mms_PTCreditCardTerminal.InsertedDateTime dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_PTCreditCardTerminal.PTCreditCardTerminalID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PTCreditCardTerminal.ValPTCreditCardClientNumberID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PTCreditCardTerminal.MerchantNumber as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PTCreditCardTerminal.TerminalNumber as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PTCreditCardTerminal.ClubID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PTCreditCardTerminal.ValCreditCardTerminalLocationID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PTCreditCardTerminal.DrawerID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PTCreditCardTerminal.TerminalAreaID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PTCreditCardTerminal.EmployeeID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_PTCreditCardTerminal
 where stage_hash_mms_PTCreditCardTerminal.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_mms_pt_credit_card_terminal records
set @insert_date_time = getdate()
insert into l_mms_pt_credit_card_terminal (
       bk_hash,
       pt_credit_card_terminal_id,
       val_pt_credit_card_client_number_id,
       merchant_number,
       terminal_number,
       club_id,
       val_credit_card_terminal_location_id,
       drawer_id,
       terminal_area_id,
       employee_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_mms_pt_credit_card_terminal_inserts.bk_hash,
       #l_mms_pt_credit_card_terminal_inserts.pt_credit_card_terminal_id,
       #l_mms_pt_credit_card_terminal_inserts.val_pt_credit_card_client_number_id,
       #l_mms_pt_credit_card_terminal_inserts.merchant_number,
       #l_mms_pt_credit_card_terminal_inserts.terminal_number,
       #l_mms_pt_credit_card_terminal_inserts.club_id,
       #l_mms_pt_credit_card_terminal_inserts.val_credit_card_terminal_location_id,
       #l_mms_pt_credit_card_terminal_inserts.drawer_id,
       #l_mms_pt_credit_card_terminal_inserts.terminal_area_id,
       #l_mms_pt_credit_card_terminal_inserts.employee_id,
       case when l_mms_pt_credit_card_terminal.l_mms_pt_credit_card_terminal_id is null then isnull(#l_mms_pt_credit_card_terminal_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #l_mms_pt_credit_card_terminal_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_mms_pt_credit_card_terminal_inserts
  left join p_mms_pt_credit_card_terminal
    on #l_mms_pt_credit_card_terminal_inserts.bk_hash = p_mms_pt_credit_card_terminal.bk_hash
   and p_mms_pt_credit_card_terminal.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_mms_pt_credit_card_terminal
    on p_mms_pt_credit_card_terminal.bk_hash = l_mms_pt_credit_card_terminal.bk_hash
   and p_mms_pt_credit_card_terminal.l_mms_pt_credit_card_terminal_id = l_mms_pt_credit_card_terminal.l_mms_pt_credit_card_terminal_id
 where l_mms_pt_credit_card_terminal.l_mms_pt_credit_card_terminal_id is null
    or (l_mms_pt_credit_card_terminal.l_mms_pt_credit_card_terminal_id is not null
        and l_mms_pt_credit_card_terminal.dv_hash <> #l_mms_pt_credit_card_terminal_inserts.source_hash)

--calculate hash and lookup to current s_mms_pt_credit_card_terminal
if object_id('tempdb..#s_mms_pt_credit_card_terminal_inserts') is not null drop table #s_mms_pt_credit_card_terminal_inserts
create table #s_mms_pt_credit_card_terminal_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_PTCreditCardTerminal.bk_hash,
       stage_hash_mms_PTCreditCardTerminal.PTCreditCardTerminalID pt_credit_card_terminal_id,
       stage_hash_mms_PTCreditCardTerminal.Name name,
       stage_hash_mms_PTCreditCardTerminal.Description description,
       stage_hash_mms_PTCreditCardTerminal.InsertedDateTime inserted_date_time,
       stage_hash_mms_PTCreditCardTerminal.UpdatedDateTime updated_date_time,
       stage_hash_mms_PTCreditCardTerminal.TerminalStatus terminal_status,
       stage_hash_mms_PTCreditCardTerminal.InsertedDateTime dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_PTCreditCardTerminal.PTCreditCardTerminalID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_PTCreditCardTerminal.Name,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_PTCreditCardTerminal.Description,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_PTCreditCardTerminal.InsertedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_PTCreditCardTerminal.UpdatedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PTCreditCardTerminal.TerminalStatus as varchar(42)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_PTCreditCardTerminal
 where stage_hash_mms_PTCreditCardTerminal.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mms_pt_credit_card_terminal records
set @insert_date_time = getdate()
insert into s_mms_pt_credit_card_terminal (
       bk_hash,
       pt_credit_card_terminal_id,
       name,
       description,
       inserted_date_time,
       updated_date_time,
       terminal_status,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mms_pt_credit_card_terminal_inserts.bk_hash,
       #s_mms_pt_credit_card_terminal_inserts.pt_credit_card_terminal_id,
       #s_mms_pt_credit_card_terminal_inserts.name,
       #s_mms_pt_credit_card_terminal_inserts.description,
       #s_mms_pt_credit_card_terminal_inserts.inserted_date_time,
       #s_mms_pt_credit_card_terminal_inserts.updated_date_time,
       #s_mms_pt_credit_card_terminal_inserts.terminal_status,
       case when s_mms_pt_credit_card_terminal.s_mms_pt_credit_card_terminal_id is null then isnull(#s_mms_pt_credit_card_terminal_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #s_mms_pt_credit_card_terminal_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mms_pt_credit_card_terminal_inserts
  left join p_mms_pt_credit_card_terminal
    on #s_mms_pt_credit_card_terminal_inserts.bk_hash = p_mms_pt_credit_card_terminal.bk_hash
   and p_mms_pt_credit_card_terminal.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mms_pt_credit_card_terminal
    on p_mms_pt_credit_card_terminal.bk_hash = s_mms_pt_credit_card_terminal.bk_hash
   and p_mms_pt_credit_card_terminal.s_mms_pt_credit_card_terminal_id = s_mms_pt_credit_card_terminal.s_mms_pt_credit_card_terminal_id
 where s_mms_pt_credit_card_terminal.s_mms_pt_credit_card_terminal_id is null
    or (s_mms_pt_credit_card_terminal.s_mms_pt_credit_card_terminal_id is not null
        and s_mms_pt_credit_card_terminal.dv_hash <> #s_mms_pt_credit_card_terminal_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_mms_pt_credit_card_terminal @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_mms_pt_credit_card_terminal @current_dv_batch_id

end
