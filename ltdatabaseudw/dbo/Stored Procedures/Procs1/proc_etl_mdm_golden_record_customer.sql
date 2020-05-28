CREATE PROC [dbo].[proc_etl_mdm_golden_record_customer] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_mdm_GoldenRecordCustomer

set @insert_date_time = getdate()
insert into dbo.stage_hash_mdm_GoldenRecordCustomer (
       bk_hash,
       LoadDateTime,
       RowNumber,
       EntityID,
       SourceID,
       SourceCode,
       BirthDate,
       ContactID,
       CreateDate,
       TerminateDate,
       Email1,
       Email2,
       Sex,
       PostalAddressCity,
       PostalAddressState,
       PostalAddressLine1,
       PostalAddressLine2,
       PostalAddressZipCode,
       IPAddress,
       LeadID,
       MemberID,
       FirstName,
       LastName,
       MiddleName,
       PrefixName,
       SuffixName,
       PartyID,
       Phone1,
       Phone2,
       MembershipID,
       SPACustomerID,
       UpdateDate,
       ActivationDate,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(convert(varchar,LoadDateTime,120),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(RowNumber as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(EntityID,'z#@$k%&P')+'P%#&z$@k'+isnull(SourceID,'z#@$k%&P')+'P%#&z$@k'+isnull(SourceCode,'z#@$k%&P'))),2) bk_hash,
       LoadDateTime,
       RowNumber,
       EntityID,
       SourceID,
       SourceCode,
       BirthDate,
       ContactID,
       CreateDate,
       TerminateDate,
       Email1,
       Email2,
       Sex,
       PostalAddressCity,
       PostalAddressState,
       PostalAddressLine1,
       PostalAddressLine2,
       PostalAddressZipCode,
       IPAddress,
       LeadID,
       MemberID,
       FirstName,
       LastName,
       MiddleName,
       PrefixName,
       SuffixName,
       PartyID,
       Phone1,
       Phone2,
       MembershipID,
       SPACustomerID,
       UpdateDate,
       ActivationDate,
       isnull(cast(stage_mdm_GoldenRecordCustomer.LoadDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_mdm_GoldenRecordCustomer
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_mdm_golden_record_customer @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_mdm_golden_record_customer (
       bk_hash,
       load_date_time,
       row_number,
       entity_id,
       source_id,
       source_code,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_mdm_GoldenRecordCustomer.bk_hash,
       stage_hash_mdm_GoldenRecordCustomer.LoadDateTime load_date_time,
       stage_hash_mdm_GoldenRecordCustomer.RowNumber row_number,
       stage_hash_mdm_GoldenRecordCustomer.EntityID entity_id,
       stage_hash_mdm_GoldenRecordCustomer.SourceID source_id,
       stage_hash_mdm_GoldenRecordCustomer.SourceCode source_code,
       isnull(cast(stage_hash_mdm_GoldenRecordCustomer.LoadDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       25,
       @insert_date_time,
       @user
  from stage_hash_mdm_GoldenRecordCustomer
  left join h_mdm_golden_record_customer
    on stage_hash_mdm_GoldenRecordCustomer.bk_hash = h_mdm_golden_record_customer.bk_hash
 where h_mdm_golden_record_customer_id is null
   and stage_hash_mdm_GoldenRecordCustomer.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_mdm_golden_record_customer
if object_id('tempdb..#l_mdm_golden_record_customer_inserts') is not null drop table #l_mdm_golden_record_customer_inserts
create table #l_mdm_golden_record_customer_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mdm_GoldenRecordCustomer.bk_hash,
       stage_hash_mdm_GoldenRecordCustomer.LoadDateTime load_date_time,
       stage_hash_mdm_GoldenRecordCustomer.RowNumber row_number,
       stage_hash_mdm_GoldenRecordCustomer.EntityID entity_id,
       stage_hash_mdm_GoldenRecordCustomer.SourceID source_id,
       stage_hash_mdm_GoldenRecordCustomer.SourceCode source_code,
       stage_hash_mdm_GoldenRecordCustomer.ContactID contact_id,
       stage_hash_mdm_GoldenRecordCustomer.LeadID lead_id,
       stage_hash_mdm_GoldenRecordCustomer.MemberID member_id,
       stage_hash_mdm_GoldenRecordCustomer.PartyID party_id,
       stage_hash_mdm_GoldenRecordCustomer.MembershipID membership_id,
       stage_hash_mdm_GoldenRecordCustomer.SPACustomerID spa_customer_id,
       stage_hash_mdm_GoldenRecordCustomer.LoadDateTime dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(convert(varchar,stage_hash_mdm_GoldenRecordCustomer.LoadDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mdm_GoldenRecordCustomer.RowNumber as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mdm_GoldenRecordCustomer.EntityID,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mdm_GoldenRecordCustomer.SourceID,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mdm_GoldenRecordCustomer.SourceCode,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mdm_GoldenRecordCustomer.ContactID,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mdm_GoldenRecordCustomer.LeadID,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mdm_GoldenRecordCustomer.MemberID,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mdm_GoldenRecordCustomer.PartyID,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mdm_GoldenRecordCustomer.MembershipID,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mdm_GoldenRecordCustomer.SPACustomerID,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mdm_GoldenRecordCustomer
 where stage_hash_mdm_GoldenRecordCustomer.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_mdm_golden_record_customer records
set @insert_date_time = getdate()
insert into l_mdm_golden_record_customer (
       bk_hash,
       load_date_time,
       row_number,
       entity_id,
       source_id,
       source_code,
       contact_id,
       lead_id,
       member_id,
       party_id,
       membership_id,
       spa_customer_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_mdm_golden_record_customer_inserts.bk_hash,
       #l_mdm_golden_record_customer_inserts.load_date_time,
       #l_mdm_golden_record_customer_inserts.row_number,
       #l_mdm_golden_record_customer_inserts.entity_id,
       #l_mdm_golden_record_customer_inserts.source_id,
       #l_mdm_golden_record_customer_inserts.source_code,
       #l_mdm_golden_record_customer_inserts.contact_id,
       #l_mdm_golden_record_customer_inserts.lead_id,
       #l_mdm_golden_record_customer_inserts.member_id,
       #l_mdm_golden_record_customer_inserts.party_id,
       #l_mdm_golden_record_customer_inserts.membership_id,
       #l_mdm_golden_record_customer_inserts.spa_customer_id,
       case when l_mdm_golden_record_customer.l_mdm_golden_record_customer_id is null then isnull(#l_mdm_golden_record_customer_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       25,
       #l_mdm_golden_record_customer_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_mdm_golden_record_customer_inserts
  left join p_mdm_golden_record_customer
    on #l_mdm_golden_record_customer_inserts.bk_hash = p_mdm_golden_record_customer.bk_hash
   and p_mdm_golden_record_customer.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_mdm_golden_record_customer
    on p_mdm_golden_record_customer.bk_hash = l_mdm_golden_record_customer.bk_hash
   and p_mdm_golden_record_customer.l_mdm_golden_record_customer_id = l_mdm_golden_record_customer.l_mdm_golden_record_customer_id
 where l_mdm_golden_record_customer.l_mdm_golden_record_customer_id is null
    or (l_mdm_golden_record_customer.l_mdm_golden_record_customer_id is not null
        and l_mdm_golden_record_customer.dv_hash <> #l_mdm_golden_record_customer_inserts.source_hash)

--calculate hash and lookup to current s_mdm_golden_record_customer
if object_id('tempdb..#s_mdm_golden_record_customer_inserts') is not null drop table #s_mdm_golden_record_customer_inserts
create table #s_mdm_golden_record_customer_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mdm_GoldenRecordCustomer.bk_hash,
       stage_hash_mdm_GoldenRecordCustomer.LoadDateTime load_date_time,
       stage_hash_mdm_GoldenRecordCustomer.RowNumber row_number,
       stage_hash_mdm_GoldenRecordCustomer.EntityID entity_id,
       stage_hash_mdm_GoldenRecordCustomer.SourceID source_id,
       stage_hash_mdm_GoldenRecordCustomer.SourceCode source_code,
       stage_hash_mdm_GoldenRecordCustomer.BirthDate birth_date,
       stage_hash_mdm_GoldenRecordCustomer.CreateDate create_date,
       stage_hash_mdm_GoldenRecordCustomer.TerminateDate terminate_date,
       stage_hash_mdm_GoldenRecordCustomer.Email1 email_1,
       stage_hash_mdm_GoldenRecordCustomer.Email2 email_2,
       stage_hash_mdm_GoldenRecordCustomer.Sex sex,
       stage_hash_mdm_GoldenRecordCustomer.PostalAddressCity postal_address_city,
       stage_hash_mdm_GoldenRecordCustomer.PostalAddressState postal_address_state,
       stage_hash_mdm_GoldenRecordCustomer.PostalAddressLine1 postal_address_line_1,
       stage_hash_mdm_GoldenRecordCustomer.PostalAddressLine2 postal_address_line_2,
       stage_hash_mdm_GoldenRecordCustomer.PostalAddressZipCode postal_address_zip_code,
       stage_hash_mdm_GoldenRecordCustomer.IPAddress ip_address,
       stage_hash_mdm_GoldenRecordCustomer.FirstName first_name,
       stage_hash_mdm_GoldenRecordCustomer.LastName last_name,
       stage_hash_mdm_GoldenRecordCustomer.MiddleName middle_name,
       stage_hash_mdm_GoldenRecordCustomer.PrefixName prefix_name,
       stage_hash_mdm_GoldenRecordCustomer.SuffixName suffix_name,
       stage_hash_mdm_GoldenRecordCustomer.Phone1 phone_1,
       stage_hash_mdm_GoldenRecordCustomer.Phone2 phone_2,
       stage_hash_mdm_GoldenRecordCustomer.UpdateDate update_date,
       stage_hash_mdm_GoldenRecordCustomer.ActivationDate activation_date,
       stage_hash_mdm_GoldenRecordCustomer.LoadDateTime dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(convert(varchar,stage_hash_mdm_GoldenRecordCustomer.LoadDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mdm_GoldenRecordCustomer.RowNumber as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mdm_GoldenRecordCustomer.EntityID,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mdm_GoldenRecordCustomer.SourceID,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mdm_GoldenRecordCustomer.SourceCode,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mdm_GoldenRecordCustomer.BirthDate,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mdm_GoldenRecordCustomer.CreateDate,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mdm_GoldenRecordCustomer.TerminateDate,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mdm_GoldenRecordCustomer.Email1,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mdm_GoldenRecordCustomer.Email2,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mdm_GoldenRecordCustomer.Sex,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mdm_GoldenRecordCustomer.PostalAddressCity,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mdm_GoldenRecordCustomer.PostalAddressState,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mdm_GoldenRecordCustomer.PostalAddressLine1,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mdm_GoldenRecordCustomer.PostalAddressLine2,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mdm_GoldenRecordCustomer.PostalAddressZipCode,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mdm_GoldenRecordCustomer.IPAddress,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mdm_GoldenRecordCustomer.FirstName,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mdm_GoldenRecordCustomer.LastName,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mdm_GoldenRecordCustomer.MiddleName,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mdm_GoldenRecordCustomer.PrefixName,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mdm_GoldenRecordCustomer.SuffixName,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mdm_GoldenRecordCustomer.Phone1,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mdm_GoldenRecordCustomer.Phone2,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mdm_GoldenRecordCustomer.UpdateDate,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mdm_GoldenRecordCustomer.ActivationDate,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mdm_GoldenRecordCustomer
 where stage_hash_mdm_GoldenRecordCustomer.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mdm_golden_record_customer records
set @insert_date_time = getdate()
insert into s_mdm_golden_record_customer (
       bk_hash,
       load_date_time,
       row_number,
       entity_id,
       source_id,
       source_code,
       birth_date,
       create_date,
       terminate_date,
       email_1,
       email_2,
       sex,
       postal_address_city,
       postal_address_state,
       postal_address_line_1,
       postal_address_line_2,
       postal_address_zip_code,
       ip_address,
       first_name,
       last_name,
       middle_name,
       prefix_name,
       suffix_name,
       phone_1,
       phone_2,
       update_date,
       activation_date,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mdm_golden_record_customer_inserts.bk_hash,
       #s_mdm_golden_record_customer_inserts.load_date_time,
       #s_mdm_golden_record_customer_inserts.row_number,
       #s_mdm_golden_record_customer_inserts.entity_id,
       #s_mdm_golden_record_customer_inserts.source_id,
       #s_mdm_golden_record_customer_inserts.source_code,
       #s_mdm_golden_record_customer_inserts.birth_date,
       #s_mdm_golden_record_customer_inserts.create_date,
       #s_mdm_golden_record_customer_inserts.terminate_date,
       #s_mdm_golden_record_customer_inserts.email_1,
       #s_mdm_golden_record_customer_inserts.email_2,
       #s_mdm_golden_record_customer_inserts.sex,
       #s_mdm_golden_record_customer_inserts.postal_address_city,
       #s_mdm_golden_record_customer_inserts.postal_address_state,
       #s_mdm_golden_record_customer_inserts.postal_address_line_1,
       #s_mdm_golden_record_customer_inserts.postal_address_line_2,
       #s_mdm_golden_record_customer_inserts.postal_address_zip_code,
       #s_mdm_golden_record_customer_inserts.ip_address,
       #s_mdm_golden_record_customer_inserts.first_name,
       #s_mdm_golden_record_customer_inserts.last_name,
       #s_mdm_golden_record_customer_inserts.middle_name,
       #s_mdm_golden_record_customer_inserts.prefix_name,
       #s_mdm_golden_record_customer_inserts.suffix_name,
       #s_mdm_golden_record_customer_inserts.phone_1,
       #s_mdm_golden_record_customer_inserts.phone_2,
       #s_mdm_golden_record_customer_inserts.update_date,
       #s_mdm_golden_record_customer_inserts.activation_date,
       case when s_mdm_golden_record_customer.s_mdm_golden_record_customer_id is null then isnull(#s_mdm_golden_record_customer_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       25,
       #s_mdm_golden_record_customer_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mdm_golden_record_customer_inserts
  left join p_mdm_golden_record_customer
    on #s_mdm_golden_record_customer_inserts.bk_hash = p_mdm_golden_record_customer.bk_hash
   and p_mdm_golden_record_customer.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mdm_golden_record_customer
    on p_mdm_golden_record_customer.bk_hash = s_mdm_golden_record_customer.bk_hash
   and p_mdm_golden_record_customer.s_mdm_golden_record_customer_id = s_mdm_golden_record_customer.s_mdm_golden_record_customer_id
 where s_mdm_golden_record_customer.s_mdm_golden_record_customer_id is null
    or (s_mdm_golden_record_customer.s_mdm_golden_record_customer_id is not null
        and s_mdm_golden_record_customer.dv_hash <> #s_mdm_golden_record_customer_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_mdm_golden_record_customer @current_dv_batch_id

end
