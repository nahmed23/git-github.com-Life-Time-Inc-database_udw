CREATE PROC [dbo].[proc_etl_crmcloudsync_invoice] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_crmcloudsync_Invoice

set @insert_date_time = getdate()
insert into dbo.stage_hash_crmcloudsync_Invoice (
       bk_hash,
       accountid,
       accountidname,
       accountidyominame,
       billto_city,
       billto_composite,
       billto_country,
       billto_fax,
       billto_line1,
       billto_line2,
       billto_line3,
       billto_name,
       billto_postalcode,
       billto_stateorprovince,
       billto_telephone,
       contactid,
       contactidname,
       contactidyominame,
       createdby,
       createdbyname,
       createdbyyominame,
       createdon,
       createdonbehalfby,
       createdonbehalfbyname,
       createdonbehalfbyyominame,
       customerid,
       customeridname,
       customeridtype,
       customeridyominame,
       datedelivered,
       description,
       discountamount,
       discountamount_base,
       discountpercentage,
       duedate,
       entityimage_timestamp,
       entityimage_url,
       entityimageid,
       exchangerate,
       freightamount,
       freightamount_base,
       importsequencenumber,
       invoiceid,
       invoicenumber,
       ispricelocked,
       ispricelockedname,
       lastbackofficesubmit,
       ltf_clubid,
       ltf_clubidname,
       ltf_membershipid,
       ltf_membershipsource,
       ltf_membershipsourcename,
       ltf_udwid,
       modifiedby,
       modifiedbyname,
       modifiedbyyominame,
       modifiedon,
       modifiedonbehalfby,
       modifiedonbehalfbyname,
       modifiedonbehalfbyyominame,
       name,
       opportunityid,
       opportunityidname,
       overriddencreatedon,
       ownerid,
       owneridname,
       owneridtype,
       owneridyominame,
       owningbusinessunit,
       owningteam,
       owninguser,
       paymenttermscode,
       paymenttermscodename,
       pricelevelid,
       pricelevelidname,
       pricingerrorcode,
       pricingerrorcodename,
       prioritycode,
       prioritycodename,
       processid,
       salesorderid,
       salesorderidname,
       shippingmethodcode,
       shippingmethodcodename,
       shipto_city,
       shipto_composite,
       shipto_country,
       shipto_fax,
       shipto_freighttermscode,
       shipto_freighttermscodename,
       shipto_line1,
       shipto_line2,
       shipto_line3,
       shipto_name,
       shipto_postalcode,
       shipto_stateorprovince,
       shipto_telephone,
       stageid,
       statecode,
       statecodename,
       statuscode,
       statuscodename,
       timezoneruleversionnumber,
       totalamount,
       totalamount_base,
       totalamountlessfreight,
       totalamountlessfreight_base,
       totaldiscountamount,
       totaldiscountamount_base,
       totallineitemamount,
       totallineitemamount_base,
       totallineitemdiscountamount,
       totallineitemdiscountamount_base,
       totaltax,
       totaltax_base,
       transactioncurrencyid,
       transactioncurrencyidname,
       utcconversiontimezonecode,
       versionnumber,
       willcall,
       willcallname,
       InsertedDateTime,
       InsertUser,
       UpdatedDateTime,
       UpdateUser,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(invoiceid,'z#@$k%&P'))),2) bk_hash,
       accountid,
       accountidname,
       accountidyominame,
       billto_city,
       billto_composite,
       billto_country,
       billto_fax,
       billto_line1,
       billto_line2,
       billto_line3,
       billto_name,
       billto_postalcode,
       billto_stateorprovince,
       billto_telephone,
       contactid,
       contactidname,
       contactidyominame,
       createdby,
       createdbyname,
       createdbyyominame,
       createdon,
       createdonbehalfby,
       createdonbehalfbyname,
       createdonbehalfbyyominame,
       customerid,
       customeridname,
       customeridtype,
       customeridyominame,
       datedelivered,
       description,
       discountamount,
       discountamount_base,
       discountpercentage,
       duedate,
       entityimage_timestamp,
       entityimage_url,
       entityimageid,
       exchangerate,
       freightamount,
       freightamount_base,
       importsequencenumber,
       invoiceid,
       invoicenumber,
       ispricelocked,
       ispricelockedname,
       lastbackofficesubmit,
       ltf_clubid,
       ltf_clubidname,
       ltf_membershipid,
       ltf_membershipsource,
       ltf_membershipsourcename,
       ltf_udwid,
       modifiedby,
       modifiedbyname,
       modifiedbyyominame,
       modifiedon,
       modifiedonbehalfby,
       modifiedonbehalfbyname,
       modifiedonbehalfbyyominame,
       name,
       opportunityid,
       opportunityidname,
       overriddencreatedon,
       ownerid,
       owneridname,
       owneridtype,
       owneridyominame,
       owningbusinessunit,
       owningteam,
       owninguser,
       paymenttermscode,
       paymenttermscodename,
       pricelevelid,
       pricelevelidname,
       pricingerrorcode,
       pricingerrorcodename,
       prioritycode,
       prioritycodename,
       processid,
       salesorderid,
       salesorderidname,
       shippingmethodcode,
       shippingmethodcodename,
       shipto_city,
       shipto_composite,
       shipto_country,
       shipto_fax,
       shipto_freighttermscode,
       shipto_freighttermscodename,
       shipto_line1,
       shipto_line2,
       shipto_line3,
       shipto_name,
       shipto_postalcode,
       shipto_stateorprovince,
       shipto_telephone,
       stageid,
       statecode,
       statecodename,
       statuscode,
       statuscodename,
       timezoneruleversionnumber,
       totalamount,
       totalamount_base,
       totalamountlessfreight,
       totalamountlessfreight_base,
       totaldiscountamount,
       totaldiscountamount_base,
       totallineitemamount,
       totallineitemamount_base,
       totallineitemdiscountamount,
       totallineitemdiscountamount_base,
       totaltax,
       totaltax_base,
       transactioncurrencyid,
       transactioncurrencyidname,
       utcconversiontimezonecode,
       versionnumber,
       willcall,
       willcallname,
       InsertedDateTime,
       InsertUser,
       UpdatedDateTime,
       UpdateUser,
       isnull(cast(stage_crmcloudsync_Invoice.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_crmcloudsync_Invoice
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_crmcloudsync_invoice @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_crmcloudsync_invoice (
       bk_hash,
       invoice_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_crmcloudsync_Invoice.bk_hash,
       stage_hash_crmcloudsync_Invoice.invoiceid invoice_id,
       isnull(cast(stage_hash_crmcloudsync_Invoice.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       3,
       @insert_date_time,
       @user
  from stage_hash_crmcloudsync_Invoice
  left join h_crmcloudsync_invoice
    on stage_hash_crmcloudsync_Invoice.bk_hash = h_crmcloudsync_invoice.bk_hash
 where h_crmcloudsync_invoice_id is null
   and stage_hash_crmcloudsync_Invoice.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_crmcloudsync_invoice
if object_id('tempdb..#l_crmcloudsync_invoice_inserts') is not null drop table #l_crmcloudsync_invoice_inserts
create table #l_crmcloudsync_invoice_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_crmcloudsync_Invoice.bk_hash,
       stage_hash_crmcloudsync_Invoice.accountid account_id,
       stage_hash_crmcloudsync_Invoice.contactid contact_id,
       stage_hash_crmcloudsync_Invoice.createdby created_by,
       stage_hash_crmcloudsync_Invoice.createdonbehalfby created_on_behalf_by,
       stage_hash_crmcloudsync_Invoice.customerid customer_id,
       stage_hash_crmcloudsync_Invoice.entityimageid entity_image_id,
       stage_hash_crmcloudsync_Invoice.invoiceid invoice_id,
       stage_hash_crmcloudsync_Invoice.ltf_clubid ltf_club_id,
       stage_hash_crmcloudsync_Invoice.ltf_membershipid ltf_membership_id,
       stage_hash_crmcloudsync_Invoice.ltf_udwid ltf_udw_id,
       stage_hash_crmcloudsync_Invoice.modifiedby modified_by,
       stage_hash_crmcloudsync_Invoice.modifiedonbehalfby modified_on_behalf_by,
       stage_hash_crmcloudsync_Invoice.opportunityid opportunity_id,
       stage_hash_crmcloudsync_Invoice.ownerid owner_id,
       stage_hash_crmcloudsync_Invoice.owningbusinessunit owning_business_unit,
       stage_hash_crmcloudsync_Invoice.owningteam owning_team,
       stage_hash_crmcloudsync_Invoice.owninguser owning_user,
       stage_hash_crmcloudsync_Invoice.pricelevelid price_level_id,
       stage_hash_crmcloudsync_Invoice.processid process_id,
       stage_hash_crmcloudsync_Invoice.salesorderid sales_order_id,
       stage_hash_crmcloudsync_Invoice.stageid stage_id,
       stage_hash_crmcloudsync_Invoice.transactioncurrencyid transaction_currency_id,
       isnull(cast(stage_hash_crmcloudsync_Invoice.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.accountid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.contactid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.createdby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.createdonbehalfby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.customerid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.entityimageid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.invoiceid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.ltf_clubid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.ltf_membershipid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.ltf_udwid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.modifiedby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.modifiedonbehalfby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.opportunityid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.ownerid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.owningbusinessunit,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.owningteam,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.owninguser,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.pricelevelid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.processid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.salesorderid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.stageid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.transactioncurrencyid,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_crmcloudsync_Invoice
 where stage_hash_crmcloudsync_Invoice.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_crmcloudsync_invoice records
set @insert_date_time = getdate()
insert into l_crmcloudsync_invoice (
       bk_hash,
       account_id,
       contact_id,
       created_by,
       created_on_behalf_by,
       customer_id,
       entity_image_id,
       invoice_id,
       ltf_club_id,
       ltf_membership_id,
       ltf_udw_id,
       modified_by,
       modified_on_behalf_by,
       opportunity_id,
       owner_id,
       owning_business_unit,
       owning_team,
       owning_user,
       price_level_id,
       process_id,
       sales_order_id,
       stage_id,
       transaction_currency_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_crmcloudsync_invoice_inserts.bk_hash,
       #l_crmcloudsync_invoice_inserts.account_id,
       #l_crmcloudsync_invoice_inserts.contact_id,
       #l_crmcloudsync_invoice_inserts.created_by,
       #l_crmcloudsync_invoice_inserts.created_on_behalf_by,
       #l_crmcloudsync_invoice_inserts.customer_id,
       #l_crmcloudsync_invoice_inserts.entity_image_id,
       #l_crmcloudsync_invoice_inserts.invoice_id,
       #l_crmcloudsync_invoice_inserts.ltf_club_id,
       #l_crmcloudsync_invoice_inserts.ltf_membership_id,
       #l_crmcloudsync_invoice_inserts.ltf_udw_id,
       #l_crmcloudsync_invoice_inserts.modified_by,
       #l_crmcloudsync_invoice_inserts.modified_on_behalf_by,
       #l_crmcloudsync_invoice_inserts.opportunity_id,
       #l_crmcloudsync_invoice_inserts.owner_id,
       #l_crmcloudsync_invoice_inserts.owning_business_unit,
       #l_crmcloudsync_invoice_inserts.owning_team,
       #l_crmcloudsync_invoice_inserts.owning_user,
       #l_crmcloudsync_invoice_inserts.price_level_id,
       #l_crmcloudsync_invoice_inserts.process_id,
       #l_crmcloudsync_invoice_inserts.sales_order_id,
       #l_crmcloudsync_invoice_inserts.stage_id,
       #l_crmcloudsync_invoice_inserts.transaction_currency_id,
       case when l_crmcloudsync_invoice.l_crmcloudsync_invoice_id is null then isnull(#l_crmcloudsync_invoice_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       3,
       #l_crmcloudsync_invoice_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_crmcloudsync_invoice_inserts
  left join p_crmcloudsync_invoice
    on #l_crmcloudsync_invoice_inserts.bk_hash = p_crmcloudsync_invoice.bk_hash
   and p_crmcloudsync_invoice.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_crmcloudsync_invoice
    on p_crmcloudsync_invoice.bk_hash = l_crmcloudsync_invoice.bk_hash
   and p_crmcloudsync_invoice.l_crmcloudsync_invoice_id = l_crmcloudsync_invoice.l_crmcloudsync_invoice_id
 where l_crmcloudsync_invoice.l_crmcloudsync_invoice_id is null
    or (l_crmcloudsync_invoice.l_crmcloudsync_invoice_id is not null
        and l_crmcloudsync_invoice.dv_hash <> #l_crmcloudsync_invoice_inserts.source_hash)

--calculate hash and lookup to current s_crmcloudsync_invoice
if object_id('tempdb..#s_crmcloudsync_invoice_inserts') is not null drop table #s_crmcloudsync_invoice_inserts
create table #s_crmcloudsync_invoice_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_crmcloudsync_Invoice.bk_hash,
       stage_hash_crmcloudsync_Invoice.accountidname account_id_name,
       stage_hash_crmcloudsync_Invoice.accountidyominame account_id_yomi_name,
       stage_hash_crmcloudsync_Invoice.billto_city bill_to_city,
       stage_hash_crmcloudsync_Invoice.billto_composite bill_to_composite,
       stage_hash_crmcloudsync_Invoice.billto_country bill_to_country,
       stage_hash_crmcloudsync_Invoice.billto_fax bill_to_fax,
       stage_hash_crmcloudsync_Invoice.billto_line1 bill_to_line_1,
       stage_hash_crmcloudsync_Invoice.billto_line2 bill_to_line_2,
       stage_hash_crmcloudsync_Invoice.billto_line3 bill_to_line_3,
       stage_hash_crmcloudsync_Invoice.billto_name bill_to_name,
       stage_hash_crmcloudsync_Invoice.billto_postalcode bill_to_postal_code,
       stage_hash_crmcloudsync_Invoice.billto_stateorprovince bill_to_state_or_province,
       stage_hash_crmcloudsync_Invoice.billto_telephone bill_to_telephone,
       stage_hash_crmcloudsync_Invoice.contactidname contact_id_name,
       stage_hash_crmcloudsync_Invoice.contactidyominame contact_id_yomi_name,
       stage_hash_crmcloudsync_Invoice.createdbyname created_by_name,
       stage_hash_crmcloudsync_Invoice.createdbyyominame created_by_yomi_name,
       stage_hash_crmcloudsync_Invoice.createdon created_on,
       stage_hash_crmcloudsync_Invoice.createdonbehalfbyname created_on_behalf_by_name,
       stage_hash_crmcloudsync_Invoice.createdonbehalfbyyominame created_on_behalf_by_yomi_name,
       stage_hash_crmcloudsync_Invoice.customeridname customer_id_name,
       stage_hash_crmcloudsync_Invoice.customeridtype customer_id_type,
       stage_hash_crmcloudsync_Invoice.customeridyominame customer_id_yomi_name,
       stage_hash_crmcloudsync_Invoice.datedelivered date_delivered,
       stage_hash_crmcloudsync_Invoice.description description,
       stage_hash_crmcloudsync_Invoice.discountamount discount_amount,
       stage_hash_crmcloudsync_Invoice.discountamount_base discount_amount_base,
       stage_hash_crmcloudsync_Invoice.discountpercentage discount_percentage,
       stage_hash_crmcloudsync_Invoice.duedate due_date,
       stage_hash_crmcloudsync_Invoice.entityimage_timestamp entity_image_time_stamp,
       stage_hash_crmcloudsync_Invoice.entityimage_url entity_image_url,
       stage_hash_crmcloudsync_Invoice.exchangerate exchange_rate,
       stage_hash_crmcloudsync_Invoice.freightamount freight_amount,
       stage_hash_crmcloudsync_Invoice.freightamount_base freight_amount_base,
       stage_hash_crmcloudsync_Invoice.importsequencenumber import_sequence_number,
       stage_hash_crmcloudsync_Invoice.invoiceid invoice_id,
       stage_hash_crmcloudsync_Invoice.invoicenumber invoice_number,
       stage_hash_crmcloudsync_Invoice.ispricelocked is_price_locked,
       stage_hash_crmcloudsync_Invoice.ispricelockedname is_price_locked_name,
       stage_hash_crmcloudsync_Invoice.lastbackofficesubmit last_back_office_submit,
       stage_hash_crmcloudsync_Invoice.ltf_clubidname ltf_club_id_name,
       stage_hash_crmcloudsync_Invoice.ltf_membershipsource ltf_membership_source,
       stage_hash_crmcloudsync_Invoice.ltf_membershipsourcename ltf_membership_source_name,
       stage_hash_crmcloudsync_Invoice.modifiedbyname modified_by_name,
       stage_hash_crmcloudsync_Invoice.modifiedbyyominame modified_by_yomi_name,
       stage_hash_crmcloudsync_Invoice.modifiedon modified_on,
       stage_hash_crmcloudsync_Invoice.modifiedonbehalfbyname modified_on_behalf_by_name,
       stage_hash_crmcloudsync_Invoice.modifiedonbehalfbyyominame modified_on_behalf_by_yomi_name,
       stage_hash_crmcloudsync_Invoice.name name,
       stage_hash_crmcloudsync_Invoice.opportunityidname opportunity_id_name,
       stage_hash_crmcloudsync_Invoice.overriddencreatedon overridden_created_on,
       stage_hash_crmcloudsync_Invoice.owneridname owner_id_name,
       stage_hash_crmcloudsync_Invoice.owneridtype owner_id_type,
       stage_hash_crmcloudsync_Invoice.owneridyominame owner_id_yomi_name,
       stage_hash_crmcloudsync_Invoice.paymenttermscode payment_terms_code,
       stage_hash_crmcloudsync_Invoice.paymenttermscodename payment_terms_code_name,
       stage_hash_crmcloudsync_Invoice.pricelevelidname price_level_id_name,
       stage_hash_crmcloudsync_Invoice.pricingerrorcode pricing_error_code,
       stage_hash_crmcloudsync_Invoice.pricingerrorcodename pricing_error_code_name,
       stage_hash_crmcloudsync_Invoice.prioritycode priority_code,
       stage_hash_crmcloudsync_Invoice.prioritycodename priority_code_name,
       stage_hash_crmcloudsync_Invoice.salesorderidname sales_order_id_name,
       stage_hash_crmcloudsync_Invoice.shippingmethodcode shipping_method_code,
       stage_hash_crmcloudsync_Invoice.shippingmethodcodename shipping_method_code_name,
       stage_hash_crmcloudsync_Invoice.shipto_city ship_to_city,
       stage_hash_crmcloudsync_Invoice.shipto_composite ship_to_composite,
       stage_hash_crmcloudsync_Invoice.shipto_country ship_to_country,
       stage_hash_crmcloudsync_Invoice.shipto_fax ship_to_fax,
       stage_hash_crmcloudsync_Invoice.shipto_freighttermscode ship_to_freight_terms_code,
       stage_hash_crmcloudsync_Invoice.shipto_freighttermscodename ship_to_freight_terms_code_name,
       stage_hash_crmcloudsync_Invoice.shipto_line1 ship_to_line_1,
       stage_hash_crmcloudsync_Invoice.shipto_line2 ship_to_line_2,
       stage_hash_crmcloudsync_Invoice.shipto_line3 ship_to_line_3,
       stage_hash_crmcloudsync_Invoice.shipto_name ship_to_name,
       stage_hash_crmcloudsync_Invoice.shipto_postalcode ship_to_postal_code,
       stage_hash_crmcloudsync_Invoice.shipto_stateorprovince ship_to_state_or_province,
       stage_hash_crmcloudsync_Invoice.shipto_telephone ship_to_telephone,
       stage_hash_crmcloudsync_Invoice.statecode state_code,
       stage_hash_crmcloudsync_Invoice.statecodename state_code_name,
       stage_hash_crmcloudsync_Invoice.statuscode status_code,
       stage_hash_crmcloudsync_Invoice.statuscodename status_code_name,
       stage_hash_crmcloudsync_Invoice.timezoneruleversionnumber time_zone_rule_version_number,
       stage_hash_crmcloudsync_Invoice.totalamount total_amount,
       stage_hash_crmcloudsync_Invoice.totalamount_base total_amount_base,
       stage_hash_crmcloudsync_Invoice.totalamountlessfreight total_amount_less_freight,
       stage_hash_crmcloudsync_Invoice.totalamountlessfreight_base total_amount_less_freight_base,
       stage_hash_crmcloudsync_Invoice.totaldiscountamount total_discount_amount,
       stage_hash_crmcloudsync_Invoice.totaldiscountamount_base total_discount_amount_base,
       stage_hash_crmcloudsync_Invoice.totallineitemamount total_line_item_amount,
       stage_hash_crmcloudsync_Invoice.totallineitemamount_base total_line_item_amount_base,
       stage_hash_crmcloudsync_Invoice.totallineitemdiscountamount total_line_item_discount_amount,
       stage_hash_crmcloudsync_Invoice.totallineitemdiscountamount_base total_line_item_discount_amount_base,
       stage_hash_crmcloudsync_Invoice.totaltax total_tax,
       stage_hash_crmcloudsync_Invoice.totaltax_base total_tax_base,
       stage_hash_crmcloudsync_Invoice.transactioncurrencyidname transaction_currency_id_name,
       stage_hash_crmcloudsync_Invoice.utcconversiontimezonecode utc_conversion_time_zone_code,
       stage_hash_crmcloudsync_Invoice.versionnumber version_number,
       stage_hash_crmcloudsync_Invoice.willcall will_call,
       stage_hash_crmcloudsync_Invoice.willcallname will_call_name,
       stage_hash_crmcloudsync_Invoice.InsertedDateTime inserted_date_time,
       stage_hash_crmcloudsync_Invoice.InsertUser insert_user,
       stage_hash_crmcloudsync_Invoice.UpdatedDateTime updated_date_time,
       stage_hash_crmcloudsync_Invoice.UpdateUser update_user,
       isnull(cast(stage_hash_crmcloudsync_Invoice.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.accountidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.accountidyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.billto_city,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.billto_composite,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.billto_country,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.billto_fax,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.billto_line1,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.billto_line2,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.billto_line3,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.billto_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.billto_postalcode,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.billto_stateorprovince,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.billto_telephone,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.contactidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.contactidyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.createdbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.createdbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_Invoice.createdon,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.createdonbehalfbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.createdonbehalfbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.customeridname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.customeridtype,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.customeridyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_Invoice.datedelivered,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.description,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Invoice.discountamount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Invoice.discountamount_base as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Invoice.discountpercentage as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_Invoice.duedate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Invoice.entityimage_timestamp as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.entityimage_url,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Invoice.exchangerate as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Invoice.freightamount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Invoice.freightamount_base as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Invoice.importsequencenumber as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.invoiceid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.invoicenumber,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Invoice.ispricelocked as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.ispricelockedname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_Invoice.lastbackofficesubmit,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.ltf_clubidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Invoice.ltf_membershipsource as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.ltf_membershipsourcename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.modifiedbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.modifiedbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_Invoice.modifiedon,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.modifiedonbehalfbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.modifiedonbehalfbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.opportunityidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_Invoice.overriddencreatedon,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.owneridname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.owneridtype,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.owneridyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Invoice.paymenttermscode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.paymenttermscodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.pricelevelidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Invoice.pricingerrorcode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.pricingerrorcodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Invoice.prioritycode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.prioritycodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.salesorderidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Invoice.shippingmethodcode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.shippingmethodcodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.shipto_city,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.shipto_composite,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.shipto_country,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.shipto_fax,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Invoice.shipto_freighttermscode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.shipto_freighttermscodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.shipto_line1,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.shipto_line2,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.shipto_line3,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.shipto_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.shipto_postalcode,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.shipto_stateorprovince,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.shipto_telephone,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Invoice.statecode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.statecodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Invoice.statuscode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.statuscodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Invoice.timezoneruleversionnumber as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Invoice.totalamount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Invoice.totalamount_base as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Invoice.totalamountlessfreight as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Invoice.totalamountlessfreight_base as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Invoice.totaldiscountamount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Invoice.totaldiscountamount_base as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Invoice.totallineitemamount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Invoice.totallineitemamount_base as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Invoice.totallineitemdiscountamount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Invoice.totallineitemdiscountamount_base as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Invoice.totaltax as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Invoice.totaltax_base as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.transactioncurrencyidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Invoice.utcconversiontimezonecode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Invoice.versionnumber as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Invoice.willcall as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.willcallname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_Invoice.InsertedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.InsertUser,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_Invoice.UpdatedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Invoice.UpdateUser,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_crmcloudsync_Invoice
 where stage_hash_crmcloudsync_Invoice.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_crmcloudsync_invoice records
set @insert_date_time = getdate()
insert into s_crmcloudsync_invoice (
       bk_hash,
       account_id_name,
       account_id_yomi_name,
       bill_to_city,
       bill_to_composite,
       bill_to_country,
       bill_to_fax,
       bill_to_line_1,
       bill_to_line_2,
       bill_to_line_3,
       bill_to_name,
       bill_to_postal_code,
       bill_to_state_or_province,
       bill_to_telephone,
       contact_id_name,
       contact_id_yomi_name,
       created_by_name,
       created_by_yomi_name,
       created_on,
       created_on_behalf_by_name,
       created_on_behalf_by_yomi_name,
       customer_id_name,
       customer_id_type,
       customer_id_yomi_name,
       date_delivered,
       description,
       discount_amount,
       discount_amount_base,
       discount_percentage,
       due_date,
       entity_image_time_stamp,
       entity_image_url,
       exchange_rate,
       freight_amount,
       freight_amount_base,
       import_sequence_number,
       invoice_id,
       invoice_number,
       is_price_locked,
       is_price_locked_name,
       last_back_office_submit,
       ltf_club_id_name,
       ltf_membership_source,
       ltf_membership_source_name,
       modified_by_name,
       modified_by_yomi_name,
       modified_on,
       modified_on_behalf_by_name,
       modified_on_behalf_by_yomi_name,
       name,
       opportunity_id_name,
       overridden_created_on,
       owner_id_name,
       owner_id_type,
       owner_id_yomi_name,
       payment_terms_code,
       payment_terms_code_name,
       price_level_id_name,
       pricing_error_code,
       pricing_error_code_name,
       priority_code,
       priority_code_name,
       sales_order_id_name,
       shipping_method_code,
       shipping_method_code_name,
       ship_to_city,
       ship_to_composite,
       ship_to_country,
       ship_to_fax,
       ship_to_freight_terms_code,
       ship_to_freight_terms_code_name,
       ship_to_line_1,
       ship_to_line_2,
       ship_to_line_3,
       ship_to_name,
       ship_to_postal_code,
       ship_to_state_or_province,
       ship_to_telephone,
       state_code,
       state_code_name,
       status_code,
       status_code_name,
       time_zone_rule_version_number,
       total_amount,
       total_amount_base,
       total_amount_less_freight,
       total_amount_less_freight_base,
       total_discount_amount,
       total_discount_amount_base,
       total_line_item_amount,
       total_line_item_amount_base,
       total_line_item_discount_amount,
       total_line_item_discount_amount_base,
       total_tax,
       total_tax_base,
       transaction_currency_id_name,
       utc_conversion_time_zone_code,
       version_number,
       will_call,
       will_call_name,
       inserted_date_time,
       insert_user,
       updated_date_time,
       update_user,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_crmcloudsync_invoice_inserts.bk_hash,
       #s_crmcloudsync_invoice_inserts.account_id_name,
       #s_crmcloudsync_invoice_inserts.account_id_yomi_name,
       #s_crmcloudsync_invoice_inserts.bill_to_city,
       #s_crmcloudsync_invoice_inserts.bill_to_composite,
       #s_crmcloudsync_invoice_inserts.bill_to_country,
       #s_crmcloudsync_invoice_inserts.bill_to_fax,
       #s_crmcloudsync_invoice_inserts.bill_to_line_1,
       #s_crmcloudsync_invoice_inserts.bill_to_line_2,
       #s_crmcloudsync_invoice_inserts.bill_to_line_3,
       #s_crmcloudsync_invoice_inserts.bill_to_name,
       #s_crmcloudsync_invoice_inserts.bill_to_postal_code,
       #s_crmcloudsync_invoice_inserts.bill_to_state_or_province,
       #s_crmcloudsync_invoice_inserts.bill_to_telephone,
       #s_crmcloudsync_invoice_inserts.contact_id_name,
       #s_crmcloudsync_invoice_inserts.contact_id_yomi_name,
       #s_crmcloudsync_invoice_inserts.created_by_name,
       #s_crmcloudsync_invoice_inserts.created_by_yomi_name,
       #s_crmcloudsync_invoice_inserts.created_on,
       #s_crmcloudsync_invoice_inserts.created_on_behalf_by_name,
       #s_crmcloudsync_invoice_inserts.created_on_behalf_by_yomi_name,
       #s_crmcloudsync_invoice_inserts.customer_id_name,
       #s_crmcloudsync_invoice_inserts.customer_id_type,
       #s_crmcloudsync_invoice_inserts.customer_id_yomi_name,
       #s_crmcloudsync_invoice_inserts.date_delivered,
       #s_crmcloudsync_invoice_inserts.description,
       #s_crmcloudsync_invoice_inserts.discount_amount,
       #s_crmcloudsync_invoice_inserts.discount_amount_base,
       #s_crmcloudsync_invoice_inserts.discount_percentage,
       #s_crmcloudsync_invoice_inserts.due_date,
       #s_crmcloudsync_invoice_inserts.entity_image_time_stamp,
       #s_crmcloudsync_invoice_inserts.entity_image_url,
       #s_crmcloudsync_invoice_inserts.exchange_rate,
       #s_crmcloudsync_invoice_inserts.freight_amount,
       #s_crmcloudsync_invoice_inserts.freight_amount_base,
       #s_crmcloudsync_invoice_inserts.import_sequence_number,
       #s_crmcloudsync_invoice_inserts.invoice_id,
       #s_crmcloudsync_invoice_inserts.invoice_number,
       #s_crmcloudsync_invoice_inserts.is_price_locked,
       #s_crmcloudsync_invoice_inserts.is_price_locked_name,
       #s_crmcloudsync_invoice_inserts.last_back_office_submit,
       #s_crmcloudsync_invoice_inserts.ltf_club_id_name,
       #s_crmcloudsync_invoice_inserts.ltf_membership_source,
       #s_crmcloudsync_invoice_inserts.ltf_membership_source_name,
       #s_crmcloudsync_invoice_inserts.modified_by_name,
       #s_crmcloudsync_invoice_inserts.modified_by_yomi_name,
       #s_crmcloudsync_invoice_inserts.modified_on,
       #s_crmcloudsync_invoice_inserts.modified_on_behalf_by_name,
       #s_crmcloudsync_invoice_inserts.modified_on_behalf_by_yomi_name,
       #s_crmcloudsync_invoice_inserts.name,
       #s_crmcloudsync_invoice_inserts.opportunity_id_name,
       #s_crmcloudsync_invoice_inserts.overridden_created_on,
       #s_crmcloudsync_invoice_inserts.owner_id_name,
       #s_crmcloudsync_invoice_inserts.owner_id_type,
       #s_crmcloudsync_invoice_inserts.owner_id_yomi_name,
       #s_crmcloudsync_invoice_inserts.payment_terms_code,
       #s_crmcloudsync_invoice_inserts.payment_terms_code_name,
       #s_crmcloudsync_invoice_inserts.price_level_id_name,
       #s_crmcloudsync_invoice_inserts.pricing_error_code,
       #s_crmcloudsync_invoice_inserts.pricing_error_code_name,
       #s_crmcloudsync_invoice_inserts.priority_code,
       #s_crmcloudsync_invoice_inserts.priority_code_name,
       #s_crmcloudsync_invoice_inserts.sales_order_id_name,
       #s_crmcloudsync_invoice_inserts.shipping_method_code,
       #s_crmcloudsync_invoice_inserts.shipping_method_code_name,
       #s_crmcloudsync_invoice_inserts.ship_to_city,
       #s_crmcloudsync_invoice_inserts.ship_to_composite,
       #s_crmcloudsync_invoice_inserts.ship_to_country,
       #s_crmcloudsync_invoice_inserts.ship_to_fax,
       #s_crmcloudsync_invoice_inserts.ship_to_freight_terms_code,
       #s_crmcloudsync_invoice_inserts.ship_to_freight_terms_code_name,
       #s_crmcloudsync_invoice_inserts.ship_to_line_1,
       #s_crmcloudsync_invoice_inserts.ship_to_line_2,
       #s_crmcloudsync_invoice_inserts.ship_to_line_3,
       #s_crmcloudsync_invoice_inserts.ship_to_name,
       #s_crmcloudsync_invoice_inserts.ship_to_postal_code,
       #s_crmcloudsync_invoice_inserts.ship_to_state_or_province,
       #s_crmcloudsync_invoice_inserts.ship_to_telephone,
       #s_crmcloudsync_invoice_inserts.state_code,
       #s_crmcloudsync_invoice_inserts.state_code_name,
       #s_crmcloudsync_invoice_inserts.status_code,
       #s_crmcloudsync_invoice_inserts.status_code_name,
       #s_crmcloudsync_invoice_inserts.time_zone_rule_version_number,
       #s_crmcloudsync_invoice_inserts.total_amount,
       #s_crmcloudsync_invoice_inserts.total_amount_base,
       #s_crmcloudsync_invoice_inserts.total_amount_less_freight,
       #s_crmcloudsync_invoice_inserts.total_amount_less_freight_base,
       #s_crmcloudsync_invoice_inserts.total_discount_amount,
       #s_crmcloudsync_invoice_inserts.total_discount_amount_base,
       #s_crmcloudsync_invoice_inserts.total_line_item_amount,
       #s_crmcloudsync_invoice_inserts.total_line_item_amount_base,
       #s_crmcloudsync_invoice_inserts.total_line_item_discount_amount,
       #s_crmcloudsync_invoice_inserts.total_line_item_discount_amount_base,
       #s_crmcloudsync_invoice_inserts.total_tax,
       #s_crmcloudsync_invoice_inserts.total_tax_base,
       #s_crmcloudsync_invoice_inserts.transaction_currency_id_name,
       #s_crmcloudsync_invoice_inserts.utc_conversion_time_zone_code,
       #s_crmcloudsync_invoice_inserts.version_number,
       #s_crmcloudsync_invoice_inserts.will_call,
       #s_crmcloudsync_invoice_inserts.will_call_name,
       #s_crmcloudsync_invoice_inserts.inserted_date_time,
       #s_crmcloudsync_invoice_inserts.insert_user,
       #s_crmcloudsync_invoice_inserts.updated_date_time,
       #s_crmcloudsync_invoice_inserts.update_user,
       case when s_crmcloudsync_invoice.s_crmcloudsync_invoice_id is null then isnull(#s_crmcloudsync_invoice_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       3,
       #s_crmcloudsync_invoice_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_crmcloudsync_invoice_inserts
  left join p_crmcloudsync_invoice
    on #s_crmcloudsync_invoice_inserts.bk_hash = p_crmcloudsync_invoice.bk_hash
   and p_crmcloudsync_invoice.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_crmcloudsync_invoice
    on p_crmcloudsync_invoice.bk_hash = s_crmcloudsync_invoice.bk_hash
   and p_crmcloudsync_invoice.s_crmcloudsync_invoice_id = s_crmcloudsync_invoice.s_crmcloudsync_invoice_id
 where s_crmcloudsync_invoice.s_crmcloudsync_invoice_id is null
    or (s_crmcloudsync_invoice.s_crmcloudsync_invoice_id is not null
        and s_crmcloudsync_invoice.dv_hash <> #s_crmcloudsync_invoice_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_crmcloudsync_invoice @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_crmcloudsync_invoice @current_dv_batch_id

end
