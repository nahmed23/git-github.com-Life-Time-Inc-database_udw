CREATE PROC [dbo].[proc_load_history_mms_MembershipBalanceSnapshot] AS
begin

set nocount on
set xact_abort on

/*Select the records from [dbo].[MembershipBalanceSnapshot] to be staged and inserted into the dv tables*/

if object_id('tempdb.dbo.#stage_mms_MembershipBalanceSnapshot_history') is not null drop table #stage_mms_MembershipBalanceSnapshot_history
create table dbo.#stage_mms_MembershipBalanceSnapshot_history with (location=user_db, distribution = hash(MembershipID)) as
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(MembershipID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(MembershipID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(MembershipBalanceID as varchar(500)),'z#@$k%&P'))),2)   l_mms_membership_balance_snapshot_hash,
        convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(MembershipID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(CurrentBalance as varchar(500)),'z#@$k%&P')
										+'P%#&z$@k'+isnull(cast(EFTAmount as varchar(500)),'z#@$k%&P')
										+'P%#&z$@k'+isnull(cast(StatementBalance as varchar(500)),'z#@$k%&P')
										+'P%#&z$@k'+isnull(cast(AssessedDateTime as varchar(500)),'z#@$k%&P')
										+'P%#&z$@k'+isnull(cast(StatementDateTime as varchar(500)),'z#@$k%&P')
										+'P%#&z$@k'+isnull(cast(PreviousStatementBalance as varchar(500)),'z#@$k%&P')
										+'P%#&z$@k'+isnull(cast(PreviousStatementDateTime as varchar(500)),'z#@$k%&P')
										+'P%#&z$@k'+isnull(cast(CommittedBalance as varchar(500)),'z#@$k%&P')
										+'P%#&z$@k'+isnull(cast(ResubmitCollectFromBankAccountFlag as varchar(500)),'z#@$k%&P')
										+'P%#&z$@k'+isnull(cast(CommittedBalanceProducts as varchar(500)),'z#@$k%&P')
										+'P%#&z$@k'+isnull(cast(CurrentBalanceProducts as varchar(500)),'z#@$k%&P')
										+'P%#&z$@k'+isnull(cast(EFTAmountProducts as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,MMSInsertedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,MMSUpdatedDateTime,120),'z#@$k%&P'))),2)  s_mms_membership_balance_snapshot_hash ,

        row_number() over(partition by MembershipID order by x.update_insert_date) rank2,
		*
  from (select row_number() over(partition by MembershipID,
                                              case when MMSUpdatedDateTime is null then MMSInsertedDateTime
                                                   else MMSUpdatedDateTime
                                               end
                                 order by MMSMembershipBalanceSnapshotKey desc) rank1,
              case when MMSUpdatedDateTime is null then MMSInsertedDateTime
                   else MMSUpdatedDateTime
               end update_insert_date,
               *
          from stage_mms_MembershipBalanceSnapshot_history) x
 /*-where rank1 = 1					*/
                              
/* Create the h records.*/
/* dv_load_date_time is the MMSInsertedDateTime or Jan 1, 1980 if MMSInsertedDateTime is null.*/
/* dv_batch_id is the dv_load_date_time converted to YYYYMMDDHHMISS*/
insert into dbo.h_mms_membership_balance_snapshot(
	   bk_hash,
       membership_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select 
       x.*
  from (select bk_hash,
              #stage_mms_MembershipBalanceSnapshot_history.MembershipID ,
               isnull(cast(#stage_mms_MembershipBalanceSnapshot_history.MMSInsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time, 
               case when #stage_mms_MembershipBalanceSnapshot_history.MMSInsertedDateTime is null then 19800101000000
                    else replace(replace(replace(convert(varchar, isnull(#stage_mms_MembershipBalanceSnapshot_history.MMSUpdatedDateTime,convert(datetime,'jan 1, 1980',107)),120 ), '-', ''),' ', ''), ':', '')
                end dv_batch_id,
               2 dv_r_load_source_id, 
               getdate() dv_inserted_date_time, 
               suser_sname() dv_insert_user 
          from dbo.#stage_mms_MembershipBalanceSnapshot_history 
         where rank2 = 1) x
         		
/* Create the l records.*/
/* Calculate dv_load_date_time*/
/* dv_batch_id is the dv_load_date_time converted to YYYYMMDDHHMISS*/
insert into dbo.l_mms_membership_balance_snapshot (
	   bk_hash,
       membership_balance_id,
       membership_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
	   	
select 
       x.*
  from (select #stage_mms_MembershipBalanceSnapshot_history.bk_hash,
       #stage_mms_MembershipBalanceSnapshot_history.MembershipBalanceID membership_balance_id,
       #stage_mms_MembershipBalanceSnapshot_history.MembershipID membership_id,
       case when #stage_mms_MembershipBalanceSnapshot_history.rank2 = 1 then
            case when #stage_mms_MembershipBalanceSnapshot_history.MMSInsertedDateTime is null then convert(datetime,'jan 1, 1980',107)
                else #stage_mms_MembershipBalanceSnapshot_history.MMSInsertedDateTime
                end
          else isnull(#stage_mms_MembershipBalanceSnapshot_history.MMSUpdatedDateTime,convert(datetime,'jan 1, 1980',107)) 
		end dv_load_date_time,	
		case when #stage_mms_MembershipBalanceSnapshot_history.rank2 = 1 then
			case when #stage_mms_MembershipBalanceSnapshot_history.MMSInsertedDateTime is null then 19800101000000
				else replace(replace(replace(convert(varchar, #stage_mms_MembershipBalanceSnapshot_history.MMSInsertedDateTime,120), '-', ''),' ', ''), ':', '')
				end
			else replace(replace(replace(convert(varchar, isnull(#stage_mms_MembershipBalanceSnapshot_history.MMSUpdatedDateTime,convert(datetime,'jan 1, 1980',107)),120), '-', ''),' ', ''), ':', '')	  
			end dv_batch_id,
		2 dv_r_load_source_id, 
        #stage_mms_MembershipBalanceSnapshot_history.l_mms_membership_balance_snapshot_hash,
        getdate() dv_inserted_date_time, 
        suser_sname() dv_insert_user 
        from dbo.#stage_mms_MembershipBalanceSnapshot_history
          left join dbo.#stage_mms_MembershipBalanceSnapshot_history prior
            on #stage_mms_MembershipBalanceSnapshot_history.MembershipID = prior.MembershipID
           and #stage_mms_MembershipBalanceSnapshot_history.rank2 = prior.rank2 + 1
         where #stage_mms_MembershipBalanceSnapshot_history.l_mms_membership_balance_snapshot_hash != isnull(prior.l_mms_membership_balance_snapshot_hash, ''))x


/* Create the s records.*/
/* dv_batch_id is the dv_load_date_time converted to YYYYMMDDHHMISS*/
insert into dbo.s_mms_membership_balance_snapshot (
       bk_hash,
       membership_id,
       current_balance,
	   eft_amount,
	   statement_balance,
       assessed_date_time,
       statement_date_time,
       previous_statement_balance,
       previous_statement_date_time,
       committed_balance,
	   resubmit_collect_from_bank_account_flag,
       committed_balance_products,
       current_balance_products,
       eft_amount_products,
       inserted_date_time,
       updated_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select 
       x.*
  from (select #stage_mms_MembershipBalanceSnapshot_history.bk_hash,
       #stage_mms_MembershipBalanceSnapshot_history.MembershipID membership_id,
 	   #stage_mms_MembershipBalanceSnapshot_history.CurrentBalance current_balance,
	   #stage_mms_MembershipBalanceSnapshot_history.EFTAmount eft_amount,
	   #stage_mms_MembershipBalanceSnapshot_history.StatementBalance statement_balance,
       #stage_mms_MembershipBalanceSnapshot_history.AssessedDateTime assessed_date_time,
       #stage_mms_MembershipBalanceSnapshot_history.StatementDateTime statement_date_time,
       #stage_mms_MembershipBalanceSnapshot_history.PreviousStatementBalance previous_statement_balance,
       #stage_mms_MembershipBalanceSnapshot_history.PreviousStatementDateTime previous_statement_date_time,
       #stage_mms_MembershipBalanceSnapshot_history.CommittedBalance committed_balance,
	   #stage_mms_MembershipBalanceSnapshot_history.ResubmitCollectFromBankAccountFlag resubmit_collect_from_bank_account_flag,
       #stage_mms_MembershipBalanceSnapshot_history.CommittedBalanceProducts committed_balance_products,
       #stage_mms_MembershipBalanceSnapshot_history.CurrentBalanceProducts current_balance_products,
       #stage_mms_MembershipBalanceSnapshot_history.EFTAmountProducts eft_amount_products,
	   #stage_mms_MembershipBalanceSnapshot_history.MMSInsertedDateTime inserted_date_time,
       #stage_mms_MembershipBalanceSnapshot_history.MMSUpdatedDateTime updated_date_time,
       case when #stage_mms_MembershipBalanceSnapshot_history.rank2 = 1 then
            case when #stage_mms_MembershipBalanceSnapshot_history.MMSInsertedDateTime is null then convert(datetime,'jan 1, 1980',107)
                  else #stage_mms_MembershipBalanceSnapshot_history.MMSInsertedDateTime
                  end
			else isnull(#stage_mms_MembershipBalanceSnapshot_history.MMSUpdatedDateTime,convert(datetime,'jan 1, 1980',107))  
			end dv_load_date_time,				
		case when #stage_mms_MembershipBalanceSnapshot_history.rank2 = 1 then
				case when #stage_mms_MembershipBalanceSnapshot_history.MMSInsertedDateTime is null then 19800101000000
						else replace(replace(replace(convert(varchar, #stage_mms_MembershipBalanceSnapshot_history.MMSInsertedDateTime,120), '-', ''),' ', ''), ':', '')
					end
			else replace(replace(replace(convert(varchar, isnull(#stage_mms_MembershipBalanceSnapshot_history.MMSUpdatedDateTime,convert(datetime,'jan 1, 1980',107)),120), '-', ''),' ', ''), ':', '')	  
				end dv_batch_id,	
		2 dv_r_load_source_id, 
        #stage_mms_MembershipBalanceSnapshot_history.s_mms_membership_balance_snapshot_hash,
		getdate() dv_inserted_date_time, 
        suser_sname() dv_insert_user
        from dbo.#stage_mms_MembershipBalanceSnapshot_history
          left join dbo.#stage_mms_MembershipBalanceSnapshot_history prior
            on #stage_mms_MembershipBalanceSnapshot_history.MembershipID = prior.MembershipID
           and #stage_mms_MembershipBalanceSnapshot_history.rank2 = prior.rank2 + 1
         where #stage_mms_MembershipBalanceSnapshot_history.s_mms_membership_balance_snapshot_hash != isnull(prior.s_mms_membership_balance_snapshot_hash, ''))x
end
