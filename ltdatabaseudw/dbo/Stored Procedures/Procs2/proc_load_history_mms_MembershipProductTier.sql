CREATE PROC [dbo].[proc_load_history_mms_MembershipProductTier] AS
begin

set nocount on
set xact_abort on

/*Select the records from [dbo].[MMSProductTierPrice] to be staged and inserted into the dv tables*/

if object_id('tempdb.dbo.#stage_mms_MembershipProductTier_history') is not null drop table #stage_mms_MembershipProductTier_history
create table dbo.#stage_mms_MembershipProductTier_history with (location=user_db, distribution = hash(MembershipProductTierID)) as
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(MembershipProductTierID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(MembershipProductTierID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(MembershipID as varchar(500)),'z#@$k%&P')
										+'P%#&z$@k'+isnull(cast(LastUpdatedEmployeeID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(ProductTierID as varchar(500)),'z#@$k%&P'))),2)   l_mms_membership_product_tier_hash,
        convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(MembershipProductTierID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,MMSInsertedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,MMSUpdatedDateTime,120),'z#@$k%&P'))),2)  s_mms_membership_product_tier_hash ,

        row_number() over(partition by MembershipProductTierID order by x.update_insert_date) rank2,
		*
  from (select row_number() over(partition by MembershipProductTierID,
              case when MMSUpdatedDateTime is null then MMSInsertedDateTime
                   else MMSUpdatedDateTime
                   end
                   order by [MembershipProductTierKey] desc) rank1,
              case when MMSUpdatedDateTime is null then MMSInsertedDateTime
                   else MMSUpdatedDateTime
               end update_insert_date,
               *
          from stage_mms_MembershipProductTier_history) x
 /*where rank1 = 1					*/
                              
/* Create the h records.*/
/* dv_load_date_time is the MMSInsertedDateTime or Jan 1, 1980 if MMSInsertedDateTime is null.*/
/* dv_batch_id is the dv_load_date_time converted to YYYYMMDDHHMISS*/
insert into dbo.h_mms_membership_product_tier(
	   bk_hash,
       membership_product_tier_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select 
       x.*
  from (select bk_hash,
              #stage_mms_MembershipProductTier_history.MembershipProductTierID ,
               isnull(cast(#stage_mms_MembershipProductTier_history.MMSInsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time, 
               case when MMSInsertedDateTime is null then 19800101000000
                    else replace(replace(replace(convert(varchar, MMSInsertedDateTime,120 ), '-', ''),' ', ''), ':', '')
                end dv_batch_id,
               2 dv_r_load_source_id, 
               getdate() dv_inserted_date_time, 
               suser_sname() dv_insert_user 
          from dbo.#stage_mms_MembershipProductTier_history 
         where rank2 = 1) x
         		
/* Create the l records.*/
/* Calculate dv_load_date_time*/
/* dv_batch_id is the dv_load_date_time converted to YYYYMMDDHHMISS*/
insert into dbo.l_mms_membership_product_tier (
	   bk_hash,
       membership_product_tier_id,
       membership_id,
       product_tier_id,
       last_updated_employee_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
	   	
select 
       x.*
  from (select #stage_mms_MembershipProductTier_history.bk_hash,
       #stage_mms_MembershipProductTier_history.MembershipProductTierID membership_product_tier_id,
       #stage_mms_MembershipProductTier_history.MembershipID membership_id,
       #stage_mms_MembershipProductTier_history.ProductTierID product_tier_id,
	   #stage_mms_MembershipProductTier_history.LastUpdatedEmployeeID last_updated_employee_id,
               case when #stage_mms_MembershipProductTier_history.rank2 = 1 then
                         case when #stage_mms_MembershipProductTier_history.MMSInsertedDateTime is null then convert(datetime,'jan 1, 1980',107)
                              else #stage_mms_MembershipProductTier_history.MMSInsertedDateTime
                          end
                    else isnull(#stage_mms_MembershipProductTier_history.MMSUpdatedDateTime,convert(datetime,'jan 1, 1980',107)) 
                end dv_load_date_time,
               case when #stage_mms_MembershipProductTier_history.rank2 = 1 then
                         case when #stage_mms_MembershipProductTier_history.MMSInsertedDateTime is null then 19800101000000
                              else replace(replace(replace(convert(varchar, #stage_mms_MembershipProductTier_history.MMSInsertedDateTime,120), '-', ''),' ', ''), ':', '')
                          end
                    else replace(replace(replace(convert(varchar, isnull(#stage_mms_MembershipProductTier_history.MMSUpdatedDateTime,convert(datetime,'jan 1, 1980',107)),120), '-', ''),' ', ''), ':', '')
                end dv_batch_id,
               2 dv_r_load_source_id, 
               #stage_mms_MembershipProductTier_history.l_mms_membership_product_tier_hash,
               getdate() dv_inserted_date_time, 
               suser_sname() dv_insert_user 
           from dbo.#stage_mms_MembershipProductTier_history
          left join dbo.#stage_mms_MembershipProductTier_history prior
            on #stage_mms_MembershipProductTier_history.MembershipProductTierID = prior.MembershipProductTierID
           and #stage_mms_MembershipProductTier_history.rank2 = prior.rank2 + 1
         where #stage_mms_MembershipProductTier_history.l_mms_membership_product_tier_hash != isnull(prior.l_mms_membership_product_tier_hash, ''))x


/* Create the s records.*/
/* dv_batch_id is the dv_load_date_time converted to YYYYMMDDHHMISS*/
insert into dbo.s_mms_membership_product_tier (
       bk_hash,
       membership_product_tier_id,
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
  from (select #stage_mms_MembershipProductTier_history.bk_hash,
       #stage_mms_MembershipProductTier_history.MembershipProductTierID membership_product_tier_id,
	   #stage_mms_MembershipProductTier_history.MMSInsertedDateTime inserted_date_time,
       #stage_mms_MembershipProductTier_history.MMSUpdatedDateTime updated_date_time,
               case when #stage_mms_MembershipProductTier_history.rank2 = 1 then
                         case when #stage_mms_MembershipProductTier_history.MMSInsertedDateTime is null then convert(datetime,'jan 1, 1980',107)
                              else #stage_mms_MembershipProductTier_history.MMSInsertedDateTime
                          end
                    else isnull(#stage_mms_MembershipProductTier_history.MMSUpdatedDateTime,convert(datetime,'jan 1, 1980',107))
                end dv_load_date_time,
               case when #stage_mms_MembershipProductTier_history.rank2 = 1 then
                         case when #stage_mms_MembershipProductTier_history.MMSInsertedDateTime is null then 19800101000000
                              else replace(replace(replace(convert(varchar, #stage_mms_MembershipProductTier_history.MMSInsertedDateTime,120), '-', ''),' ', ''), ':', '')
                          end
                    else replace(replace(replace(convert(varchar, isnull(#stage_mms_MembershipProductTier_history.MMSUpdatedDateTime,convert(datetime,'jan 1, 1980',107)),120), '-', ''),' ', ''), ':', '')
                end dv_batch_id,
               2 dv_r_load_source_id, 
               #stage_mms_MembershipProductTier_history.s_mms_membership_product_tier_hash,
               getdate() dv_inserted_date_time, 
               suser_sname() dv_insert_user
                   from dbo.#stage_mms_MembershipProductTier_history
          left join dbo.#stage_mms_MembershipProductTier_history prior
            on #stage_mms_MembershipProductTier_history.MembershipProductTierID = prior.MembershipProductTierID
           and #stage_mms_MembershipProductTier_history.rank2 = prior.rank2 + 1
         where #stage_mms_MembershipProductTier_history.s_mms_membership_product_tier_hash != isnull(prior.s_mms_membership_product_tier_hash, ''))x

end
