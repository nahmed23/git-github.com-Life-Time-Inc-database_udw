﻿CREATE VIEW [marketing].[v_dim_mdm_golden_record_customer] AS select dim_mdm_golden_record_customer.dim_mdm_golden_record_customer_key dim_mdm_golden_record_customer_key,
       dim_mdm_golden_record_customer.entity_id entity_id,
       dim_mdm_golden_record_customer.birth_date birth_date,
       dim_mdm_golden_record_customer.email_1 email_1,
       dim_mdm_golden_record_customer.first_name first_name,
       dim_mdm_golden_record_customer.former_member_flag former_member_flag,
       dim_mdm_golden_record_customer.last_name last_name,
       dim_mdm_golden_record_customer.middle_name middle_name,
       dim_mdm_golden_record_customer.phone_1 phone_1,
       dim_mdm_golden_record_customer.postal_address_city postal_address_city,
       dim_mdm_golden_record_customer.postal_address_line_1 postal_address_line_1,
       dim_mdm_golden_record_customer.postal_address_line_2 postal_address_line_2,
       dim_mdm_golden_record_customer.postal_address_state postal_address_state,
       dim_mdm_golden_record_customer.postal_address_zip_code postal_address_zip_code,
       dim_mdm_golden_record_customer.prefix_name prefix_name,
       dim_mdm_golden_record_customer.sex sex,
       dim_mdm_golden_record_customer.suffix_name suffix_name
  from dbo.dim_mdm_golden_record_customer;