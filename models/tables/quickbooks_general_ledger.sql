with unioned as (

  select
    id,
    txn_date,
    amount,
    account_id,
    {{ dbt_utils.safe_cast('transaction_type', dbt_utils.type_string()) }} as transaction_type,
    {{ dbt_utils.safe_cast('source', dbt_utils.type_string()) }} as source
    {% if var('classes_enabled', true) %}
      ,
      class_id
    {% endif %}
  from {{ref('quickbooks_bill_transactions')}}

  union all
  select * from {{ref('quickbooks_billpayment_transactions')}}

  {% if var('invoices_enabled', true) %}
    union all
    select * from {{ref('quickbooks_invoice_transactions')}}
  {% endif %}

  union all
  select * from {{ref('quickbooks_purchase_transactions')}}

  union all
  select * from {{ref('quickbooks_journal_transactions')}}

  union all
  select * from {{ref('quickbooks_deposit_transactions')}}

  {% if var('payments_enabled', true) %}
    union all
    select * from {{ref('quickbooks_payment_transactions')}}
  {% endif %}

),

accounts as (

  select * from {{ref('quickbooks_accounts_xf')}}

)

select
  unioned.*,
  case
    when accounts.account_type = unioned.transaction_type
      then amount
    else
      amount * -1
  end as adj_amount
from unioned
  inner join accounts on unioned.account_id = accounts.id
