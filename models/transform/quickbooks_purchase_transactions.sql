--purchases debit expense and credits the payment account (cash or credit).
--this query creates both of those transactions.

with purchases as (

  select * from {{ref('quickbooks_purchases')}}

),

purchase_lines as (

  select * from {{ref('quickbooks_purchase_lines')}}

),

accounts as (

  select * from {{ref('quickbooks_accounts_xf')}}

),

d1 as (

  select
    purchases.id,
    purchases.txn_date,
    purchases.account_id as payed_from_acct_id,
    case coalesce(purchases.credit, {{ dbt_utils.safe_cast('false', 'bool') }})
      when true then 'debit'
    else 'credit'
    end as payed_from_transaction_type,
    case coalesce(purchases.credit, {{ dbt_utils.safe_cast('false', 'bool') }})
      when true then 'credit'
    else 'debit'
    end as payed_to_transaction_type,
    purchase_lines.amount,
    purchase_lines.account_id as payed_to_acct_id
    {% if var('classes_enabled', true) %}
      ,
      purchase_lines.class_id
    {% endif %}
  from purchases
    inner join purchase_lines on purchases.id = purchase_lines.purchase_id

)

select
  id,
  txn_date,
  amount,
  payed_from_acct_id as account_id,
  payed_from_transaction_type as transaction_type,
  'purchase' as source
  {% if var('classes_enabled', true) %}
    ,
    class_id
  {% endif %}
from d1

union all

select
  id,
  txn_date,
  amount,
  payed_to_acct_id,
  payed_to_transaction_type,
  'purchase'
  {% if var('classes_enabled', true) %}
    ,
    class_id
  {% endif %}
from d1
