--deposits debit cash and credit either undeposited funds or a specific other account indicated in the deposit line.
--this query creates both of those transactions.

with deposits as (

  select * from {{ref('quickbooks_deposits')}}

),

{% if var('deposits_lines_enabled', true) %}
lines as (

  select * from {{ref('quickbooks_deposit_lines')}}

),
{% endif %}

d1 as (

  select
    deposits.id,
    deposits.txn_date,
    deposits.account_id as deposit_to_acct_id,
    coalesce(lines.account_id, udf.id) as deposit_from_acct_id,
    lines.amount
    {% if var('classes_enabled', true) %}
    ,
    lines.class_id
    {% endif %}
  from deposits
    {% if var('deposits_lines_enabled', true) %}
    inner join lines on deposits.id = lines.deposit_id
    {% endif %}
    join (select id from {{ref('quickbooks_accounts')}} where subtype = 'UndepositedFunds') udf
      on 1 = 1

)

select
  id,
  txn_date,
  amount,
  deposit_to_acct_id as account_id,
  'debit' as transaction_type,
  'deposit' as source
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
  deposit_from_acct_id,
  'credit',
  'deposit'
  {% if var('classes_enabled', true) %}
  ,
  class_id
  {% endif %}
from d1
