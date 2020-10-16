with accounts as (

  select * from {{ref('quickbooks_accounts_xf')}}

),

ledger as (

  select * from {{ref('quickbooks_ledger_xf')}}

),

months as (

  select * from {{ref('quickbooks_months')}}

),

monthly_account_totals as (

  select
    {% if target.type == 'bigquery' %}
      date_trunc(txn_date, month) as date_month,
    {% else %}
      date_trunc('month', txn_date) as date_month,
    {% endif %}
    account_id,
    sum(adj_amount) as total
  from ledger
  group by 1, 2

),

account_months as (

  select id as account_id, date_month
  from accounts
    join months on 1=1

)

select am.date_month, am.account_id, coalesce(mat.total, 0.0) as total,
  sum(coalesce(mat.total, 0.0)) over (partition by am.account_id order by am.date_month rows unbounded preceding) as cumulative_total
from account_months am
  left outer join monthly_account_totals mat on
    am.account_id = mat.account_id and
    am.date_month = mat.date_month
order by 1, 2
