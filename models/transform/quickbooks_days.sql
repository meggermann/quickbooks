select
  {% if target.type == 'bigquery' %}
    date_add(
      min(txn_date) over (),
      interval row_number() over () day
    ) as date_day
  {% else %}
    {{ dbt_utils.safe_cast('(min(txn_date) over () + row_number() over ())', 'date') }} as date_day
  {% endif %}
from {{ref('quickbooks_general_ledger')}}
