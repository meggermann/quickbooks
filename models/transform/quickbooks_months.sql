select
  {% if target.type == 'bigquery' %}
    date_trunc(date_day, month) as date_month
  {% else %}
    distinct {{ dbt_utils.safe_cast("date_trunc('month', date_day)", 'date') }} as date_month
  {% endif %}
from {{ref('quickbooks_days')}}
where date_day <= current_date
