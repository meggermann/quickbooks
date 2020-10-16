with bill_lines as (

  {% if target.type == 'bigquery' %}

    select
      --only unique within a given bill_id
      {{ dbt_utils.safe_cast('line.id', dbt_utils.type_int()) }} as id,
      {{ dbt_utils.safe_cast('bills.id', dbt_utils.type_int()) }} as bill_id,
      {{ dbt_utils.surrogate_key('line.id', 'bills.id') }} as bill_line_id,
      line.amount,
      
      nullif({{ dbt_utils.safe_cast('accountbasedexpenselinedetail.classref.value', dbt_utils.type_bigint() ) }}, 0) as class_id,

      {{ dbt_utils.safe_cast('accountbasedexpenselinedetail.accountref.value', dbt_utils.type_int()) }} as account_id,
      _sdc_received_at as received_at

    from
      {{ var('base.bills') }} as bills
    cross join unnest(line) as line

  {% else %}

    select
      --only unique within a given bill_id
      {{ dbt_utils.safe_cast('id', dbt_utils.type_int()) }} as id,
      {{ var('source_key_id_field') }} as bill_id,
      {{ dbt_utils.surrogate_key('id', 'bill_id') }} as bill_line_id,
      amount,

      {% if var('classes_enabled', true) %}
        nullif({{ dbt_utils.safe_cast('accountbasedexpenselinedetail__classref__value', dbt_utils.type_bigint() ) }}, 0) as class_id,
      {% endif %}

      {{ dbt_utils.safe_cast('accountbasedexpenselinedetail__accountref__value', dbt_utils.type_int()) }} as account_id,
      _sdc_received_at as received_at

    from
      {{ var('base.bills_line') }}

  {% endif %}

),

deduplicate as (

    select
        *,
        row_number() over (partition by bill_line_id order by
            received_at desc) as dedupe
    from bill_lines

)

select * except (dedupe) from deduplicate
where dedupe = 1
