with source as (

  {% if target.type == 'bigquery' %}

    select
      {{ dbt_utils.surrogate_key('line.id', 'purchases.id') }} as id,
      amount,
      {{ dbt_utils.safe_cast('purchases.id', dbt_utils.type_int()) }} as purchase_id,
      {% if var('classes_enabled', true) %}
        {{ dbt_utils.safe_cast('accountbasedexpenselinedetail.classref.value', dbt_utils.type_bigint()) }} as class_id,
      {% endif %}
      {{ dbt_utils.safe_cast('accountbasedexpenselinedetail.accountref.value', dbt_utils.type_int()) }} as account_id,
      _sdc_received_at as received_at
    from
      {{ var('base.purchases') }} as purchases
      cross join unnest(line) as line

  {% else %}

    select
      --this id is only unique within a given purchase_id; may also want to create a globally unique id for this table.
      {{ dbt_utils.safe_cast('id', dbt_utils.type_int()) }} as id,
      amount,
      {{ dbt_utils.safe_cast(var('source_key_id_field'), dbt_utils.type_int()) }} as purchase_id,

      {% if var('classes_enabled', true) %}
        {{ dbt_utils.safe_cast('accountbasedexpenselinedetail__classref__value', dbt_utils.type_bigint()) }} as class_id,
      {% endif %}
      {{ dbt_utils.safe_cast('accountbasedexpenselinedetail__accountref__value', dbt_utils.type_int()) }} as account_id,
      _sdc_received_at as received_at
    from
      {{ var('base.purchases_line') }}

  {% endif %}

),

deduplicate as (

    select
        *,
        row_number() over (partition by id order by
            received_at desc) as dedupe
    from source

)

select * except (dedupe) from deduplicate
where dedupe = 1
