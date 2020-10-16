{{
  config(
    enabled = var('classes_enabled', true)
  )
}}

with classes as (

  select
    {{ dbt_utils.safe_cast('id', dbt_utils.type_bigint()) }} as id,
    name,
    fullyqualifiedname as fully_qualified_name,
    active,
    subclass,
    sparse,
    domain,
    {% if target.type == 'bigquery' %}
      nullif({{ dbt_utils.safe_cast('parentref.value', dbt_utils.type_bigint()) }}, 0) as parent_class_id,
      metadata.createtime as created_at,
      metadata.lastupdatedtime as updated_at,
    {% else %}
      nullif({{ dbt_utils.safe_cast('parentref__value', dbt_utils.type_bigint()) }}, 0) as parent_class_id,
      metadata__createtime as created_at,
      metadata__lastupdatedtime as updated_at,
    {% endif %}
    _sdc_received_at as received_at
  from
  {{ var('base.classes') }}

),

deduplicate as (

    select
        *,
        row_number() over (partition by id order by
            received_at desc) as dedupe
    from classes

)

select * except (dedupe) from deduplicate
where dedupe = 1
