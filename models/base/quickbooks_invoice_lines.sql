{{
  config(
    enabled = var('invoices_enabled', true)
  )
}}

with invoices_lines as (

  {% if target.type == 'bigquery' %}

    select
      {{ dbt_utils.surrogate_key('line.id', 'invoices.id') }} as id,
      {{ dbt_utils.safe_cast('invoices.id', dbt_utils.type_int()) }} as invoice_id,
      amount,
      description,
      {% if var('classes_enabled', true) %}
        {{ dbt_utils.safe_cast('salesitemlinedetail.classref.value', dbt_utils.type_bigint()) }} as class_id,
      {% endif %}
      {{ dbt_utils.safe_cast('salesitemlinedetail.itemref.value', dbt_utils.type_int()) }} as item_id,
      _sdc_received_at as received_at

    from
      {{ var('base.invoices') }} as invoices
      cross join unnest(line) as line
    where
      detailtype = 'SalesItemLineDetail'

  {% else %}

    select
      --this id is only unique within a given invoice_id; may also want to create a globally unique id for this table.
      {{ dbt_utils.safe_cast('id', dbt_utils.type_int()) }} as id,
      amount,
      description,
      {{ dbt_utils.safe_cast(var('source_key_id_field'), dbt_utils.type_int()) }} as invoice_id,
      {% if var('classes_enabled', true) %}
        {{ dbt_utils.safe_cast('salesitemlinedetail__classref__value', dbt_utils.type_bigint()) }} as class_id,
      {% endif %}
      {{ dbt_utils.safe_cast('salesitemlinedetail__itemref__value', dbt_utils.type_int()) }} as item_id,
      _sdc_received_at as received_at

    from
      {{ var('base.invoices_lines') }}
    where
      detailtype = 'SalesItemLineDetail'

  {% endif %}

),

deduplicate as (

    select
        *,
        row_number() over (partition by id order by
            received_at desc) as dedupe
    from invoices_lines

)

select * except (dedupe) from deduplicate
where dedupe = 1
