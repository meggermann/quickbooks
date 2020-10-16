with lines as (

  {% if target.type == 'bigquery' %}

    select
      {{ dbt_utils.surrogate_key('line.id', 'entries.id') }} as id,
      {{ dbt_utils.safe_cast('entries.id', dbt_utils.type_int()) }} as entry_id,
      coalesce(
        {{ dbt_utils.safe_cast('amount', 'numeric') }},
        {{ dbt_utils.safe_cast('0', 'numeric') }}
      ) as amount,
      description,
      {{ dbt_utils.safe_cast("nullif(journalentrylinedetail.accountref.value, '')", dbt_utils.type_int()) }} as account_id,
      {% if var('classes_enabled', true) %}
        {{ dbt_utils.safe_cast('journalentrylinedetail.classref.value', dbt_utils.type_bigint()) }} as class_id,
      {% endif %}
      journalentrylinedetail.postingtype as posting_type,
      _sdc_received_at as received_at
    from
      {{ var('base.journal_entries') }} as entries
      cross join unnest(line) as line

  {% else %}

    select
      --this id is only unique within a given entry_id; may also want to create a globally unique id for this table.
      {{ dbt_utils.safe_cast('id', dbt_utils.type_int()) }} as id,
      {{ var('source_key_id_field') }} as entry_id,
      coalesce(
        {{ dbt_utils.safe_cast('amount', 'numeric(38,6)') }},
        {{ dbt_utils.safe_cast('0', 'numeric(38,6)') }}
      ) as amount,
      description,
      {{ dbt_utils.safe_cast("nullif(journalentrylinedetail__accountref__value, '')", dbt_utils.type_int()) }} as account_id,
      {% if var('classes_enabled', true) %}
        {{ dbt_utils.safe_cast('journalentrylinedetail__classref__value', dbt_utils.type_bigint()) }} as class_id,
      {% endif %}
      journalentrylinedetail__postingtype as posting_type,
      _sdc_received_at as received_at
    from
      {{ var('base.journal_entries_line') }}

  {% endif %}

),

deduplicate as (

    select
        *,
        row_number() over (partition by id order by
            received_at desc) as dedupe
    from lines

)

select * except (dedupe) from deduplicate
where dedupe = 1
