# quickbooks data models

dbt data models for Quickbooks Online.

Currently, these models support transformation of a large number of Quickbooks objects into the corresponding journal transactions so that account flows and balances can be trended over time.

The data structures in this repo are built off of the [Stitch Quickbooks integration](https://www.stitchdata.com/integrations/quickbooks-online/). Other methods of denormalizing Quickbooks data could output slightly different data structures.

The most important model in this repository is `general_ledger`. This model is the running GL for all accounts and should likely form the basis of most analysis. We recommend materializing this model into a table to improve query performance times, as it can take some time to generate with moderate dataset sizes.


### installation

- modify your `packages.yml` to include the following:
```YAML
packages:
  - git: https://github.com/arjunbanker/quickbooks.git
    revision: 0.1.1
```
- copy the models within the `base-models` directory into your dbt project and modify them so that they select from the appropriate tables and fields within your environment.
- run `dbt deps`.

### config

If you're using sources, add this to your source config:

```
  - name: quickbooks
    database: your-database-name-here
    schema: quickbooks
    loaded_at_field: _sdc_batched_at
    tables:
      - name: quickbooks_accounts
      - name: quickbooks_billpayments
      - name: quickbooks_bills
      - name: quickbooks_classes
      - name: quickbooks_customers
      - name: quickbooks_deposits
      - name: quickbooks_invoices
      - name: quickbooks_items
      - name: quickbooks_journalentries
      - name: quickbooks_payments
      - name: quickbooks_purchases
      - name: quickbooks_vendors
    quoting:
      database: true
      schema: true
```

If you're on BigQuery, you're all set. If you are on Redshift, you'll need these additional tables:

```
bills_line
billpayments_line
billpayments_line__linkedtxn
deposits_line
deposits_line_linkedtxn
invoices_lines
journal_entries_line
purchases_line
```

If you don't use classes or other entities, set the appropriate variables to false to your `dbt_project.yml`:

```
quickbooks:
    vars:
      classes_enabled:                        false
      invoices_enabled:                       false
      payments_enabled:                       false
      creditcard_payments_for_bills:          false -- if you don't have any bill payments made by credit card
```

From the data/ folder:
- import quickbooks_classifications.csv to your datawarehouse

### usage

Once installation is completed, `dbt run` will build these models along with the other models in your project.

### contribution

Additional contributions to this repo are very welcome! Please submit PRs to master. All PRs should only include functionality that is contained within generic Quickbooks implementations; no implementation-specific details should be included.
