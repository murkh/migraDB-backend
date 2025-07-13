"""Custom row-splitting transform hooks.

Create functions named `split_<source_table>` that accept a single `row` dict
and return a dict mapping *target table name* to list-of-row-dicts to insert.

Example for a `customers` table split into `customers` + `customer_phone`:

```python
import uuid

def split_customers(row):
    cust_id = uuid.uuid4()
    customer = {
        "id": cust_id,
        "name": row["full_name"],
    }
    phones = [
        {"customer_id": cust_id, "number": p.strip()}
        for p in row["phones"].split(";") if p.strip()
    ]
    return {
        "customers": [customer],
        "customer_phone": phones,
    }
```

Optional post-insert hook:

Define `transform_<target_table>(session, rows)` to run immediately after a
batch insert of that table (e.g., to update full-text indexes).
"""
