# dataframe_to_database
By using Python Pandas DataFrame you can upload 
your duplicated and not duplicated data to database.
Tested only on PostgreSQL 12 version.

### Run

```python
From DBInteraction import DBInteraction
From sqlalchemy import create_engine

DB = DBInteraction('Database_name', 'Schema_name', create_engine())

DB.data_upload(Pandas.DataFrame, table_name, update_table=(True or False, False as default))
```

### Python Libraries
```
# Python 3.7+ compatibility.
#    libs -> Pandas 1.4.1 version
#            Sqlalchemy 1.4.47 version
```
---
Made by Daulet Beknazarov @Stunez

