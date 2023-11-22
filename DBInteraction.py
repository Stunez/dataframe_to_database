""" 
    Python 3.7+ compatibility.
    libs -> Pandas 1.4.1 version
            Sqlalchemy 1.4.47 version
            
    By using Python Pandas DataFrame you can upload 
your duplicated and not duplicated data to database.
Tested only on PostgreSQL 12 version.

    Made by Daulet Beknazarov @Stunez
"""

import pandas as pd

from sqlalchemy import create_engine, dialects, text, bindparam

class DBInteractionException(Exception):
    def __init__(self, message):
        super().__init__(message)


class DBInteraction(object):
    """ Class, as the name itself says, for interaction with the DBMS
     using DML statements (SELECT, INSERT, UPDATE, DELETE, MERGE).
     """
    
    def __init__(self, database : str, schema : str, engine : str, if_exists='append'):
        """ Database: str, schema: str, 
        engine: sqlalchemy.engine.(Engine or Connection) or sqlite3.Connection,
        if_exists{‘fail’, ‘replace’, ‘append’}, default ‘append’.
        """
        self.database = database
        self.schema = schema
        self.engine = engine
        self.if_exists = if_exists
        
        self.postgresql_types = {
            'bigint': dialects.postgresql.BIGINT,
            'character varying': dialects.postgresql.VARCHAR,
            'integer': dialects.postgresql.INTEGER,
            'smallint': dialects.postgresql.SMALLINT,
            'text': dialects.postgresql.TEXT,
            'jsonb': dialects.postgresql.JSONB,
            'timestamp without time zone': dialects.postgresql.TIMESTAMP,
        }
        
        self.select_constraint = {
        }
    
    
    def get_constraint_of_table(self, table_name):
        """ Gets of table constraint for a given table.
        
        if record exists, then add contraint name to the "select_constraint"
        mapper. 
        
        If record not exists, then you should add constraint name by yourself 
        into "select_constraint".
        """
        if self.select_constraint.get(table_name, None) is None:
            query = f"""--sql \n
                SELECT
                    tc.constraint_name
                FROM
                    information_schema.key_column_usage AS kcu
                JOIN
                    information_schema.table_constraints AS tc
                    ON tc.constraint_name = kcu.constraint_name
                        and tc.table_catalog = kcu.table_catalog 
                        and tc.table_schema = kcu.table_schema 
                        and tc.table_name = kcu.table_name
                where kcu.table_catalog = '{self.database}'
                and kcu.constraint_schema = '{self.schema}'
                and kcu.table_name = '{table_name}'
            """
            result_df = pd.read_sql(query, self.engine)
            if result_df.shape[0] == 1:
                self.select_constraint.update({table_name: result_df['constraint_name'].iloc[0]})
            elif result_df.shape[0] > 1:
                msg = 'Count of CONSTRAINTS in the table is more then 1! add value to select_constraint'
                raise DBInteractionException(msg)
            else:
                msg = 'Count of CONSTRAINTS in the table is equal to 0! add value to select_constraint'
                raise DBInteractionException(msg)
    
    
    def updater_table(self, table, engine, keys, data_iter):
        """ If records is exists in the table of the database,
        then this method is used to update records.
        
            select_constraint - its a mapper, which executes
        role to determine index conflict when adding
        records in the database. By the table name  
        you can pull out unique index from the mapper.
        
            При существовании записи в табличке в БД 
        этот метод используется для обновления записей.
        
            select_constraint - этот маппер исполняет 
        роль для определения конфликта индекса при добавлении 
        записи в БД. С помощью названия таблички можно вытащить 
        уникальный индекс из маппера.  
        """
        print('Starting method updater_table!')
        column_names = ', '.join(keys) 
        
        upload_dtype = self.get_table_attr_types(table.name)
        self.get_constraint_of_table(table.name)
        
        # для обработки значении query запроса 
        values_str = ', '.join([':' + x for x in keys])
        set_values_str = ', '.join([x + ' = ' + ':' + x for x in keys[1:]])
        
        # Итерация записей
        for data in data_iter:
            # query sample
            query = text(f"""
                INSERT INTO {self.schema}.{table.name}
                ({column_names})
                VALUES({values_str})
                ON CONFLICT ON CONSTRAINT {self.select_constraint.get(table.name, None)}
                DO UPDATE
                SET {set_values_str}
            """)
            
            bind_params_list = []
            for N, column in enumerate(keys):
                param = bindparam(
                    column,
                    value=data[N],
                    type_=upload_dtype.get(column)
                )
                bind_params_list.append(param)
            query = query.bindparams(*bind_params_list)
            
            # Connection to DB
            with engine.connect() as conn:
                conn.execute(query)
                
        print('Successfully data updated!')
        
        
    def get_table_attr_types(self, table_name):
        """ Получение типов данных у атрибутов в posgresql."""
        query = f"""--sql \n
            SELECT 
                column_name, 
                data_type 
            FROM information_schema.columns
            WHERE 1=1
                AND table_name = '{table_name}'
                AND table_schema = '{self.schema}'
        """
        table_type = pd.read_sql(query, self.engine)
        
        upload_dtype = {}
        for column_type in table_type.to_dict('split')['data']:
            upload_dtype.update(
                {column_type[0]: self.postgresql_types.get(column_type[1], None)}
            )
        if None in upload_dtype.values():
            print(upload_dtype)
            msg = "ERROR: ONE OF THE DATATYPES IS NOT DECLARED. THEN RETURNED NONETYPE"
            DBInteractionException(msg)
            
        return upload_dtype
    
    
    def data_upload(self, data_df, table_name, update_table=False):
        """ Главный процесс который запускает добавление данных в базу
        или же запускает ее обновление данных.
        """
        print('data_upload starting...')
        _method_upload = None
        if update_table:
            _method_upload = self.updater_table

        upload_dtype = self.get_table_attr_types(table_name)
        
        data_df.to_sql(
            name=table_name,
            con=self.engine,
            schema=self.schema,
            if_exists=self.if_exists,
            index=False,
            dtype=upload_dtype,
            method=_method_upload,
        )
        print('data_upload finished...')

if __name__ == '__main__':
    DB = DBInteraction('Database_name', 'Schema_name', create_engine())