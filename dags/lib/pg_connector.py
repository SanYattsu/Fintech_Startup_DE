from sqlalchemy import MetaData, create_engine, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import bindparam
from sqlalchemy.sql import functions as F
from sqlalchemy import inspect


class PgConnect:
    def __init__(self, host: str, port: int, db_name: str,
                 user: str, pw: str, sslmode: str = None) -> None:
        connection_str = f"postgresql://{user}:{pw}@{host}:{port}/{db_name}"
        if sslmode:
            connection_str += f"?sslmode={sslmode}"
            
        # Создать движек
        self.engine = create_engine(connection_str)

        self.meta = MetaData()
        self.tables = {}

        # Инспектор, он выводит детальные параметы таблиц (уникальность, PK)
        self.inspector = inspect(self.engine)

    def get_table(self, table: str, schema: str):
        if f'{schema}.{table}' not in self.tables:
            self.meta.reflect(bind=self.engine, schema=schema, views=True)
            self.tables[f'{schema}.{table}'] = self.meta.tables[f'{schema}.{table}']

        return self.meta.tables[f'{schema}.{table}']

    def get_data(self, table: str, schema: str, whereclause: str = None):
        tmp = self.get_table(table, schema)

        # Можно сделать выбор данных из БД с условиями
        if whereclause is not None:
            stmt = tmp.select().where(text(whereclause))
        else:
            stmt = tmp.select()

        with self.engine.connect() as conn:
            with conn.begin():
                return conn.execute(stmt)
        
    def upsert_do_nothing(self, table: str, schema: str, values: list, columns: list):
        # Получим поля по которым идет уникальность
        constraints = self.inspector.get_unique_constraints(
            table_name=table, schema=schema
        )[0]['column_names']
        
        # ORM таблицы
        target_table = self.get_table(table, schema)
        
        # Вставка
        insert_stmt = insert(target_table).values(
            {x: bindparam(x) for x in columns}
        )
        
        # Пропуск строк при конфликте
        update_stmt = insert_stmt.on_conflict_do_nothing(index_elements=[
            target_table.c.get(x) for x in constraints
        ])

        return self.commit_to_db(update_stmt, values)

    def upsert_do_update(self, table: str, schema: str, values: list, columns: list):
        # Получим поля по которым идет уникальность
        constraints = self.inspector.get_unique_constraints(
            table_name=table, schema=schema
        )[0]['column_names']
        
        # ORM таблицы
        target_table = self.get_table(table, schema)
        
        # Вставка
        insert_stmt = insert(target_table).values(
            {x: bindparam(x) for x in columns}
        )
        
        # Обновление строк при конфликте
        update_stmt = insert_stmt.on_conflict_do_update(
            index_elements=[target_table.c.get(x) for x in constraints],
            set_={x:insert_stmt.excluded.get(x) for x in set(columns).difference(constraints)}
        )
        
        return self.commit_to_db(update_stmt, values)
        
    def commit_to_db(self, stmt: str, values: list):
        with self.engine.connect() as conn:
            with conn.begin():
                result = conn.execute(
                    stmt,
                    values
                )
                return result.rowcount