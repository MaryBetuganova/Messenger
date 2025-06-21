import os
import psycopg2
import random
from faker import Faker
from psycopg2 import sql
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement
import uuid

cassandra_auth_provider = PlainTextAuthProvider(username=os.getenv("CASSANDRA_USER"),
                                                password=os.getenv("CASSANDRA_PASSWORD"))

seed_count = os.getenv("SEED_COUNT")

cassandra_cluster = Cluster([os.getenv("CASSANDRA_HOST")],
                            port=os.getenv("CASSANDRA_PORT"),
                            auth_provider=cassandra_auth_provider)

cassandra_session = cassandra_cluster.connect()

cassandra_session.set_keyspace(os.getenv("CASSANDRA_DB"))

pg_connection = psycopg2.connect(
    dbname=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    host=os.getenv("POSTGRES_HOST"),
    port=os.getenv("POSTGRES_PORT")
)

fake_generator = Faker()

userCache = dict()
userIDs = dict()
messageCache = dict()
messageIDs = dict()
chatCache = dict()
chatIDs = dict()
chatUserBondCache = dict()
attachmentCache = dict()
attachmentIDs = dict()


seed_count = int(os.getenv("SEED_COUNT"))
class PostgreSQL:
    def __init__(self):
        pass

    @staticmethod
    def is_table_exists(table_name, table_schema='public'):
        with pg_connection.cursor() as cur:
            cur.execute("""
                select exists (
                select 1 from information_schema.tables
                where table_schema = %s
                and table_name = %s
                );
            """, (table_schema, table_name))
            ans = cur.fetchone()[0]
            if not ans:
                print(f"table {table_name} not found")
            
            return ans

    @staticmethod
    def is_column_exists(table_name, column_name, table_schema='public'):
        if not PostgreSQL.is_table_exists(table_name, table_schema):
            return False

        with pg_connection.cursor() as cur:
            cur.execute("""
                select exists (
                    select column_name from information_schema.columns
                    where table_schema = %s and table_name = %s and column_name = %s
                );
            """, (table_schema, table_name, column_name))
            return cur.fetchone()[0]

    @staticmethod
    def insert_row(table_name, data_dict):
        if not PostgreSQL.is_table_exists(table_name):
            return False

        actual_data=dict()

        for column in data_dict:
            if PostgreSQL.is_column_exists(table_name, column):
                actual_data[column] = data_dict[column]
            else:
                print(f"failed to insert {column}")

        query = sql.SQL("insert into {} ({}) values ({})").format(
            sql.Identifier(table_name),
            sql.SQL(', ').join(map(sql.Identifier, actual_data.keys())),
            sql.SQL(', ').join([sql.Placeholder()] * len(actual_data))
        )

        with pg_connection.cursor() as cur:
            try:
                cur.execute(query, list(actual_data.values()))
                pg_connection.commit()

                return True

            except psycopg2.IntegrityError as e:
                pg_connection.rollback()
                print("Integrity error")
                return False

            except Exception as e:
                pg_connection.rollback()
                print(f"Unknown error {e}")
                return False

    @staticmethod
    def get_row(table_name, column_name):
        if not PostgreSQL.is_table_exists(table_name):
            return list()

        if not PostgreSQL.is_column_exists(table_name, column_name):
            return list()

        query = sql.SQL("select {} from {}").format(
            sql.Identifier(column_name),
            sql.Identifier(table_name)
        )

        with pg_connection.cursor() as cur:
            try:
                cur.execute(query)

                return [row[0] for row in cur.fetchall()]

            except psycopg2.IntegrityError as e:
                pg_connection.rollback()
                print("Integrity error")
                return list()

            except Exception as e:
                pg_connection.rollback()
                print(f"Unknown error {e}")
                return list()


class CassandraDB:
    def __init__(self):
        pass

    @staticmethod
    def is_table_exists(table_name):
        return table_name in cassandra_session.cluster.metadata.keyspaces[cassandra_session.keyspace].tables

    @staticmethod
    def is_column_exists(table_name, column_name):
        if not CassandraDB.is_table_exists(table_name):
            return False

        return column_name in cassandra_session.cluster.metadata.keyspaces[cassandra_session.keyspace].tables[table_name].columns

    @staticmethod
    def insert_row(table_name, data_dict):
        if not CassandraDB.is_table_exists(table_name):
            return False

        actual_data=dict()

        for column in data_dict:
            if CassandraDB.is_column_exists(table_name, column):
                actual_data[column] = data_dict[column]
            else:
                print(f"failed to insert {column}")

        table_ref = f"{cassandra_session.keyspace}.{table_name}"
        cols = list(actual_data.keys())
        columns = ", ".join(cols)
        placeholders = ", ".join(["%s"] * len(cols))

        query = f"INSERT INTO {table_ref} ({columns}) VALUES ({placeholders})"
        statement = SimpleStatement(query)
        cassandra_session.execute(statement, list(actual_data.values()))

        return True

    @staticmethod
    def get_rows(table_name, columns):
        if not CassandraDB.is_table_exists(table_name):
            return False

        actual_columns=list()

        for column in columns:
            if CassandraDB.is_column_exists(table_name, column):
                actual_columns.append(column)

        table_ref = f"{cassandra_session.keyspace}.{table_name}"
        columns_ref = ", ".join(actual_columns.keys)

        query = f"SELECT {columns_ref} FROM {table_ref}"
        statement = SimpleStatement(query)

        rows = cassandra_session.execute(statement)

        return rows

# -------------------------------Relation seeds-------------------------------------------

def user_seed():
    for i in range(1, seed_count + 1):
        userCache[i] = {
            "username": fake_generator.user_name(),
            "name": fake_generator.name(),
            "email": fake_generator.email(),
            "phone_number": fake_generator.phone_number(),
            "profile_picture_url": fake_generator.url()
        }

        userIDs[i] = uuid.uuid4()
        PostgreSQL.insert_row("users", {"user_id": str(userIDs[i]), **userCache[i]})
        CassandraDB.insert_row("users", {"user_id": userIDs[i], **userCache[i]})
    print("users done")

def chat_seed():
    for i in range(1, seed_count + 1):
        chatCache[i] = {
            "name": fake_generator.name() + fake_generator.word(),
            "chat_picture_url": fake_generator.url()
        }

        chatIDs[i] = uuid.uuid4()
        PostgreSQL.insert_row("chats", {"chat_id": str(chatIDs[i]), **chatCache[i]})
        CassandraDB.insert_row("chats", {"chat_id": chatIDs[i], **chatCache[i]})
    print("chats done")

def member_association_seed():
    users = set(range(1, seed_count + 1))
    for i in range(1, seed_count + 1):
        chatUserBondCache[i] = set()
        for j in users:
            if random.choice([True, False]) or len(chatUserBondCache[i]) == 0:
                chatUserBondCache[i].add(j)
                PostgreSQL.insert_row("chatmembersassociation", {
                    "chat_id": str(chatIDs[i]),
                    "user_id": str(userIDs[j])
                })
                CassandraDB.insert_row("chatmembersassociation", {
                    "chat_id": chatIDs[i],
                    "user_id": userIDs[j]
                })
    print("members done")

def message_seed():
    for i in range(1, seed_count * 3):
        chat = random.choice(list(chatCache.keys()))
        user = random.choice(list(chatUserBondCache[chat]))

        messageIDs[i] = uuid.uuid4()
        messageCache[i] = {
            "chat_id": chatIDs[chat],
            "author_id": userIDs[user],
            "text": fake_generator.paragraph(),
            "send_at": fake_generator.date_time(),
            "updated_at": fake_generator.date_time()
        }

        CassandraDB.insert_row("messages", {"message_id": messageIDs[i], **messageCache[i]})

        messageCache[i]["chat_id"] = str(chatIDs[chat])
        messageCache[i]["author_id"] = str(userIDs[user])
        PostgreSQL.insert_row("messages", {"message_id": str(messageIDs[i]), **messageCache[i]})
    print("messages done")

def attachment_seed():
    for i in range(1, seed_count + 1):
        attachmentCache[i] = {"attachment_url": fake_generator.url()}
        attachmentIDs[i] = uuid.uuid4()
        PostgreSQL.insert_row("attachments", {"attachment_id": str(attachmentIDs[i]), **attachmentCache[i]})
        CassandraDB.insert_row("attachments", {"attachment_id": attachmentIDs[i], **attachmentCache[i]})
    print("attachments done")

def attachment_message_association_seed():
    for i in attachmentCache.keys():
        message = random.choice(list(messageCache.keys()))
        PostgreSQL.insert_row("messageattachmentassociation", {
            "attachment_id": str(attachmentIDs[i]),
            "message_id": str(messageIDs[message])
        })
        CassandraDB.insert_row("messageattachmentassociation", {
            "attachment_id": attachmentIDs[i],
            "message_id": messageIDs[message]
        })
    print("last container is done")

#---------------------------------------main----------------------------------------------

if __name__ == "__main__":
    user_seed()
    chat_seed()
    member_association_seed()
    message_seed()
    attachment_seed()
    attachment_message_association_seed()

    pg_connection.close()
    cassandra_session.shutdown()