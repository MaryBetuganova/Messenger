# Messenger

## Структура

```plaintext
messenger/
├── README.md
├── .env
├── docker-compose.yml
├── seeder/
│   ├── Dockerfile.seeder
│   ├── entry.sh           // скрипт для запуска сидирования
│   ├── requirements.txt   // зависимости Python
│   └── seeder.py          // скрипт сидирования
├── postgres/
│   └── schema.sql         // миграции
└── cassandra/
    └── schema.cql         // миграции
```

## Примеры запросов

### PostgreSQL

1. Найти топ-5 чатов с наибольшим количеством участников
```sql
SELECT c.chat_id, c.name, COUNT(cma.user_id) AS member_count
FROM chats c
JOIN chatmembersassociation cma ON c.chat_id = cma.chat_id
GROUP BY c.chat_id, c.name
ORDER BY member_count DESC
LIMIT 5;
```

2. Найти количество сообщений в каждом чате, упорядочив их по возрастанию
```sql
SELECT c.chat_id, c.name, COUNT(m.message_id) AS message_count
FROM chats c
LEFT JOIN messages m ON c.chat_id = m.chat_id
GROUP BY  c.chat_id, c.name
ORDER BY message_count ASC;
```

3. Найти количество чатов у каждого пользователя
```sql
SELECT u.user_id, u.username, COUNT(cma.chat_id) AS chat_count
FROM users u
LEFT JOIN chatmembersassociation cma ON u.user_id = cma.user_id
GROUP BY u.user_id, u.username;
```

4. Найти количество вложений в чате
```sql
SELECT c.name AS chat_name, COUNT(maa.attachment_id) AS attachment_count
FROM chats c
JOIN messages m ON c.chat_id = m.chat_id
JOIN messageattachmentassociation maa ON m.message_id = maa.message_id
WHERE c.chat_id = 'example_id'
GROUP BY c.chat_id, c.name;
```

### Cassandra

1. Найти всех участников конкретного чата
```cassandraql
SELECT user_id FROM chatmembersassociation WHERE chat_id = example_id;
```

2. Найти все вложения у конкретного сообщения
```cassandraql
SELECT attachment_id FROM messageattachmentassociation WHERE message_id = example_id;
```

3. Получить информацию о чате
```cassandraql
SELECT * FROM chats WHERE chat_id = example_id;
```