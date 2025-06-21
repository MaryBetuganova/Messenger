CREATE TABLE users (
    user_id UUID PRIMARY KEY,
    username VARCHAR(255) NOT NULL,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE,
    phone_number VARCHAR(255) UNIQUE,
    profile_picture_url VARCHAR(2048)
);

CREATE TABLE chats (
    chat_id UUID PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    chat_picture_url VARCHAR(2048)
);

CREATE TABLE chatmembersassociation (
    chat_id UUID NOT NULL,
    user_id UUID NOT NULL,
    PRIMARY KEY (chat_id, user_id),
    FOREIGN KEY (chat_id) REFERENCES chats(chat_id),
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);

CREATE TABLE messages (
    message_id UUID PRIMARY KEY,
    chat_id UUID NOT NULL,
    author_id UUID NOT NULL,
    text VARCHAR(4096),
    send_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    FOREIGN KEY (chat_id) REFERENCES chats(chat_id),
    FOREIGN KEY (author_id) REFERENCES users(user_id)
);

CREATE TABLE attachments (
    attachment_id UUID PRIMARY KEY,
    attachment_url VARCHAR(2048) NOT NULL
);

CREATE TABLE messageattachmentassociation (
    attachment_id UUID NOT NULL,
    message_id UUID NOT NULL,
    PRIMARY KEY (attachment_id, message_id),
    FOREIGN KEY (attachment_id) REFERENCES attachments(attachment_id),
    FOREIGN KEY (message_id) REFERENCES messages(message_id)
);