CREATE TABLE users (
    user_id UUID PRIMARY KEY,
    username TEXT,
    name TEXT,
    email TEXT,
    phone_number TEXT,
    profile_picture_url TEXT
);

CREATE INDEX ON users(email);
CREATE INDEX ON users(phone_number);

CREATE TABLE chats (
    chat_id UUID PRIMARY KEY,
    name TEXT,
    chat_picture_url TEXT
);

CREATE TABLE chatmembersassociation (
    chat_id UUID,
    user_id UUID,
    PRIMARY KEY (chat_id, user_id)
);

CREATE TABLE messages (
    message_id UUID,
    chat_id UUID,
    author_id UUID,
    text TEXT,
    send_at TIMESTAMP,
    updated_at TIMESTAMP,
    PRIMARY KEY (chat_id, message_id)
);

CREATE TABLE attachments (
    attachment_id UUID PRIMARY KEY,
    attachment_url TEXT
);

CREATE TABLE messageattachmentassociation (
    attachment_id UUID,
    message_id UUID,
    PRIMARY KEY (message_id, attachment_id)
);