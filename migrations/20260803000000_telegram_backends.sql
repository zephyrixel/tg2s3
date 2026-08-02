ALTER TABLE telegram_blocks
    ADD COLUMN backend TEXT NOT NULL DEFAULT 'bot_api';

ALTER TABLE telegram_blocks
    ADD COLUMN document_id INTEGER;
