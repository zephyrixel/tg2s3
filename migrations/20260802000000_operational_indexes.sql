CREATE INDEX IF NOT EXISTS idx_object_blocks_block
    ON object_blocks(block_id);

CREATE INDEX IF NOT EXISTS idx_multipart_part_blocks_block
    ON multipart_part_blocks(block_id);
