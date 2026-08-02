use super::block_size;
use anyhow::Result;

#[test]
fn calculates_non_empty_block_sizes() -> Result<()> {
    assert_eq!(block_size(8, 0, 20)?, 8);
    assert_eq!(block_size(8, 8, 20)?, 8);
    assert_eq!(block_size(8, 16, 20)?, 4);
    assert!(block_size(0, 0, 20).is_err());
    assert!(block_size(8, 20, 20).is_err());
    Ok(())
}
