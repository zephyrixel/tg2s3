use crate::model::{BucketRecord, CorsConfiguration, TelegramBackend};
use anyhow::{Context, Result};
use sqlx::Row;

use super::Db;
use super::support::now;

impl Db {
    pub async fn create_bucket(&self, name: &str) -> Result<bool> {
        let result = sqlx::query("INSERT OR IGNORE INTO buckets(name, created_at) VALUES(?1, ?2)")
            .bind(name)
            .bind(now())
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected() == 1)
    }

    pub async fn bucket_exists(&self, name: &str) -> Result<bool> {
        let exists =
            sqlx::query_scalar::<_, i64>("SELECT EXISTS(SELECT 1 FROM buckets WHERE name = ?1)")
                .bind(name)
                .fetch_one(&self.pool)
                .await?;
        Ok(exists != 0)
    }

    pub async fn has_backend(&self, backend: TelegramBackend) -> Result<bool> {
        let exists = sqlx::query_scalar::<_, i64>(
            "SELECT EXISTS(SELECT 1 FROM telegram_blocks WHERE backend = ?1)",
        )
        .bind(backend.as_str())
        .fetch_one(&self.pool)
        .await?;
        Ok(exists != 0)
    }

    pub async fn list_buckets(&self) -> Result<Vec<BucketRecord>> {
        let rows = sqlx::query("SELECT name, created_at FROM buckets ORDER BY name")
            .fetch_all(&self.pool)
            .await?;
        Ok(rows
            .into_iter()
            .map(|row| BucketRecord {
                name: row.get("name"),
                created_at: row.get("created_at"),
            })
            .collect())
    }

    pub async fn delete_bucket(&self, name: &str) -> Result<Option<bool>> {
        let mut tx = self.pool.begin().await?;
        let exists =
            sqlx::query_scalar::<_, i64>("SELECT EXISTS(SELECT 1 FROM buckets WHERE name = ?1)")
                .bind(name)
                .fetch_one(&mut *tx)
                .await?;
        if exists == 0 {
            tx.rollback().await?;
            return Ok(None);
        }
        let occupied = sqlx::query_scalar::<_, i64>(
            "SELECT EXISTS(SELECT 1 FROM objects WHERE bucket = ?1)
             OR EXISTS(SELECT 1 FROM multipart_uploads WHERE bucket = ?1)",
        )
        .bind(name)
        .fetch_one(&mut *tx)
        .await?;
        if occupied != 0 {
            tx.rollback().await?;
            return Ok(Some(false));
        }
        sqlx::query("DELETE FROM bucket_cors WHERE bucket = ?1")
            .bind(name)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM buckets WHERE name = ?1")
            .bind(name)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(Some(true))
    }

    pub async fn get_bucket_cors(&self, bucket: &str) -> Result<Option<CorsConfiguration>> {
        let row = sqlx::query("SELECT configuration_json FROM bucket_cors WHERE bucket = ?1")
            .bind(bucket)
            .fetch_optional(&self.pool)
            .await?;
        row.map(|row| {
            serde_json::from_str(&row.get::<String, _>("configuration_json"))
                .context("decode stored bucket CORS configuration")
        })
        .transpose()
    }

    pub async fn set_bucket_cors(
        &self,
        bucket: &str,
        configuration: &CorsConfiguration,
    ) -> Result<()> {
        let timestamp = now();
        sqlx::query(
            "INSERT INTO bucket_cors(bucket, configuration_json, created_at, updated_at)
             VALUES(?1, ?2, ?3, ?3)
             ON CONFLICT(bucket) DO UPDATE SET
               configuration_json = excluded.configuration_json,
               updated_at = excluded.updated_at",
        )
        .bind(bucket)
        .bind(serde_json::to_string(configuration)?)
        .bind(timestamp)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn delete_bucket_cors(&self, bucket: &str) -> Result<()> {
        sqlx::query("DELETE FROM bucket_cors WHERE bucket = ?1")
            .bind(bucket)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}
