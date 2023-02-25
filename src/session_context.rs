use datafusion::prelude::CsvReadOptions;
use napi_derive::napi;

use crate::data_frame::DataFrame;

#[napi]
pub struct SessionContext {
  inner: datafusion::prelude::SessionContext,
}

#[napi]
impl SessionContext {
  #[napi(constructor)]
  pub fn new() -> Self {
    Self {
      inner: datafusion::prelude::SessionContext::new(),
    }
  }

  #[napi(factory)]
  /// Create `SessionContext` from an execution config with config options read from the environment
  pub fn with_config_env() -> Result<Self, napi::Error> {
    Ok(Self {
      inner: datafusion::prelude::SessionContext::with_config(
        datafusion::prelude::SessionConfig::from_env().map_err(anyhow::Error::from)?,
      ),
    })
  }

  #[napi]
  /// Creates a [`DataFrame`] that will execute a SQL query.
  ///
  /// Note: This api implements DDL such as `CREATE TABLE` and `CREATE VIEW` with in memory
  /// default implementations.
  pub async fn sql(&self, sql: String) -> Result<DataFrame, napi::Error> {
    let df = self.inner.sql(&sql).await.map_err(anyhow::Error::from)?;
    Ok(DataFrame { inner: Some(df) })
  }

  #[napi]
  /// Creates a [`DataFrame`] for reading a CSV data source.
  pub async fn read_csv(&self, path: String) -> Result<DataFrame, napi::Error> {
    let df = self
      .inner
      .read_csv(path, CsvReadOptions::default())
      .await
      .map_err(anyhow::Error::from)?;
    Ok(DataFrame { inner: Some(df) })
  }
}
