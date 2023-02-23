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

  #[napi]
  pub async fn read_csv(&self, path: String) -> Result<DataFrame, napi::Error> {
    let df = self
      .inner
      .read_csv(path, CsvReadOptions::default())
      .await
      .map_err(anyhow::Error::from)?;
    Ok(DataFrame { inner: Some(df) })
  }
}
