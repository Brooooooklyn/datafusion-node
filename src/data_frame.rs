use napi_derive::napi;

use crate::expr::Expr;

macro_rules! take_inner {
  ($self:ident, $inner:ident, $op:expr) => {{
    if let Some($inner) = std::mem::replace(&mut $self.inner, None) {
      $self.inner = Some($op.map_err(anyhow::Error::from)?);
    }
  }};
}

#[napi]
pub struct DataFrame {
  pub(crate) inner: Option<datafusion::prelude::DataFrame>,
}

#[napi]
impl DataFrame {
  #[napi]
  /// Filter a DataFrame to only include rows that match the specified filter expression.
  pub fn filter(&mut self, expr: &Expr) -> Result<&Self, napi::Error> {
    take_inner!(self, inner, inner.filter(expr.value()));
    Ok(self)
  }

  #[napi]
  /// Perform an aggregate query with optional grouping expressions.
  pub fn aggregate(
    &mut self,
    group_expr: Vec<&Expr>,
    aggr_expr: Vec<&Expr>,
  ) -> Result<&Self, napi::Error> {
    take_inner!(
      self,
      inner,
      inner.aggregate(
        group_expr.iter().map(|e| e.value()).collect(),
        aggr_expr.iter().map(|e| e.value()).collect()
      )
    );
    Ok(self)
  }

  #[napi]
  /// Limit the number of rows returned from this DataFrame.
  ///
  /// `skip` - Number of rows to skip before fetch any row
  ///
  /// `fetch` - Maximum number of rows to fetch, after skipping `skip` rows.
  /// ```
  /// import { SessionContext } from '@napi-rs/datafusion';
  /// const ctx = SessionContext::new();
  /// const df = await ctx.readCsv("tests/data/example.csv");
  /// const df = df.limit(0, Some(100));
  /// ```
  pub fn limit(&mut self, skip: i64, fetch: Option<i64>) -> Result<&Self, napi::Error> {
    take_inner!(
      self,
      inner,
      inner.limit(skip as usize, fetch.map(|v| v as usize))
    );
    Ok(self)
  }

  #[napi]
  /// Print results.
  ///
  /// ```
  /// import { SessionContext } from '@napi-rs/datafusion'
  ///
  /// const ctx = new SessionContext();
  /// const df = await ctx.readCsv('tests/data/example.csv');
  /// await df.show();
  /// ```
  pub async fn show(&self) -> Result<(), napi::Error> {
    self
      .inner
      .clone()
      .unwrap()
      .show()
      .await
      .map_err(anyhow::Error::from)?;
    Ok(())
  }
}
