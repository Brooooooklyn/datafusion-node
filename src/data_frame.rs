use napi::bindgen_prelude::*;
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
  /// Clone the current `DataFrame` and return a new `DataFrame` instance.
  #[allow(clippy::should_implement_trait)]
  pub fn clone(&self) -> Self {
    Self {
      inner: self.inner.clone(),
    }
  }

  #[napi]
  /// Filter the DataFrame by column. Returns a new DataFrame only containing the
  /// specified columns.
  ///
  /// ```
  /// import { SessionContext } from '@napi-rs/datafusion';
  /// const ctx = new SessionContext();
  /// const df = await ctx.readCsv('tests/data/example.csv');
  /// df.selectColumns(&["a", "b"])?;
  /// ```
  pub fn select_columns(&mut self, columns: Vec<&str>) -> Result<&Self> {
    take_inner!(self, inner, inner.select_columns(&columns));
    Ok(self)
  }

  #[napi]
  /// Create a projection based on arbitrary expressions.
  ///
  /// ```
  /// import { SessionContext } from '@napi-rs/datafusion';
  /// const ctx = new SessionContext();
  /// const df = await ctx.readCsv('tests/data/example.csv');
  /// df.select([col("a") * col("b"), col("c")]);
  /// ```
  pub fn select(&mut self, expr_list: Vec<&Expr>) -> Result<&Self> {
    take_inner!(
      self,
      inner,
      inner.select(expr_list.iter().map(|e| e.value()).collect())
    );
    Ok(self)
  }

  #[napi]
  /// Filter a DataFrame to only include rows that match the specified filter expression.
  pub fn filter(&mut self, expr: &Expr) -> Result<&Self> {
    take_inner!(self, inner, inner.filter(expr.value()));
    Ok(self)
  }

  #[napi]
  /// Perform an aggregate query with optional grouping expressions.
  pub fn aggregate(&mut self, group_expr: Vec<&Expr>, aggr_expr: Vec<&Expr>) -> Result<&Self> {
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
  /// const ctx = new SessionContext();
  /// const df = await ctx.readCsv('tests/data/example.csv');
  /// df.limit(0, Some(100));
  /// ```
  pub fn limit(&mut self, skip: i64, fetch: Option<i64>) -> Result<&Self> {
    take_inner!(
      self,
      inner,
      inner.limit(skip as usize, fetch.map(|v| v as usize))
    );
    Ok(self)
  }

  #[napi]
  /// Calculate the union of two [`DataFrame`]s, preserving duplicate rows.The
  /// two [`DataFrame`]s must have exactly the same schema
  ///
  /// ```
  /// import { SessionContext } from '@napi-rs/datafusion';
  /// const ctx = new SessionContext();
  /// const df = await ctx.readCsv('tests/data/example.csv');
  /// const d2 = df.clone();
  /// df.union(d2);
  /// ```
  pub fn union(&mut self, dataframe: &DataFrame) -> Result<&Self> {
    take_inner!(self, inner, inner.union(dataframe.value()));
    Ok(self)
  }

  #[napi]
  /// Calculate the distinct union of two [`DataFrame`]s.  The
  /// two [`DataFrame`]s must have exactly the same schema
  ///
  /// ```
  /// import { SessionContext } from '@napi-rs/datafusion';
  /// const ctx = new SessionContext();
  /// const df = await ctx.readCsv('tests/data/example.csv');
  /// df.distinct();
  /// ```
  pub fn union_distinct(&mut self, dataframe: &DataFrame) -> Result<&Self> {
    take_inner!(self, inner, inner.union_distinct(dataframe.value()));
    Ok(self)
  }

  #[napi]
  /// Filter out duplicate rows
  ///
  /// ```
  /// import { SessionContext } from '@napi-rs/datafusion';
  /// const ctx = new SessionContext();
  /// const df = await ctx.readCsv('tests/data/example.csv');
  /// df.distinct(d2);
  /// ```
  pub fn distinct(&mut self) -> Result<&Self> {
    take_inner!(self, inner, inner.distinct());
    Ok(self)
  }

  #[napi]
  /// Sort the DataFrame by the specified sorting expressions. Any expression can be turned into
  /// a sort expression by calling its [sort](../logical_plan/enum.Expr.html#method.sort) method.
  ///
  /// ```
  /// import { SessionContext } from '@napi-rs/datafusion';
  /// const ctx = new SessionContext();
  /// const df = await ctx.readCsv('tests/data/example.csv');
  /// df.sort([col('a').sort(true, true), col('b').sort(false, false)]);
  /// ```
  pub fn sort(&mut self, expr: Vec<&Expr>) -> Result<&Self> {
    take_inner!(
      self,
      inner,
      inner.sort(expr.iter().map(|e| e.value()).collect())
    );
    Ok(self)
  }

  /// Join this DataFrame with another DataFrame using the specified columns as join keys.
  ///
  /// Filter expression expected to contain non-equality predicates that can not be pushed
  /// down to any of join inputs.
  /// In case of outer join, filter applied to only matched rows.
  ///
  /// ```
  /// import { SessionContext } from '@napi-rs/datafusion';
  /// const ctx = new SessionContext();
  /// const left = await ctx.readCsv('tests/data/example.csv');
  /// const right = (await ctx.readCsv('tests/data/example.csv'))
  ///   .select([
  ///     col('a').alias('a2'),
  ///     col('b').alias('b2'),
  ///     col('c').alias('c2'),
  ///   ]);
  /// const batches = await left.join(right, JoinType::Inner, ['a', 'b'], ['a2', 'b2']).collect();
  /// ```
  pub fn join(
    &mut self,
    right: &DataFrame,
    join_type: JoinType,
    left_cols: Vec<&str>,
    right_cols: Vec<&str>,
    filter: Option<&Expr>,
  ) -> Result<&Self> {
    take_inner!(
      self,
      inner,
      inner.join(
        right.value(),
        join_type.into(),
        left_cols.as_ref(),
        right_cols.as_ref(),
        filter.map(|f| f.value())
      )
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
  pub async fn show(&self) -> Result<()> {
    self
      .inner
      .clone()
      .unwrap()
      .show()
      .await
      .map_err(anyhow::Error::from)?;
    Ok(())
  }

  pub(crate) fn value(&self) -> datafusion::prelude::DataFrame {
    if let Some(value) = &self.inner {
      value.clone()
    } else {
      unreachable!("DataFrame must have a value")
    }
  }
}

#[napi]
/// Join type
#[derive(Debug, PartialEq, Eq, Hash)]
pub enum JoinType {
  /// Inner Join
  Inner,
  /// Left Join
  Left,
  /// Right Join
  Right,
  /// Full Join
  Full,
  /// Left Semi Join
  LeftSemi,
  /// Right Semi Join
  RightSemi,
  /// Left Anti Join
  LeftAnti,
  /// Right Anti Join
  RightAnti,
}

impl From<JoinType> for datafusion::prelude::JoinType {
  fn from(value: JoinType) -> Self {
    match value {
      JoinType::Inner => datafusion::prelude::JoinType::Inner,
      JoinType::Left => datafusion::prelude::JoinType::Left,
      JoinType::Right => datafusion::prelude::JoinType::Right,
      JoinType::Full => datafusion::prelude::JoinType::Full,
      JoinType::LeftSemi => datafusion::prelude::JoinType::LeftSemi,
      JoinType::RightSemi => datafusion::prelude::JoinType::RightSemi,
      JoinType::LeftAnti => datafusion::prelude::JoinType::LeftAnti,
      JoinType::RightAnti => datafusion::prelude::JoinType::RightAnti,
    }
  }
}
