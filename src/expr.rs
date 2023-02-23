use napi_derive::napi;

macro_rules! take_inner {
  ($self:ident, $inner:ident, $lit:expr) => {{
    if let Some($inner) = std::mem::replace(&mut $self.inner, None) {
      $self.inner = Some($lit);
    }
  }};
}

#[napi]
pub struct Expr {
  pub(crate) inner: Option<datafusion::prelude::Expr>,
}

#[napi]
impl Expr {
  #[napi]
  /// Create a literal expression
  pub fn lit(value: String) -> Self {
    Self {
      inner: Some(datafusion::prelude::lit(value)),
    }
  }

  #[napi]
  /// Return `self AS name` alias expression
  pub fn alias(&mut self, name: String) -> &Self {
    take_inner!(self, inner, inner.alias(name));
    self
  }

  #[napi]
  /// Return `self <= other`
  pub fn lt_eq(&mut self, other: &Expr) -> &Self {
    take_inner!(self, inner, inner.lt_eq(other.value()));
    self
  }

  #[inline]
  pub(crate) fn value(&self) -> datafusion::prelude::Expr {
    if let Some(v) = &self.inner {
      v.clone()
    } else {
      unreachable!("Expr must have a value");
    }
  }
}

#[napi]
pub fn col(name: String) -> Expr {
  Expr {
    inner: Some(datafusion::prelude::col(name)),
  }
}
