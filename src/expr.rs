use napi::bindgen_prelude::*;
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

  #[napi]
  /// Return `self && other`
  pub fn and(&mut self, other: &Expr) -> &Self {
    take_inner!(self, inner, inner.and(other.value()));
    self
  }

  #[napi]
  /// Return `self || other`
  pub fn or(&mut self, other: &Expr) -> &Self {
    take_inner!(self, inner, inner.or(other.value()));
    self
  }

  #[napi]
  /// Return `!self`
  pub fn not(&mut self) -> &Self {
    take_inner!(self, inner, inner.not());
    self
  }

  #[napi]
  /// Calculate the modulus of two expressions.
  /// Return `self % other`
  pub fn modulus(&mut self, other: &Expr) -> &Self {
    take_inner!(self, inner, inner.modulus(other.value()));
    self
  }

  #[napi]
  /// Return `self LIKE other`
  pub fn like(&mut self, other: &Expr) -> &Self {
    take_inner!(self, inner, inner.like(other.value()));
    self
  }

  #[napi]
  /// Return `self NOT LIKE other`
  pub fn not_like(&mut self, other: &Expr) -> &Self {
    take_inner!(self, inner, inner.not_like(other.value()));
    self
  }

  #[napi]
  /// Return `self ILIKE other`
  pub fn ilike(&mut self, other: &Expr) -> &Self {
    take_inner!(self, inner, inner.ilike(other.value()));
    self
  }

  #[napi]
  /// Return `self NOT ILIKE other`
  pub fn not_ilike(&mut self, other: &Expr) -> &Self {
    take_inner!(self, inner, inner.not_ilike(other.value()));
    self
  }

  #[napi]
  /// Remove an alias from an expression if one exists.
  pub fn unalias(&mut self) -> &Self {
    take_inner!(self, inner, inner.unalias());
    self
  }

  #[napi]
  /// Return `self IN <list>` if `negated` is false, otherwise
  /// return `self NOT IN <list>`.a
  pub fn in_list(&mut self, list: Vec<&Expr>, negated: bool) -> &Self {
    take_inner!(
      self,
      inner,
      inner.in_list(list.into_iter().map(|e| e.value()).collect(), negated)
    );
    self
  }

  #[napi]
  /// Return `IsNull(Box(self))
  pub fn is_null(&mut self) -> &Self {
    take_inner!(self, inner, inner.is_null());
    self
  }

  #[napi]
  /// Return `IsNotNull(Box(self))
  pub fn is_not_null(&mut self) -> &Self {
    take_inner!(self, inner, inner.is_not_null());
    self
  }

  #[napi]
  /// Create a sort expression from an existing expression.
  ///
  /// ```
  /// const sortExpr = col('foo').sort(true, true); // SORT ASC NULLS_FIRST
  /// ```
  pub fn sort(&mut self, asc: bool, nulls_first: bool) -> &Self {
    take_inner!(self, inner, inner.sort(asc, nulls_first));
    self
  }

  #[napi]
  /// Return `IsTrue(Box(self))`
  pub fn is_true(&mut self) -> &Self {
    take_inner!(self, inner, inner.is_true());
    self
  }

  #[napi]
  /// Return `IsNotTrue(Box(self))`
  pub fn is_not_true(&mut self) -> &Self {
    take_inner!(self, inner, inner.is_not_true());
    self
  }

  #[napi]
  /// Return `IsFalse(Box(self))`
  pub fn is_false(&mut self) -> &Self {
    take_inner!(self, inner, inner.is_false());
    self
  }

  #[napi]
  /// Return `IsNotFalse(Box(self))`
  pub fn is_not_false(&mut self) -> &Self {
    take_inner!(self, inner, inner.is_not_false());
    self
  }

  #[napi]
  /// Return `IsUnknown(Box(self))`
  pub fn is_unknown(&mut self) -> &Self {
    take_inner!(self, inner, inner.is_unknown());
    self
  }

  #[napi]
  /// Return `IsNotUnknown(Box(self))`
  pub fn is_not_unknown(&mut self) -> &Self {
    take_inner!(self, inner, inner.is_not_unknown());
    self
  }

  #[napi]
  /// Clone the `Expr` and return the new `Expr` instance.
  #[allow(clippy::should_implement_trait)]
  pub fn clone(&self) -> Self {
    Self {
      inner: self.inner.clone(),
    }
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
/// Create a column expression based on a qualified or unqualified column name
///
/// example:
/// ```
/// const c = col('my_column');
/// ```
pub fn col(name: String) -> Expr {
  Expr {
    inner: Some(datafusion::prelude::col(name)),
  }
}

#[napi]
/// Return a new expression `left <op> right`
pub fn binary_expr(left: &Expr, op: Operator, right: &Expr) -> Expr {
  Expr {
    inner: Some(datafusion::prelude::binary_expr(
      left.value(),
      op.into(),
      right.value(),
    )),
  }
}

#[napi]
/// Return a new expression `left <op> right`
pub fn and(left: &Expr, right: &Expr) -> Expr {
  Expr {
    inner: Some(datafusion::prelude::and(left.value(), right.value())),
  }
}

#[napi]
/// Return a new expression with a logical OR
pub fn or(left: &Expr, right: &Expr) -> Expr {
  Expr {
    inner: Some(datafusion::prelude::or(left.value(), right.value())),
  }
}

#[napi]
/// Create an expression to represent the min() aggregate function
pub fn min(expr: &Expr) -> Expr {
  Expr {
    inner: Some(datafusion::prelude::min(expr.value())),
  }
}

#[napi]
/// Create an expression to represent the max() aggregate function
pub fn max(expr: &Expr) -> Expr {
  Expr {
    inner: Some(datafusion::prelude::max(expr.value())),
  }
}

#[napi]
/// Create an expression to represent the sum() aggregate function
pub fn sum(expr: &Expr) -> Expr {
  Expr {
    inner: Some(datafusion::prelude::sum(expr.value())),
  }
}

#[napi]
/// Create an expression to represent the avg() aggregate function
pub fn avg(expr: &Expr) -> Expr {
  Expr {
    inner: Some(datafusion::prelude::avg(expr.value())),
  }
}

#[napi]
/// Create an expression to represent the count() aggregate function
pub fn count(expr: &Expr) -> Expr {
  Expr {
    inner: Some(datafusion::prelude::count(expr.value())),
  }
}

#[napi]
/// Create an expression to represent the count(distinct) aggregate function
pub fn count_distinct(expr: &Expr) -> Expr {
  Expr {
    inner: Some(datafusion::prelude::count_distinct(expr.value())),
  }
}

#[napi]
/// Create an in_list expression
pub fn in_list(expr: &Expr, list: Vec<&Expr>, negated: bool) -> Expr {
  Expr {
    inner: Some(datafusion::prelude::in_list(
      expr.value(),
      list.into_iter().map(|e| e.value()).collect(),
      negated,
    )),
  }
}

#[napi]
/// Concatenates the text representations of all the arguments. NULL arguments are ignored.
pub fn concat(args: Vec<&Expr>) -> Expr {
  Expr {
    inner: Some(datafusion::prelude::concat(
      args
        .into_iter()
        .map(|e| e.value())
        .collect::<Vec<datafusion::prelude::Expr>>()
        .as_ref(),
    )),
  }
}

#[napi]
/// Concatenates all but the first argument, with separators.
/// The first argument is used as the separator.
/// NULL arguments in `values` are ignored.
pub fn concat_ws(sep: &Expr, values: Vec<&Expr>) -> Expr {
  Expr {
    inner: Some(datafusion::prelude::concat_ws(
      sep.value(),
      values.into_iter().map(|e| e.value()).collect(),
    )),
  }
}

#[napi]
/// Returns a random value in the range 0.0 <= x < 1.0
pub fn random() -> Expr {
  Expr {
    inner: Some(datafusion::prelude::random()),
  }
}

#[napi]
/// Returns the approximate number of distinct input values.
/// This function provides an approximation of count(DISTINCT x).
/// Zero is returned if all input values are null.
/// This function should produce a standard error of 0.81%,
/// which is the standard deviation of the (approximately normal)
/// error distribution over all possible sets.
/// It does not guarantee an upper bound on the error for any specific input set.
pub fn approx_distinct(expr: &Expr) -> Expr {
  Expr {
    inner: Some(datafusion::prelude::approx_distinct(expr.value())),
  }
}

#[napi]
/// Calculate an approximation of the median for `expr`.
pub fn approx_median(expr: &Expr) -> Expr {
  Expr {
    inner: Some(datafusion::prelude::approx_median(expr.value())),
  }
}

#[napi]
/// Calculate an approximation of the specified `percentile` for `expr`.
pub fn approx_percentile_cont(expr: &Expr, percentile: &Expr) -> Expr {
  Expr {
    inner: Some(datafusion::prelude::approx_percentile_cont(
      expr.value(),
      percentile.value(),
    )),
  }
}

#[napi]
/// Calculate an approximation of the specified `percentile` for `expr` and `weight_expr`.
pub fn approx_percentile_cont_with_weight(
  expr: &Expr,
  weight_expr: &Expr,
  percentile: &Expr,
) -> Expr {
  Expr {
    inner: Some(datafusion::prelude::approx_percentile_cont_with_weight(
      expr.value(),
      weight_expr.value(),
      percentile.value(),
    )),
  }
}

#[napi]
/// Create a grouping set
pub fn grouping_set(exprs: Vec<Vec<&Expr>>) -> Expr {
  Expr {
    inner: Some(datafusion::prelude::grouping_set(
      exprs
        .into_iter()
        .map(|e| e.into_iter().map(|e| e.value()).collect())
        .collect(),
    )),
  }
}

#[napi]
/// Create a grouping set for all combination of `exprs`
pub fn cube(exprs: Vec<&Expr>) -> Expr {
  Expr {
    inner: Some(datafusion::prelude::cube(
      exprs.into_iter().map(|e| e.value()).collect(),
    )),
  }
}

#[napi]
/// Create a grouping set for rollup
pub fn rollup(exprs: Vec<&Expr>) -> Expr {
  Expr {
    inner: Some(datafusion::prelude::rollup(
      exprs.into_iter().map(|e| e.value()).collect(),
    )),
  }
}

#[napi]
/// Create is null expression
pub fn is_null(expr: &Expr) -> Expr {
  Expr {
    inner: Some(datafusion::prelude::is_null(expr.value())),
  }
}

#[napi]
/// Create is true expression
pub fn is_true(expr: &Expr) -> Expr {
  Expr {
    inner: Some(datafusion::prelude::is_true(expr.value())),
  }
}

#[napi]
/// Create is not true expression
pub fn is_not_true(expr: &Expr) -> Expr {
  Expr {
    inner: Some(datafusion::prelude::is_not_true(expr.value())),
  }
}

#[napi]
/// Create is false expression
pub fn is_false(expr: &Expr) -> Expr {
  Expr {
    inner: Some(datafusion::prelude::is_false(expr.value())),
  }
}

#[napi]
/// Create is not false expression
pub fn is_not_false(expr: &Expr) -> Expr {
  Expr {
    inner: Some(datafusion::prelude::is_not_false(expr.value())),
  }
}

#[napi]
/// Create is unknown expression
pub fn is_unknown(expr: &Expr) -> Expr {
  Expr {
    inner: Some(datafusion::prelude::is_unknown(expr.value())),
  }
}

#[napi]
/// Create is not unknown expression
pub fn is_not_unknown(expr: &Expr) -> Expr {
  Expr {
    inner: Some(datafusion::prelude::is_not_unknown(expr.value())),
  }
}

#[napi]
/// Operators applied to expressions
#[derive(PartialEq, Eq, PartialOrd, Hash)]
pub enum Operator {
  /// Expressions are equal
  Eq,
  /// Expressions are not equal
  NotEq,
  /// Left side is smaller than right side
  Lt,
  /// Left side is smaller or equal to right side
  LtEq,
  /// Left side is greater than right side
  Gt,
  /// Left side is greater or equal to right side
  GtEq,
  /// Addition
  Plus,
  /// Subtraction
  Minus,
  /// Multiplication operator, like `*`
  Multiply,
  /// Division operator, like `/`
  Divide,
  /// Remainder operator, like `%`
  Modulo,
  /// Logical AND, like `&&`
  And,
  /// Logical OR, like `||`
  Or,
  /// IS DISTINCT FROM
  IsDistinctFrom,
  /// IS NOT DISTINCT FROM
  IsNotDistinctFrom,
  /// Case sensitive regex match
  RegexMatch,
  /// Case insensitive regex match
  RegexIMatch,
  /// Case sensitive regex not match
  RegexNotMatch,
  /// Case insensitive regex not match
  RegexNotIMatch,
  /// Bitwise and, like `&`
  BitwiseAnd,
  /// Bitwise or, like `|`
  BitwiseOr,
  /// Bitwise xor, like `#`
  BitwiseXor,
  /// Bitwise right, like `>>`
  BitwiseShiftRight,
  /// Bitwise left, like `<<`
  BitwiseShiftLeft,
  /// String concat
  StringConcat,
}

impl From<Operator> for datafusion::logical_expr::Operator {
  fn from(op: Operator) -> Self {
    match op {
      Operator::Eq => datafusion::logical_expr::Operator::Eq,
      Operator::NotEq => datafusion::logical_expr::Operator::NotEq,
      Operator::Lt => datafusion::logical_expr::Operator::Lt,
      Operator::LtEq => datafusion::logical_expr::Operator::LtEq,
      Operator::Gt => datafusion::logical_expr::Operator::Gt,
      Operator::GtEq => datafusion::logical_expr::Operator::GtEq,
      Operator::Plus => datafusion::logical_expr::Operator::Plus,
      Operator::Minus => datafusion::logical_expr::Operator::Minus,
      Operator::Multiply => datafusion::logical_expr::Operator::Multiply,
      Operator::Divide => datafusion::logical_expr::Operator::Divide,
      Operator::Modulo => datafusion::logical_expr::Operator::Modulo,
      Operator::And => datafusion::logical_expr::Operator::And,
      Operator::Or => datafusion::logical_expr::Operator::Or,
      Operator::IsDistinctFrom => datafusion::logical_expr::Operator::IsDistinctFrom,
      Operator::IsNotDistinctFrom => datafusion::logical_expr::Operator::IsNotDistinctFrom,
      Operator::RegexMatch => datafusion::logical_expr::Operator::RegexMatch,
      Operator::RegexIMatch => datafusion::logical_expr::Operator::RegexIMatch,
      Operator::RegexNotMatch => datafusion::logical_expr::Operator::RegexNotMatch,
      Operator::RegexNotIMatch => datafusion::logical_expr::Operator::RegexNotIMatch,
      Operator::BitwiseAnd => datafusion::logical_expr::Operator::BitwiseAnd,
      Operator::BitwiseOr => datafusion::logical_expr::Operator::BitwiseOr,
      Operator::BitwiseXor => datafusion::logical_expr::Operator::BitwiseXor,
      Operator::BitwiseShiftRight => datafusion::logical_expr::Operator::BitwiseShiftRight,
      Operator::BitwiseShiftLeft => datafusion::logical_expr::Operator::BitwiseShiftLeft,
      Operator::StringConcat => datafusion::logical_expr::Operator::StringConcat,
    }
  }
}
