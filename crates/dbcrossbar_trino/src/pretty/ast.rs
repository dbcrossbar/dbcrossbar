//! A simple abstract syntax tree for small parts of Trino SQL. We use this to
//! generate Trino SQL expressions and format them in a readable manner.

use std::fmt::{self, Debug};

use pretty::RcDoc;
#[cfg(any(test, feature = "proptest"))]
use proptest_derive::Arbitrary;

use super::{comma_sep_list, parens, square_brackets, INDENT};
use crate::{DataType, Ident, QuotedString};

/// Construct a static identifier known at compile time. Must not be the empty
/// string.
pub fn ident(s: &'static str) -> Ident {
    // The `unwrap` here should be safe because `TrinoIdent::new` only fails on
    // empty identifiers, and we're only called internally with string literals
    // supplied by the programmer.
    Ident::new(s).unwrap()
}

/// An expression in Trino SQL.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum Expr {
    /// A literal value.
    Lit(SimpleValue),
    /// A variable reference.
    Var(Ident),
    /// A binary operation.
    BinOp {
        /// The left-hand side of the operation.
        lhs: Box<Expr>,
        /// The operator.
        op: BinOp,
        /// The right-hand side of the operation.
        rhs: Box<Expr>,
    },
    /// A function call.
    Func {
        /// The name of the function. We could use `TrinoIdent` here, but that
        /// automatically downcases and quotes function nmaes. And we have a
        /// fixed set of function names that we know at compile time.
        name: &'static str,
        /// The arguments to the function.
        args: Vec<Expr>,
    },
    /// A cast.
    Cast {
        /// The expression to cast.
        expr: Box<Expr>,
        /// The type to cast to.
        ty: DataType,
    },
    /// A `CASE` expression (match version).
    CaseMatch {
        /// The value to match.
        value: Box<Expr>,
        /// The list of `WHEN`/`THEN` pairs.
        when_clauses: Vec<(Expr, Expr)>,
        /// The `ELSE` value.
        r#else: Box<Expr>,
    },
    /// A lambda (`->`) expression.
    Lambda {
        /// The argument name.
        arg: Ident,
        /// The body of the lambda.
        body: Box<Expr>,
    },
    /// An `ARRAY[..]` expression.
    Array(Vec<Expr>),
    /// An array or row element access (`expr[index]`).
    Index {
        /// The array or row.
        expr: Box<Expr>,
        /// The index.
        index: Box<Expr>,
    },
    /// A field reference.
    Field {
        /// The row.
        row: Box<Expr>,
        /// The field name.
        field: Ident,
    },
    /// An `AT TIME ZONE` expression.
    AtTimeZone {
        /// The expression to format.
        expr: Box<Expr>,
        /// The time zone to use.
        time_zone: Box<Expr>,
    },
    /// Raw SQL.
    RawSql(String),
}

impl Expr {
    /// A string literal.
    pub fn str<S: Into<String>>(s: S) -> Expr {
        Expr::Lit(SimpleValue::String(s.into()))
    }

    /// An integer literal.
    pub fn int(i: i64) -> Expr {
        Expr::Lit(SimpleValue::Int(i))
    }

    /// A Boolean literal.
    pub fn bool(b: bool) -> Expr {
        Expr::Lit(SimpleValue::Bool(b))
    }

    /// A NULL literal.
    pub fn null() -> Expr {
        Expr::Lit(SimpleValue::Null)
    }

    /// A binary operation.
    pub fn binop(lhs: Expr, op: BinOp, rhs: Expr) -> Expr {
        Expr::BinOp {
            lhs: Box::new(lhs),
            op,
            rhs: Box::new(rhs),
        }
    }

    /// A function call.
    pub fn func(name: &'static str, args: Vec<Expr>) -> Expr {
        Expr::Func { name, args }
    }

    /// A cast.
    pub fn cast(expr: Expr, ty: DataType) -> Expr {
        Expr::Cast {
            expr: Box::new(expr),
            ty,
        }
    }

    /// An `IF` expression.
    pub fn r#if(cond: Expr, then: Expr, r#else: Expr) -> Expr {
        Expr::func("IF", vec![cond, then, r#else])
    }

    /// A `CASE` expression (match version).
    pub fn case_match(
        value: Expr,
        when_clauses: Vec<(Expr, Expr)>,
        r#else: Expr,
    ) -> Expr {
        Expr::CaseMatch {
            value: Box::new(value),
            when_clauses,
            r#else: Box::new(r#else),
        }
    }

    /// A lambda (`->`) expression.
    pub fn lambda(arg: Ident, body: Expr) -> Expr {
        Expr::Lambda {
            arg,
            body: Box::new(body),
        }
    }

    /// Serialize as JSON.
    pub fn json_to_string(expr: Expr) -> Expr {
        Expr::func("JSON_FORMAT", vec![expr])
    }

    // Cast to JSON, then serialize.
    pub fn json_to_string_with_cast(expr: Expr) -> Expr {
        Self::json_to_string(Self::cast(expr, DataType::Json))
    }

    /// An `ARRAY` expression.
    pub fn array(exprs: Vec<Expr>) -> Expr {
        Expr::Array(exprs)
    }

    /// An array or row element access (positional).
    pub fn index(expr: Expr, index: Expr) -> Expr {
        Expr::Index {
            expr: Box::new(expr),
            index: Box::new(index),
        }
    }

    /// A row field access by name.
    pub fn field(row: Expr, field: Ident) -> Expr {
        Expr::Field {
            row: Box::new(row),
            field,
        }
    }

    /// Bind a variable in a lambda. We fake this using `ARRAY` and `TRANSFORM`,
    /// because Trino doesn't seem to have any better way to do this.
    pub fn bind_var(var: Ident, expr: Expr, body: Expr) -> Expr {
        Expr::index(
            Expr::func(
                "TRANSFORM",
                vec![Expr::array(vec![expr]), Expr::lambda(var, body)],
            ),
            Expr::int(1),
        )
    }

    /// Like [`bind_var`], but with a type cast.
    ///
    /// This exists to work around what appear to be type inference bugs in
    /// Trino version 445-460. See the `dbcrossbar` test
    /// `cp_csv_to_trino_to_csv_lambda_regression`.
    pub fn bind_var_with_return_type(
        var: Ident,
        expr: Expr,
        body: Expr,
        return_ty: &DataType,
    ) -> Expr {
        Expr::index(
            Expr::cast(
                Expr::func(
                    "TRANSFORM",
                    vec![Expr::array(vec![expr]), Expr::lambda(var, body)],
                ),
                DataType::Array(Box::new(return_ty.to_owned())),
            ),
            Expr::int(1),
        )
    }

    /// A `ROW` expression with a `CAST`.
    pub fn row(ty: DataType, exprs: Vec<Expr>) -> Expr {
        Expr::cast(Expr::row_with_anonymous_fields(exprs), ty)
    }

    /// A `ROW` expression without a `CAST`. This may only have anonymous
    /// fields, unless you `CAST` it later.
    pub fn row_with_anonymous_fields(exprs: Vec<Expr>) -> Expr {
        Expr::func("ROW", exprs)
    }

    /// An [`RcDoc`] expression.
    pub fn at_time_zone(expr: Expr, time_zone: &str) -> Expr {
        Expr::AtTimeZone {
            expr: Box::new(expr),
            time_zone: Box::new(Expr::str(time_zone)),
        }
    }

    /// A raw SQL expression.
    pub fn raw_sql(s: impl fmt::Display) -> Expr {
        Expr::RawSql(s.to_string())
    }
}

impl Expr {
    /// Return a pretty-printed version of `self``.
    pub fn to_doc(&self) -> RcDoc<'static, ()> {
        match self {
            Expr::Lit(lit) => lit.to_doc(),
            Expr::Var(ident) => RcDoc::as_string(ident),
            // Canonical multi-line format for a binop is "LHS\nOP RHS".
            Expr::BinOp { lhs, op, rhs } => RcDoc::concat(vec![
                lhs.to_doc(),
                RcDoc::line(),
                op.to_doc(),
                RcDoc::space(),
                rhs.to_doc(),
            ])
            .group(),
            Expr::Func { name, args } => {
                let args = args.iter().map(|a| a.to_doc().group());
                RcDoc::concat(vec![
                    RcDoc::as_string(name),
                    parens(comma_sep_list(args)),
                ])
                .group()
            }
            Expr::Cast { expr, ty } => RcDoc::concat(vec![
                RcDoc::as_string("CAST"),
                parens(RcDoc::concat(vec![
                    expr.to_doc(),
                    RcDoc::line(),
                    RcDoc::as_string("AS"),
                    RcDoc::space(),
                    ty.to_doc(),
                ])),
            ])
            .group(),
            Expr::CaseMatch {
                value,
                when_clauses,
                r#else,
            } => RcDoc::concat(vec![
                RcDoc::concat(vec![
                    RcDoc::as_string("CASE"),
                    RcDoc::space(),
                    value.to_doc(),
                ])
                .group(),
                RcDoc::line(),
                RcDoc::concat(when_clauses.iter().map(|(w, t)| {
                    RcDoc::concat(vec![
                        RcDoc::concat(vec![
                            RcDoc::concat(vec![
                                RcDoc::as_string("WHEN"),
                                RcDoc::space(),
                                w.to_doc(),
                            ])
                            .group(),
                            RcDoc::line(),
                            RcDoc::concat(vec![
                                RcDoc::as_string("THEN"),
                                RcDoc::line(),
                                t.to_doc().nest(INDENT),
                            ])
                            .group(),
                        ])
                        .group(),
                        RcDoc::line(),
                    ])
                })),
                RcDoc::concat(vec![
                    RcDoc::as_string("ELSE"),
                    RcDoc::line(),
                    r#else.to_doc().nest(INDENT),
                ])
                .group(),
                RcDoc::line(),
                RcDoc::as_string("END"),
            ])
            .group(),
            Expr::Lambda { arg, body } => RcDoc::concat(vec![
                RcDoc::as_string(arg),
                RcDoc::space(),
                RcDoc::as_string("->"),
                RcDoc::concat(vec![RcDoc::line(), body.to_doc()])
                    .nest(INDENT)
                    .group(),
            ])
            .group(),
            Expr::Array(exprs) => RcDoc::concat(vec![
                RcDoc::as_string("ARRAY"),
                square_brackets(comma_sep_list(
                    exprs.iter().map(|e| e.to_doc().group()),
                )),
            ])
            .group(),
            Expr::Index { expr, index } => RcDoc::concat(vec![
                expr.to_doc(),
                square_brackets(index.to_doc().group()),
            ]),
            Expr::Field { row, field } => RcDoc::concat(vec![
                row.to_doc(),
                RcDoc::text("."),
                RcDoc::as_string(field),
            ]),
            Expr::AtTimeZone { expr, time_zone } => parens(RcDoc::concat(vec![
                expr.to_doc(),
                RcDoc::space(),
                RcDoc::as_string("AT TIME ZONE"),
                RcDoc::space(),
                time_zone.to_doc(),
            ])),
            Expr::RawSql(s) => RcDoc::as_string(s),
        }
    }
}

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.to_doc().pretty(usize::MAX))
    }
}

/// A Trino literal value, but only one of the simple types.
///
/// If the `values` feature is enabled for this crate, we will also have a
/// [`crate::Value`] type that can represent a much wider range of values.
///
/// This type is only used for simple values that commonly appear in source
/// code.
#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(any(test, feature = "proptest"), derive(Arbitrary))]
#[non_exhaustive]
pub enum SimpleValue {
    /// A string literal.
    String(String),
    /// An integer literal.
    Int(i64),
    /// A Boolean literal.
    Bool(bool),
    /// A NULL literal.
    Null,
}

impl SimpleValue {
    /// Return a pretty-printed version of `self``.
    pub fn to_doc(&self) -> RcDoc<'static, ()> {
        RcDoc::as_string(self)
    }
}

impl fmt::Display for SimpleValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SimpleValue::String(s) => write!(f, "{}", QuotedString(s)),
            SimpleValue::Int(i) => write!(f, "{}", i),
            SimpleValue::Bool(true) => write!(f, "TRUE"),
            SimpleValue::Bool(false) => write!(f, "FALSE"),
            SimpleValue::Null => write!(f, "NULL"),
        }
    }
}

/// A binary operator in Trino SQL. We only include operators that we actually
/// use.
///
/// TODO: Handle precedence when quoting, once it matters.
#[derive(Clone, Debug, PartialEq)]
pub enum BinOp {
    /// The `=` operator.
    Eq,
}

impl BinOp {
    /// Return a pretty-printed version of `self``.
    fn to_doc(&self) -> RcDoc<'static, ()> {
        RcDoc::as_string(self)
    }
}

impl fmt::Display for BinOp {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            BinOp::Eq => write!(f, "="),
        }
    }
}
