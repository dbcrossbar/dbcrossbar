//! A simple abstract syntax tree for small parts of Trino SQL. We use this to
//! generate import and export queries for Trino.

use std::fmt::{self, Debug};

use pretty::RcDoc;
#[cfg(test)]
use proptest_derive::Arbitrary;

use super::{
    pretty::{comma_sep_list, parens, square_brackets, INDENT},
    TrinoDataType, TrinoIdent, TrinoStringLiteral,
};

/// Construct a static identifier known at compile time. Must not be the empty
/// string.
pub(super) fn ident(s: &'static str) -> TrinoIdent {
    // The `unwrap` here should be safe because `TrinoIdent::new` only fails on
    // empty identifiers, and we're only called internally with string literals
    // supplied by the programmer.
    TrinoIdent::new(s).unwrap()
}

/// An expression in Trino SQL.
#[derive(Clone, Debug, PartialEq)]
pub(super) enum Expr {
    /// A literal value.
    Lit(Literal),
    /// A variable reference.
    Var(TrinoIdent),
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
        ty: TrinoDataType,
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
        arg: TrinoIdent,
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
}

impl Expr {
    /// A string literal.
    pub(super) fn str<S: Into<String>>(s: S) -> Expr {
        Expr::Lit(Literal::String(s.into()))
    }

    /// An integer literal.
    pub(super) fn int(i: i64) -> Expr {
        Expr::Lit(Literal::Int(i))
    }

    /// A Boolean literal.
    pub(super) fn bool(b: bool) -> Expr {
        Expr::Lit(Literal::Bool(b))
    }

    /// A NULL literal.
    pub(super) fn null() -> Expr {
        Expr::Lit(Literal::Null)
    }

    /// A binary operation.
    pub(super) fn binop(lhs: Expr, op: BinOp, rhs: Expr) -> Expr {
        Expr::BinOp {
            lhs: Box::new(lhs),
            op,
            rhs: Box::new(rhs),
        }
    }

    /// A function call.
    pub(super) fn func(name: &'static str, args: Vec<Expr>) -> Expr {
        Expr::Func { name, args }
    }

    /// A cast.
    pub(super) fn cast(expr: Expr, ty: TrinoDataType) -> Expr {
        Expr::Cast {
            expr: Box::new(expr),
            ty,
        }
    }

    /// An `IF` expression.
    pub(super) fn r#if(cond: Expr, then: Expr, r#else: Expr) -> Expr {
        Expr::func("IF", vec![cond, then, r#else])
    }

    /// A `CASE` expression (match version).
    pub(super) fn case_match(
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
    pub(super) fn lambda(arg: TrinoIdent, body: Expr) -> Expr {
        Expr::Lambda {
            arg,
            body: Box::new(body),
        }
    }

    /// Serialize as JSON.
    pub(super) fn json_to_string(expr: Expr) -> Expr {
        Expr::func("JSON_FORMAT", vec![expr])
    }

    // Cast to JSON, then serialize.
    pub(super) fn json_to_string_with_cast(expr: Expr) -> Expr {
        Self::json_to_string(Self::cast(expr, TrinoDataType::Json))
    }

    /// An `ARRAY` expression.
    pub(super) fn array(exprs: Vec<Expr>) -> Expr {
        Expr::Array(exprs)
    }

    /// An array or row element access.
    pub(super) fn index(expr: Expr, index: Expr) -> Expr {
        Expr::Index {
            expr: Box::new(expr),
            index: Box::new(index),
        }
    }

    /// Bind a variable in a lambda. We fake this using `ARRAY` and `TRANSFORM`,
    /// because Trino doesn't seem to have any better way to do this.
    pub(super) fn bind_var(var: TrinoIdent, expr: Expr, body: Expr) -> Expr {
        Expr::index(
            Expr::func(
                "TRANSFORM",
                vec![Expr::array(vec![expr]), Expr::lambda(var, body)],
            ),
            Expr::int(1),
        )
    }

    /// A `ROW` expression with a `CAST`
    pub(super) fn row(ty: TrinoDataType, exprs: Vec<Expr>) -> Expr {
        Expr::cast(Expr::func("ROW", exprs), ty)
    }
}

impl Expr {
    /// Return a pretty-printed version of `self``.
    pub(super) fn to_doc(&self) -> RcDoc<'static, ()> {
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
        }
    }
}

/// A Trino literal value.
#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(test, derive(Arbitrary))]
pub(super) enum Literal {
    /// A string literal.
    String(String),
    /// An integer literal.
    Int(i64),
    /// A Boolean literal.
    Bool(bool),
    /// A NULL literal.
    Null,
}

impl Literal {
    /// Return a pretty-printed version of `self``.
    pub(super) fn to_doc(&self) -> RcDoc<'static, ()> {
        RcDoc::as_string(self)
    }
}

impl fmt::Display for Literal {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Literal::String(s) => write!(f, "{}", TrinoStringLiteral(s)),
            Literal::Int(i) => write!(f, "{}", i),
            Literal::Bool(true) => write!(f, "TRUE"),
            Literal::Bool(false) => write!(f, "FALSE"),
            Literal::Null => write!(f, "NULL"),
        }
    }
}

/// A binary operator in Trino SQL. We only include operators that we actually
/// use.
///
/// TODO: Handle precedence when quoting, once it matters.
#[derive(Clone, Debug, PartialEq)]
pub(super) enum BinOp {
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
