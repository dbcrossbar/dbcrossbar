//! A simple abstract syntax tree for small parts of Trino SQL. We use this to
//! generate import and export queries for Trino.

use std::fmt;

use super::{TrinoDataType, TrinoIdent, TrinoStringLiteral};

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
    /// An `IF` expression.
    If {
        /// The condition.
        cond: Box<Expr>,
        /// The value if the condition is true.
        then: Box<Expr>,
        /// The value if the condition is false.
        r#else: Box<Expr>,
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
        Expr::If {
            cond: Box::new(cond),
            then: Box::new(then),
            r#else: Box::new(r#else),
        }
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
}

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Expr::Lit(lit) => write!(f, "{}", lit),
            Expr::Var(ident) => write!(f, "{}", ident),
            Expr::BinOp { lhs, op, rhs } => write!(f, "{} {} {}", lhs, op, rhs),
            Expr::Func { name, args } => {
                write!(f, "{}(", name)?;
                for (i, arg) in args.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", arg)?;
                }
                write!(f, ")")
            }
            Expr::Cast { expr, ty } => write!(f, "CAST({} AS {})", expr, ty),
            Expr::If { cond, then, r#else } => {
                write!(f, "IF({}, {}, {})", cond, then, r#else)
            }
            Expr::CaseMatch {
                value,
                when_clauses,
                r#else,
            } => {
                write!(f, "CASE {} ", value)?;
                for (when, then) in when_clauses {
                    write!(f, "WHEN {} THEN {} ", when, then)?;
                }
                write!(f, "ELSE {} END", r#else)
            }
            Expr::Lambda { arg, body } => write!(f, "{} -> {}", arg, body),
        }
    }
}

/// A Trino literal value.
///
/// TODO: Merge with literal type used for `WITH` expressions.
#[derive(Clone, Debug, PartialEq)]
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
#[derive(Clone, Debug, PartialEq)]
pub(super) enum BinOp {
    /// The `=` operator.
    Eq,
}

impl fmt::Display for BinOp {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            BinOp::Eq => write!(f, "="),
        }
    }
}
