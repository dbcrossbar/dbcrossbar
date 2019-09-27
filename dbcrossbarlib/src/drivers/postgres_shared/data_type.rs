//! PostgreSQL data types.

use std::fmt;

use crate::common::*;
use crate::schema::{DataType, Srid};

/// A native PostgreSQL data type.
///
/// This is obviously simplified, but feel free to "unsimplify" it by adding
/// any other useful types or details of types.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum PgDataType {
    /// An array type.
    Array {
        /// The number of dimensions of this array.
        dimension_count: i32,
        /// The type of the array's elements..
        ty: PgScalarDataType,
    },
    /// A simple, non-array data type.
    Scalar(PgScalarDataType),
}

impl PgDataType {
    /// Given a `DataType`, try to find a corresponding `PgDataType`.
    pub(crate) fn from_data_type(ty: &DataType) -> Result<PgDataType> {
        match ty {
            DataType::Array(nested) => {
                // Iterate over our nested child array types, figuring out how
                // many array dimensions we have before we hit a scalar type.
                let mut dimension_count = 1;
                let mut nested = nested.as_ref();
                while let DataType::Array(next) = nested {
                    dimension_count += 1;
                    nested = next.as_ref();
                }
                Ok(PgDataType::Array {
                    dimension_count,
                    ty: PgScalarDataType::from_data_type(nested)?,
                })
            }
            scalar => Ok(PgDataType::Scalar(PgScalarDataType::from_data_type(
                scalar,
            )?)),
        }
    }

    /// Convert this `PgDataType` to a portable `DataType`.
    pub(crate) fn to_data_type(&self) -> Result<DataType> {
        match self {
            PgDataType::Array {
                dimension_count,
                ty,
            } => {
                let mut built = ty.to_data_type()?;
                for _ in 0..(*dimension_count) {
                    built = DataType::Array(Box::new(built));
                }
                Ok(built)
            }
            PgDataType::Scalar(ty) => ty.to_data_type(),
        }
    }
}

#[test]
fn nested_array_conversions() {
    let original_ty =
        DataType::Array(Box::new(DataType::Array(Box::new(DataType::Int32))));
    let pg_ty = PgDataType::from_data_type(&original_ty).unwrap();
    assert_eq!(
        pg_ty,
        PgDataType::Array {
            dimension_count: 2,
            ty: PgScalarDataType::Int,
        },
    );
    let portable_ty = pg_ty.to_data_type().unwrap();
    assert_eq!(portable_ty, original_ty);
}

#[test]
fn scalar_conversions() {
    let original_ty = DataType::Int32;
    let pg_ty = PgDataType::from_data_type(&original_ty).unwrap();
    assert_eq!(pg_ty, PgDataType::Scalar(PgScalarDataType::Int));
    let portable_ty = pg_ty.to_data_type().unwrap();
    assert_eq!(portable_ty, original_ty);
}

impl fmt::Display for PgDataType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            PgDataType::Array {
                dimension_count,
                ty,
            } => {
                ty.fmt(f)?;
                for _ in 0..*dimension_count {
                    write!(f, "[]")?;
                }
                Ok(())
            }
            PgDataType::Scalar(ty) => ty.fmt(f),
        }
    }
}

/// A non-array PostgreSQL data type.
///
/// As with `PgDataType`, feel free to add any details you need here.
#[derive(Clone, Debug, Eq, PartialEq)]
#[allow(missing_docs)]
pub(crate) enum PgScalarDataType {
    Boolean,
    Date,
    Numeric,
    Real,
    DoublePrecision,
    Geometry(Srid),
    Smallint,
    Int,
    Bigint,
    Json,
    Jsonb,
    Text,
    TimestampWithoutTimeZone,
    TimestampWithTimeZone,
    Uuid,
}

impl PgScalarDataType {
    /// Given a `DataType`, try to find a corresponding `PgScalarDataType`.
    /// Panics if called with a non-scalar type.
    fn from_data_type(ty: &DataType) -> Result<PgScalarDataType> {
        match ty {
            DataType::Array(_) => {
                unreachable!("should have been handled by PgDataType::from_data_type")
            }
            DataType::Bool => Ok(PgScalarDataType::Boolean),
            DataType::Date => Ok(PgScalarDataType::Date),
            DataType::Decimal => Ok(PgScalarDataType::Numeric),
            DataType::Float32 => Ok(PgScalarDataType::Real),
            DataType::Float64 => Ok(PgScalarDataType::DoublePrecision),
            DataType::GeoJson(srid) => Ok(PgScalarDataType::Geometry(*srid)),
            DataType::Int16 => Ok(PgScalarDataType::Smallint),
            DataType::Int32 => Ok(PgScalarDataType::Int),
            DataType::Int64 => Ok(PgScalarDataType::Bigint),
            DataType::Json => Ok(PgScalarDataType::Jsonb),
            DataType::OldPgJson => Ok(PgScalarDataType::Json),
            DataType::Other(_) => Ok(PgScalarDataType::Text),
            DataType::Text => Ok(PgScalarDataType::Text),
            DataType::TimestampWithoutTimeZone => {
                Ok(PgScalarDataType::TimestampWithoutTimeZone)
            }
            DataType::TimestampWithTimeZone => {
                Ok(PgScalarDataType::TimestampWithTimeZone)
            }
            DataType::Uuid => Ok(PgScalarDataType::Uuid),
        }
    }

    /// Convert this `PgDataType` to a portable `DataType`.
    pub(crate) fn to_data_type(&self) -> Result<DataType> {
        match self {
            PgScalarDataType::Boolean => Ok(DataType::Bool),
            PgScalarDataType::Date => Ok(DataType::Date),
            PgScalarDataType::Numeric => Ok(DataType::Decimal),
            PgScalarDataType::Real => Ok(DataType::Float32),
            PgScalarDataType::DoublePrecision => Ok(DataType::Float64),
            PgScalarDataType::Geometry(srid) => Ok(DataType::GeoJson(*srid)),
            PgScalarDataType::Smallint => Ok(DataType::Int16),
            PgScalarDataType::Int => Ok(DataType::Int32),
            PgScalarDataType::Bigint => Ok(DataType::Int64),
            PgScalarDataType::Json => Ok(DataType::OldPgJson),
            PgScalarDataType::Jsonb => Ok(DataType::Json),
            PgScalarDataType::Text => Ok(DataType::Text),
            PgScalarDataType::TimestampWithoutTimeZone => {
                Ok(DataType::TimestampWithoutTimeZone)
            }
            PgScalarDataType::TimestampWithTimeZone => {
                Ok(DataType::TimestampWithTimeZone)
            }
            PgScalarDataType::Uuid => Ok(DataType::Uuid),
        }
    }

    /// See [this list of types and OIDs][types].
    ///
    /// [types]: https://github.com/postgres/postgres/blob/master/src/include/catalog/pg_type.dat
    pub(crate) fn oid(&self) -> Result<i32> {
        match self {
            PgScalarDataType::Boolean => Ok(16),
            PgScalarDataType::Date => Ok(1082),
            PgScalarDataType::Numeric => Ok(1700),
            PgScalarDataType::Real => Ok(700),
            PgScalarDataType::DoublePrecision => Ok(701),
            PgScalarDataType::Geometry(_) => Err(format_err!(
                "don't know the PostgreSQL OID for type `geometry`"
            )),
            PgScalarDataType::Smallint => Ok(21),
            PgScalarDataType::Int => Ok(23),
            PgScalarDataType::Bigint => Ok(20),
            PgScalarDataType::Json => Ok(114),
            PgScalarDataType::Jsonb => Ok(3802),
            PgScalarDataType::Text => Ok(25),
            PgScalarDataType::TimestampWithoutTimeZone => Ok(1114),
            PgScalarDataType::TimestampWithTimeZone => Ok(1184),
            PgScalarDataType::Uuid => Ok(2950),
        }
    }
}

impl fmt::Display for PgScalarDataType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            PgScalarDataType::Boolean => write!(f, "boolean")?,
            PgScalarDataType::Date => write!(f, "date")?,
            PgScalarDataType::Numeric => write!(f, "numeric")?,
            PgScalarDataType::Real => write!(f, "real")?,
            PgScalarDataType::DoublePrecision => write!(f, "double precision")?,
            PgScalarDataType::Geometry(srid) => {
                write!(f, "public.geometry(Geometry, {})", srid)?
            }
            PgScalarDataType::Smallint => write!(f, "smallint")?,
            PgScalarDataType::Int => write!(f, "int")?,
            PgScalarDataType::Bigint => write!(f, "bigint")?,
            PgScalarDataType::Json => write!(f, "json")?,
            PgScalarDataType::Jsonb => write!(f, "jsonb")?,
            PgScalarDataType::Text => write!(f, "text")?,
            PgScalarDataType::TimestampWithoutTimeZone => {
                write!(f, "timestamp without time zone")?
            }
            PgScalarDataType::TimestampWithTimeZone => {
                write!(f, "timestamp with time zone")?
            }
            PgScalarDataType::Uuid => write!(f, "uuid")?,
        }
        Ok(())
    }
}
