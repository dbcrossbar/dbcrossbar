//! Downgrading Trino types for storage.

use crate::{
    pretty::ast::{ident, Expr},
    DataType, Field, Ident,
};

/// Downgrades from stanard Trino types (used when running SQL) to "simpler"
/// types that are supported by particular storage backends.
///
/// This is necessary because Trino's storage backends are often much less
/// capable than Trino itself.
#[derive(Clone, Debug)]
pub struct StorageTransform {
    /// The original type.
    original_type: DataType,

    /// The storage type.
    storage_type: DataType,

    /// How to transform this type for storage.
    transform: TypeStorageTransform,
}

impl StorageTransform {
    /// Create a new storage transform.
    pub(crate) fn new(
        original_type: DataType,
        transform: TypeStorageTransform,
    ) -> Self {
        let storage_type = transform.storage_type_for(&original_type);
        Self {
            original_type,
            storage_type,
            transform,
        }
    }

    /// Is this the identity transform?
    pub fn is_identity(&self) -> bool {
        self.transform.is_identity()
    }

    /// The original type, before any transformation.
    pub fn original_type(&self) -> &DataType {
        &self.original_type
    }

    /// What storage type should we use for this type?
    pub fn storage_type(&self) -> &DataType {
        &self.storage_type
    }

    /// Format an SQL fragment, wrapping it code that converts the value to the
    /// storage type. Returns an [`Expr`] that can be pretty-printed.
    pub fn store_expr(&self, wrapped_expr: Expr) -> Expr {
        let transformed_expr = self
            .transform
            .store_transform_expr(wrapped_expr, &self.storage_type);
        let needs_cast = self.transform.requires_cast_on_store();
        if needs_cast {
            Expr::cast(transformed_expr, self.storage_type().to_owned())
        } else {
            transformed_expr
        }
    }

    /// Format an SQL fragment, wrapping it code that converts the value from
    /// the storage type. Returns an [`Expr`] that can be pretty-printed.
    pub fn load_expr(&self, wrapped_expr: Expr) -> Expr {
        let transformed_expr = self
            .transform
            .load_transform_expr(wrapped_expr, &self.original_type);
        let needs_cast = self.transform.requires_cast_on_load();
        if needs_cast {
            Expr::cast(transformed_expr, self.original_type().to_owned())
        } else {
            transformed_expr
        }
    }
}

/// Internal helper for `StorageTransform`.
#[derive(Clone, Debug)]
pub(crate) enum TypeStorageTransform {
    Identity,
    JsonAsVarchar,
    UuidAsVarchar,
    SphericalGeographyAsWkt,
    SmallerIntAsInt,
    TimeAsVarchar,
    TimestampWithTimeZoneAsTimestamp {
        stored_precision: u32,
    },
    TimeWithPrecision {
        stored_precision: u32,
    },
    TimestampWithPrecision {
        stored_precision: u32,
    },
    TimestampWithTimeZoneWithPrecision {
        stored_precision: u32,
    },
    Array {
        element_transform: Box<TypeStorageTransform>,
    },
    Row {
        /// Should we name anonymous fields? (Not all back ends support anonymous
        /// fields.)
        name_anonymous_fields: bool,
        field_transforms: Vec<FieldStorageTransform>,
    },
}

impl TypeStorageTransform {
    /// Is this the identity transform?
    fn is_identity(&self) -> bool {
        matches!(self, TypeStorageTransform::Identity)
    }

    /// Simplify the storage transform by seeing if we can reduce the
    /// top-level transform to `Identity`.
    pub(crate) fn simplify_top_level(self) -> Self {
        match self {
            // We can simplify an `Array` if the element transform simplifies to
            // `Identity`.
            TypeStorageTransform::Array { element_transform }
                if element_transform.is_identity() =>
            {
                TypeStorageTransform::Identity
            }

            // We can't simplify away a `Row` if we need to name fields.
            TypeStorageTransform::Row {
                name_anonymous_fields,
                field_transforms,
            } if !name_anonymous_fields
                && field_transforms.iter().all(|ft| ft.transform.is_identity()) =>
            {
                TypeStorageTransform::Identity
            }

            // Everything else is already simplified.
            other => other,
        }
    }

    /// Return the type used to store the given type.
    pub(crate) fn storage_type_for(&self, ty: &DataType) -> DataType {
        match self {
            TypeStorageTransform::Identity => ty.clone(),
            TypeStorageTransform::JsonAsVarchar => {
                assert!(matches!(*ty, DataType::Json));
                DataType::varchar()
            }
            TypeStorageTransform::UuidAsVarchar => {
                assert!(matches!(*ty, DataType::Uuid));
                DataType::varchar()
            }
            TypeStorageTransform::SphericalGeographyAsWkt => {
                assert!(matches!(*ty, DataType::SphericalGeography));
                DataType::varchar()
            }
            TypeStorageTransform::SmallerIntAsInt => {
                assert!(matches!(*ty, DataType::TinyInt | DataType::SmallInt));
                DataType::Int
            }
            TypeStorageTransform::TimeAsVarchar => {
                assert!(matches!(*ty, DataType::Time { .. }));
                DataType::varchar()
            }
            TypeStorageTransform::TimestampWithTimeZoneAsTimestamp {
                stored_precision,
            } => match ty {
                DataType::TimestampWithTimeZone { .. } => DataType::Timestamp {
                    precision: *stored_precision,
                },
                _ => panic!("expected TimestampWithTimeZone"),
            },
            TypeStorageTransform::TimeWithPrecision { stored_precision } => {
                assert!(matches!(*ty, DataType::Time { .. }));
                DataType::Time {
                    precision: *stored_precision,
                }
            }
            TypeStorageTransform::TimestampWithPrecision { stored_precision } => {
                assert!(matches!(*ty, DataType::Timestamp { .. }));
                DataType::Timestamp {
                    precision: *stored_precision,
                }
            }
            TypeStorageTransform::TimestampWithTimeZoneWithPrecision {
                stored_precision,
            } => {
                assert!(matches!(*ty, DataType::TimestampWithTimeZone { .. }));
                DataType::TimestampWithTimeZone {
                    precision: *stored_precision,
                }
            }
            TypeStorageTransform::Array { element_transform } => match ty {
                DataType::Array(elem_ty) => DataType::Array(Box::new(
                    element_transform.storage_type_for(elem_ty),
                )),
                _ => panic!("expected Array"),
            },
            TypeStorageTransform::Row {
                name_anonymous_fields: name_anoymous_fields,
                field_transforms,
                ..
            } => match ty {
                DataType::Row(fields) => DataType::Row(
                    fields
                        .iter()
                        .zip(field_transforms)
                        .enumerate()
                        .map(|(idx, (field, field_transform))| Field {
                            name: if *name_anoymous_fields {
                                Some(field.name.as_ref().map_or_else(
                                    || Ident::placeholder(idx + 1),
                                    |ident| ident.to_owned(),
                                ))
                            } else {
                                field.name.clone()
                            },
                            data_type: field_transform
                                .transform
                                .storage_type_for(&field.data_type),
                        })
                        .collect(),
                ),
                _ => panic!("expected Row"),
            },
        }
    }

    /// Does this transform require a cast on store? We leave these off when we
    /// can, to make the generated code slightly easier to debug.
    fn requires_cast_on_store(&self) -> bool {
        match self {
            TypeStorageTransform::Identity => false,
            TypeStorageTransform::JsonAsVarchar => false,
            TypeStorageTransform::UuidAsVarchar => true,
            TypeStorageTransform::SphericalGeographyAsWkt => false,
            TypeStorageTransform::SmallerIntAsInt => true,
            TypeStorageTransform::TimeAsVarchar => true,
            TypeStorageTransform::TimestampWithTimeZoneAsTimestamp { .. } => false,
            TypeStorageTransform::TimeWithPrecision { .. } => true,
            TypeStorageTransform::TimestampWithPrecision { .. } => true,
            TypeStorageTransform::TimestampWithTimeZoneWithPrecision { .. } => true,
            TypeStorageTransform::Array { element_transform } => {
                element_transform.requires_cast_on_store()
            }
            // Rows may require casting, even if none of the fields do. For
            // example, if we store a `ROW('', 0)` in a `ROW(VARCHAR(1),
            // SMALLINT)` column, we need to cast the entire row for Hive or the
            // Memory backend. But if only one of those two columns is present,
            // the cast happens implicitly. So, for example, `ROW(VARCHAR,
            // SMALLINT)`, `ROW(VARCHAR(1))` and `ROW(SMALLINT)` all work fine.
            TypeStorageTransform::Row { .. } => true,
        }
    }

    /// Write an expression that transforms the given expression to the storage
    /// type.
    fn store_transform_expr(&self, expr: Expr, storage_type: &DataType) -> Expr {
        match self {
            // These can either be stored as-is, or any conversion they need will
            // be taken care of by the outermost `CAST`.
            TypeStorageTransform::Identity
            | TypeStorageTransform::UuidAsVarchar
            | TypeStorageTransform::TimeAsVarchar
            | TypeStorageTransform::SmallerIntAsInt
            | TypeStorageTransform::TimeWithPrecision { .. }
            | TypeStorageTransform::TimestampWithPrecision { .. }
            | TypeStorageTransform::TimestampWithTimeZoneWithPrecision { .. } => expr,

            TypeStorageTransform::JsonAsVarchar => {
                Expr::func("JSON_FORMAT", vec![expr])
            }

            TypeStorageTransform::SphericalGeographyAsWkt => {
                // After careful consideration and a poll, I've decided to use
                // WKT here:
                //
                // 1. It's the format Trino uses output geography types,
                //    including in error messages and on the wire.
                // 2. Prior to compression, it seems to be about half the size
                //    of GeoJSON.
                Expr::func("ST_AsText", vec![Expr::func("to_geometry", vec![expr])])
            }
            TypeStorageTransform::TimestampWithTimeZoneAsTimestamp {
                stored_precision,
            } => {
                // This is a weird case we don't have in our AST.
                Expr::cast(
                    Expr::at_time_zone(expr, "+00:00"),
                    DataType::Timestamp {
                        precision: *stored_precision,
                    },
                )
            }
            TypeStorageTransform::Array { element_transform } => {
                // We need to use `TRANSFORM` to handle each array element.
                let DataType::Array(elem_storage_type) = storage_type else {
                    panic!("expected DataType::Array")
                };
                let x = ident("x");
                Expr::func(
                    "TRANSFORM",
                    vec![
                        expr,
                        Expr::lambda(
                            x.clone(),
                            element_transform
                                .store_transform_expr(Expr::Var(x), elem_storage_type),
                        ),
                    ],
                )
            }
            TypeStorageTransform::Row {
                name_anonymous_fields: _,
                field_transforms,
            } => {
                let DataType::Row(storage_fields) = storage_type else {
                    panic!("expected DataType::Row")
                };
                let x = ident("x");
                let x_expr = Expr::Var(x.clone());
                Expr::bind_var(
                    x.clone(),
                    expr,
                    Expr::row(
                        storage_type.to_owned(),
                        field_transforms
                            .iter()
                            .zip(storage_fields)
                            .map(|(ft, sf)| {
                                let field_expr = match &ft.name {
                                    FieldName::Named(ident) => {
                                        Expr::field(x_expr.clone(), ident.clone())
                                    }
                                    FieldName::Indexed(idx) => {
                                        // Convert to 1-based index of type i64.
                                        // This should never fail on any
                                        // real-world example.
                                        let idx = i64::try_from(*idx)
                                            .expect("index too large");
                                        Expr::index(x_expr.clone(), Expr::int(idx))
                                    }
                                };
                                ft.transform
                                    .store_transform_expr(field_expr, &sf.data_type)
                            })
                            .collect(),
                    ),
                )
            }
        }
    }

    /// Does this transform require a cast on load? We leave these off when we
    /// can, to make the generated code slightly easier to debug.
    fn requires_cast_on_load(&self) -> bool {
        match self {
            TypeStorageTransform::Identity => false,
            TypeStorageTransform::JsonAsVarchar => false,
            TypeStorageTransform::UuidAsVarchar => false,
            TypeStorageTransform::SphericalGeographyAsWkt => false,
            TypeStorageTransform::SmallerIntAsInt => true,
            TypeStorageTransform::TimeAsVarchar => true,
            TypeStorageTransform::TimestampWithTimeZoneAsTimestamp { .. } => true,
            TypeStorageTransform::TimeWithPrecision { .. } => true,
            TypeStorageTransform::TimestampWithPrecision { .. } => true,
            TypeStorageTransform::TimestampWithTimeZoneWithPrecision { .. } => true,
            TypeStorageTransform::Array { element_transform } => {
                element_transform.requires_cast_on_load()
            }
            TypeStorageTransform::Row {
                name_anonymous_fields,
                field_transforms,
            } => {
                *name_anonymous_fields
                    || field_transforms
                        .iter()
                        .any(|ft| ft.transform.requires_cast_on_load())
            }
        }
    }

    /// Write an expression that transforms the given expression from the storage
    /// type to the standard type.
    fn load_transform_expr(&self, expr: Expr, original_type: &DataType) -> Expr {
        match self {
            TypeStorageTransform::Identity => expr,
            TypeStorageTransform::JsonAsVarchar => {
                Expr::func("JSON_PARSE", vec![expr])
            }
            TypeStorageTransform::UuidAsVarchar => {
                // TODO: Not entirely sure why we still do this cast here. Try without.
                Expr::cast(expr, DataType::Uuid)
            }
            TypeStorageTransform::SphericalGeographyAsWkt => Expr::func(
                "to_spherical_geography",
                vec![Expr::func("ST_GeometryFromText", vec![expr])],
            ),
            TypeStorageTransform::SmallerIntAsInt => expr,
            TypeStorageTransform::TimeAsVarchar => expr,
            TypeStorageTransform::TimestampWithTimeZoneAsTimestamp { .. } => {
                Expr::at_time_zone(expr, "+00:00")
            }
            TypeStorageTransform::TimeWithPrecision { .. } => expr,
            TypeStorageTransform::TimestampWithPrecision { .. } => expr,
            TypeStorageTransform::TimestampWithTimeZoneWithPrecision { .. } => expr,
            TypeStorageTransform::Array { element_transform } => {
                // We need to use `TRANSFORM` to handle each array element.
                let DataType::Array(elem_original_type) = original_type else {
                    panic!("expected DataType::Array")
                };
                let x = ident("x");
                Expr::func(
                    "TRANSFORM",
                    vec![
                        expr,
                        Expr::lambda(
                            x.clone(),
                            element_transform
                                .load_transform_expr(Expr::Var(x), elem_original_type),
                        ),
                    ],
                )
            }
            TypeStorageTransform::Row {
                name_anonymous_fields,
                field_transforms,
            } => {
                let DataType::Row(original_fields) = original_type else {
                    panic!("expected DataType::Row")
                };

                // If all fields are the identity transform, we don't need to do
                // anything here, because our final CAST will handle it.
                if field_transforms.iter().all(|ft| ft.transform.is_identity()) {
                    debug_assert!(name_anonymous_fields);
                    return expr;
                }

                let x = ident("x");
                let x_expr = Expr::Var(x.clone());

                Expr::bind_var(
                    x,
                    expr,
                    Expr::row(
                        original_type.clone(),
                        field_transforms
                            .iter()
                            .zip(original_fields)
                            .map(|(ft, of)| {
                                let field_expr = match &ft.name {
                                    FieldName::Named(ident) => {
                                        Expr::field(x_expr.clone(), ident.clone())
                                    }
                                    FieldName::Indexed(idx) => {
                                        // Convert to 1-based index of type i64.
                                        // This should never fail on any
                                        // real-world example.
                                        let idx = i64::try_from(*idx)
                                            .expect("index too large");
                                        Expr::index(x_expr.clone(), Expr::int(idx))
                                    }
                                };
                                ft.transform
                                    .load_transform_expr(field_expr, &of.data_type)
                            })
                            .collect(),
                    ),
                )
            }
        }
    }
}

/// A field name in a row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum FieldName {
    Named(Ident),
    Indexed(usize),
}

/// A storage transform for a field in a row.
#[derive(Clone, Debug)]
pub(crate) struct FieldStorageTransform {
    pub(crate) name: FieldName,
    pub(crate) transform: TypeStorageTransform,
}

#[cfg(all(test, feature = "client"))]
mod tests {
    use std::str::FromStr as _;

    use chrono::{FixedOffset, NaiveDate, NaiveTime};
    use geo_types::{Coord, Geometry, Point};
    #[cfg(feature = "proptest")]
    use proptest::prelude::*;
    use rust_decimal::Decimal;
    use wkt::TryFromWkt as _;

    use super::*;
    #[cfg(feature = "proptest")]
    use crate::proptest::any_trino_value_with_type;
    use crate::{
        client::Client,
        connectors::ConnectorType,
        pretty::INDENT,
        values::{IsCloseEnoughTo as _, Value},
    };

    /// What's our width for pretty-printing?
    const TEST_WIDTH: usize = 79;

    async fn test_storage_transform_roundtrip_helper(
        test_name: &str,
        connector: ConnectorType,
        value: Value,
        trino_ty: DataType,
        assume_identity_transform_passes: bool,
    ) {
        // How should we transform this type for storage using this connector?
        let storage_transform = connector.storage_transform_for(&trino_ty);

        // Assume the identity transform passes if we're asked to do so.
        if assume_identity_transform_passes && storage_transform.is_identity() {
            return;
        }

        // Create our client.
        let client = Client::default();

        // Get a table name for this test.
        let table_name = connector.test_table_name(test_name);

        // Drop our test table if it exists.
        client
            .run_statement(&format!("DROP TABLE IF EXISTS {}", table_name))
            .await
            .expect("could not drop table");

        // Create a new table with the transformed type.
        let create_table_sql = format!(
            // TODO: This may require things like `WITH (format = 'ORC')`.
            "CREATE TABLE {} (x {})",
            table_name,
            storage_transform.storage_type(),
        );
        eprintln!();
        eprintln!("create_table_sql: {}", create_table_sql);
        client
            .run_statement(&create_table_sql)
            .await
            .expect("could not create table");

        // Insert a value into the table. We use SELECT to avoid hitting
        // https://github.com/trinodb/trino/discussions/16457.
        let insert_sql = format!(
            "INSERT INTO {} SELECT
            {}
            AS x",
            table_name,
            storage_transform
                .store_expr(Expr::raw_sql(&value))
                .to_doc()
                .nest(INDENT)
                .pretty(TEST_WIDTH),
        );
        eprintln!("insert_sql: {}", insert_sql);
        client
            .run_statement(&insert_sql)
            .await
            .expect("could not insert");

        // Read the value back out.
        let select_sql = format!(
            "SELECT
            {}
            FROM {}",
            storage_transform
                .load_expr(Expr::Var(ident("x")))
                .to_doc()
                .nest(INDENT)
                .pretty(TEST_WIDTH),
            table_name
        );
        eprintln!("select_sql: {}", select_sql);
        let loaded_value = client
            .get_one_value::<Value>(&select_sql)
            .await
            .expect("could not select");
        eprintln!("loaded_value: {}", loaded_value);

        if !value.is_close_enough_to(&loaded_value) {
            panic!(
                "Loaded value does not match (type = {}, expected = {}, loaded = {})",
                trino_ty, value, loaded_value
            );
        }

        eprintln!(
            "SUCCESS (type = {}, value = {}, loaded = {})",
            trino_ty, value, loaded_value
        );
    }

    #[tokio::test]
    #[ignore]
    async fn test_storage_transform_roundtrip_manual() {
        use DataType as Ty;
        use Value as Tv;
        let examples = &[
            (Tv::Boolean(true), Ty::Boolean),
            (Tv::TinyInt(i8::MAX), Ty::TinyInt),
            (Tv::SmallInt(i16::MAX), Ty::SmallInt),
            (Tv::Int(i32::MAX), Ty::Int),
            (Tv::BigInt(i64::MAX), Ty::BigInt),
            (Tv::Real(1.0), Ty::Real),
            (Tv::Double(1.0), Ty::Double),
            (
                Tv::Decimal(Decimal::from_str("1.3").unwrap()),
                Ty::Decimal {
                    precision: 6,
                    scale: 2,
                },
            ),
            (
                Tv::Varchar("hello".to_string()),
                Ty::Varchar { length: None },
            ),
            (
                Tv::Varchar("hello".to_string()),
                Ty::Varchar { length: Some(5) },
            ),
            (Tv::Varbinary(vec![0, 1, 2]), Ty::Varbinary),
            (
                Tv::Json(serde_json::from_str(r#"{"a": 1}"#).unwrap()),
                Ty::Json,
            ),
            (
                Tv::Date(NaiveDate::from_ymd_opt(2021, 1, 1).unwrap()),
                Ty::Date,
            ),
            (
                Tv::Time(NaiveTime::from_hms_opt(1, 2, 3).unwrap()),
                Ty::Time { precision: 6 },
            ),
            (
                Tv::Timestamp(
                    NaiveDate::from_ymd_opt(2021, 1, 1)
                        .unwrap()
                        .and_hms_opt(1, 2, 3)
                        .unwrap(),
                ),
                Ty::Timestamp { precision: 6 },
            ),
            (
                Tv::TimestampWithTimeZone(
                    NaiveDate::from_ymd_opt(2021, 1, 1)
                        .unwrap()
                        .and_hms_opt(1, 2, 3)
                        .unwrap()
                        .and_local_timezone(FixedOffset::east_opt(0).unwrap())
                        .single()
                        .unwrap(),
                ),
                Ty::TimestampWithTimeZone { precision: 6 },
            ),
            (
                Tv::Uuid(
                    uuid::Uuid::parse_str("55a05e99-d6ff-49e3-abb8-38c87ccaabb2")
                        .unwrap(),
                ),
                Ty::Uuid,
            ),
            (
                Tv::SphericalGeography(
                    Geometry::<f64>::try_from_wkt_str("POINT(1.0 2.0)")
                        .expect("could not parse WKT"),
                ),
                Ty::SphericalGeography,
            ),
        ];

        for connector in ConnectorType::all_testable() {
            // Try base types.
            for (value, trino_ty) in examples {
                test_storage_transform_roundtrip_helper(
                    "test_storage_transform_roundtrip_manual",
                    connector.to_owned(),
                    value.to_owned(),
                    trino_ty.to_owned(),
                    false,
                )
                .await;
            }

            // Try arrays.
            for (value, trino_ty) in examples {
                let array_ty = Ty::Array(Box::new(trino_ty.to_owned()));
                test_storage_transform_roundtrip_helper(
                    "test_storage_transform_roundtrip_manual",
                    connector.to_owned(),
                    Tv::Array {
                        values: vec![value.to_owned()],
                        literal_type: array_ty.clone(),
                    },
                    array_ty,
                    false,
                )
                .await;
            }

            // Try anonymous single-field rows.
            for (value, trino_ty) in examples {
                let row_ty = Ty::Row(vec![Field {
                    name: None,
                    data_type: trino_ty.to_owned(),
                }]);
                test_storage_transform_roundtrip_helper(
                    "test_storage_transform_roundtrip_manual",
                    connector.to_owned(),
                    Tv::Row {
                        values: vec![value.to_owned()],
                        literal_type: row_ty.clone(),
                    },
                    row_ty,
                    false,
                )
                .await;
            }

            // Try named single-field rows.
            for (value, trino_ty) in examples {
                let row_ty = Ty::Row(vec![Field {
                    name: Some(Ident::new("f").unwrap()),
                    data_type: trino_ty.to_owned(),
                }]);
                test_storage_transform_roundtrip_helper(
                    "test_storage_transform_roundtrip_manual",
                    connector.to_owned(),
                    Tv::Row {
                        values: vec![value.to_owned()],
                        literal_type: row_ty.clone(),
                    },
                    row_ty,
                    false,
                )
                .await;
            }
        }
    }

    #[tokio::test]
    #[ignore]
    async fn test_storage_transform_roundtrip_regressions() {
        use ConnectorType::*;
        use DataType as Ty;
        use Value as Tv;

        // Some regressions we've seen in the past.
        let regressions = &[
            (
                Hive,
                Tv::SphericalGeography(Geometry::Point(Point(Coord {
                    x: 114.85827585275118,
                    y: 0.0,
                }))),
                Ty::SphericalGeography,
            ),
            (
                Hive,
                Tv::Timestamp(
                    NaiveDate::from_ymd_opt(1900, 1, 1)
                        .unwrap()
                        .and_hms_opt(1, 2, 3)
                        .unwrap(),
                ),
                Ty::Timestamp { precision: 6 },
            ),
            {
                // This is a super-weird failure. Almost every part of the code
                // below was required to trigger it. Either field by itself
                // won't. `VARCHAR` with no length constraint won't. `SMALLINT`
                // can be replaced with `TINYINT`, and it will still trigger.
                let lit_type = Ty::Row(vec![
                    Field {
                        name: None,
                        data_type: Ty::Varchar { length: Some(1) },
                    },
                    Field {
                        name: None,
                        data_type: Ty::SmallInt,
                    },
                ]);
                (
                    Hive,
                    Tv::Row {
                        values: vec![
                            // So weird.
                            Tv::Varchar("".to_string()),
                            Tv::SmallInt(0),
                        ],
                        literal_type: lit_type.clone(),
                    },
                    lit_type,
                )
            },
        ];

        for (connector, value, trino_ty) in regressions {
            test_storage_transform_roundtrip_helper(
                "test_storage_transform_roundtrip_regressions",
                connector.to_owned(),
                value.to_owned(),
                trino_ty.to_owned(),
                false,
            )
            .await;
        }
    }

    #[cfg(feature = "proptest")]
    proptest! {
        #[test]
        #[ignore]
        fn test_storage_transform_roundtrip_generated(
            connector in any::<ConnectorType>(),
            (value, trino_ty) in any_trino_value_with_type(),
        ) {
            // We can't use `proptest` with an async function, but we can create
            // a future and run it synchronously using Tokio.
            //
            // We assume that any identity transform passes here, because we
            // already test simple versions of those in
            // `test_storage_transform_roundtrip_manual`.
            let fut = test_storage_transform_roundtrip_helper(
                "test_storage_transform_roundtrip_generated",
                connector, value, trino_ty, true
            );
            tokio::runtime::Runtime::new().unwrap().block_on(fut);
        }
    }
}
