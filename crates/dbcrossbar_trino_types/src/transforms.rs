//! Downgrading Trino types for storage.

use std::fmt;

use crate::{TrinoDataType, TrinoField, TrinoIdent};

/// Downgrades from stanard Trino types (used when running SQL) to "simpler"
/// types that are supported by particular storage backends.
///
/// This is necessary because Trino's storage backends are often much less
/// capable than Trino itself.
pub enum StorageTransform {
    Identity,
    JsonAsVarchar,
    UuidAsVarchar,
    SphericalGeographyAsVarchar,
    SmallerIntAsInt,
    TimeAsVarchar,
    TimestampWithTimeZoneAsTimezone {
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
        element_transform: Box<StorageTransform>,
    },
    Row {
        /// Should we name anonymous fields? (Not all back ends support anonymous
        /// fields.)
        name_anonymous_fields: bool,
        field_transforms: Vec<FieldTypeStorageTransform>,
        original_type: TrinoDataType,
    },
}

impl StorageTransform {
    /// Simplify the storage transform by reducing anything that's just an
    /// identity transform.
    ///
    /// TODO: Assume recursive simplification has already been done?
    pub(crate) fn simplify(self) -> Self {
        match self {
            // We can simplify an `Array` if the element transform simplifies to
            // `Identity`.
            StorageTransform::Array { element_transform } => {
                match element_transform.simplify() {
                    StorageTransform::Identity => StorageTransform::Identity,
                    element_transform => StorageTransform::Array {
                        element_transform: Box::new(element_transform),
                    },
                }
            }

            // We can't simplify away a `Row` if we need to name fields.
            StorageTransform::Row {
                name_anonymous_fields: true,
                field_transforms,
                original_type,
            } => StorageTransform::Row {
                name_anonymous_fields: true,
                field_transforms: field_transforms
                    .into_iter()
                    .map(|field_transform| field_transform.simplify())
                    .collect(),
                original_type,
            },

            // We can simplify away a `Row` if we don't need to name fields, and if
            // all the field transforms simplify to `Identity`.
            StorageTransform::Row {
                name_anonymous_fields: false,
                field_transforms,
                original_type,
            } => {
                let simplified_field_transforms = field_transforms
                    .into_iter()
                    .map(|field_transform| field_transform.simplify())
                    .collect::<Vec<_>>();
                if simplified_field_transforms
                    .iter()
                    .all(|ft| matches!(ft.transform, StorageTransform::Identity))
                {
                    StorageTransform::Identity
                } else {
                    StorageTransform::Row {
                        name_anonymous_fields: false,
                        field_transforms: simplified_field_transforms,
                        original_type,
                    }
                }
            }

            // Everything else is already simplified.
            other => other,
        }
    }

    /// Return the type used to store the given type.
    pub(crate) fn storage_type_for(&self, ty: &TrinoDataType) -> TrinoDataType {
        match self {
            StorageTransform::Identity => ty.clone(),
            StorageTransform::JsonAsVarchar => {
                assert!(matches!(*ty, TrinoDataType::Json));
                TrinoDataType::varchar()
            }
            StorageTransform::UuidAsVarchar => {
                assert!(matches!(*ty, TrinoDataType::Uuid));
                TrinoDataType::varchar()
            }
            StorageTransform::SphericalGeographyAsVarchar => {
                assert!(matches!(*ty, TrinoDataType::SphericalGeography));
                TrinoDataType::varchar()
            }
            StorageTransform::SmallerIntAsInt => {
                assert!(matches!(
                    *ty,
                    TrinoDataType::TinyInt | TrinoDataType::SmallInt
                ));
                TrinoDataType::Int
            }
            StorageTransform::TimeAsVarchar => {
                assert!(matches!(*ty, TrinoDataType::Time { .. }));
                TrinoDataType::varchar()
            }
            StorageTransform::TimestampWithTimeZoneAsTimezone { stored_precision } => {
                match ty {
                    TrinoDataType::TimestampWithTimeZone { .. } => {
                        TrinoDataType::Timestamp {
                            precision: *stored_precision,
                        }
                    }
                    _ => panic!("expected TimestampWithTimeZone"),
                }
            }
            StorageTransform::TimeWithPrecision { stored_precision } => {
                assert!(matches!(*ty, TrinoDataType::Time { .. }));
                TrinoDataType::Time {
                    precision: *stored_precision,
                }
            }
            StorageTransform::TimestampWithPrecision { stored_precision } => {
                assert!(matches!(*ty, TrinoDataType::Timestamp { .. }));
                TrinoDataType::Timestamp {
                    precision: *stored_precision,
                }
            }
            StorageTransform::TimestampWithTimeZoneWithPrecision {
                stored_precision,
            } => {
                assert!(matches!(*ty, TrinoDataType::TimestampWithTimeZone { .. }));
                TrinoDataType::TimestampWithTimeZone {
                    precision: *stored_precision,
                }
            }
            StorageTransform::Array { element_transform } => match ty {
                TrinoDataType::Array(elem_ty) => TrinoDataType::Array(Box::new(
                    element_transform.storage_type_for(elem_ty),
                )),
                _ => panic!("expected Array"),
            },
            StorageTransform::Row {
                name_anonymous_fields: name_anoymous_fields,
                field_transforms,
                ..
            } => match ty {
                TrinoDataType::Row(fields) => TrinoDataType::Row(
                    fields
                        .iter()
                        .zip(field_transforms)
                        .enumerate()
                        .map(|(idx, (field, field_transform))| TrinoField {
                            name: if *name_anoymous_fields {
                                Some(field.name.as_ref().map_or_else(
                                    || TrinoIdent::placeholder(idx + 1),
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

    /// Write an expression that transforms the given expression to the storage
    /// type.
    fn fmt_store_transform_expr(
        &self,
        f: &mut dyn fmt::Write,
        expr: &dyn fmt::Display,
    ) -> std::fmt::Result {
        match self {
            StorageTransform::Identity => write!(f, "{}", expr),

            StorageTransform::UuidAsVarchar | StorageTransform::TimeAsVarchar => {
                write!(f, "CAST({} AS VARCHAR)", expr)
            }

            StorageTransform::JsonAsVarchar => {
                write!(f, "JSON_FORMAT({})", expr)
            }

            StorageTransform::SphericalGeographyAsVarchar => {
                // TODO: GeoJSON or WKT?
                write!(f, "TO_GEOJSON_GEOMETRY({})", expr)
            }
            StorageTransform::SmallerIntAsInt => {
                write!(f, "CAST({} AS INT)", expr)
            }
            StorageTransform::TimestampWithTimeZoneAsTimezone { stored_precision } => {
                write!(
                    f,
                    "CAST(({} AT TIME ZONE '+00:00') AS TIMESTAMP({}))",
                    expr, stored_precision
                )
            }
            StorageTransform::TimeWithPrecision { stored_precision } => {
                write!(f, "CAST({} AS TIME({}))", expr, stored_precision)
            }
            StorageTransform::TimestampWithPrecision { stored_precision } => {
                write!(f, "CAST({} AS TIMESTAMP({}))", expr, stored_precision)
            }
            StorageTransform::TimestampWithTimeZoneWithPrecision {
                stored_precision,
            } => {
                write!(
                    f,
                    "CAST({} AS TIMESTAMP({}) WITH TIME ZONE)",
                    expr, stored_precision
                )
            }
            StorageTransform::Array { element_transform } => {
                // We need to use `TRANSFORM` to handle each array element.
                write!(
                    f,
                    "TRANSFORM({}, x -> {})",
                    expr,
                    StoreTransformExpr(element_transform, &"x")
                )
            }
            StorageTransform::Row {
                field_transforms,
                original_type,
                ..
            } => {
                // This is a bit of a trick. We only want to evaluate `expr`
                // once, but we can't bind local variables. So we construct a
                // one element array, map over it, and then take the first
                // element.
                write!(f, "TRANSFORM(ARRAY[{}], x -> ROW(", expr)?;
                for (idx, ft) in field_transforms.iter().enumerate() {
                    if idx > 0 {
                        write!(f, ", ")?;
                    }
                    let field_expr = match &ft.name {
                        FieldName::Named(ident) => format!("x.{}", ident),
                        FieldName::Indexed(idx) => format!("x[{}]", idx),
                    };
                    write!(f, "{}", StoreTransformExpr(&ft.transform, &field_expr))?;
                }
                write!(f, "))[1]")
            }
        }
    }

    /// Write an expression that transforms the given expression from the storage
    /// type to the standard type.
    fn fmt_load_transform_expr(
        &self,
        f: &mut dyn fmt::Write,
        expr: &dyn fmt::Display,
    ) -> std::fmt::Result {
        match self {
            StorageTransform::Identity => write!(f, "{}", expr),
            StorageTransform::JsonAsVarchar => {
                write!(f, "JSON_PARSE({})", expr)
            }
            StorageTransform::UuidAsVarchar => {
                write!(f, "CAST({} AS UUID)", expr)
            }
            StorageTransform::SphericalGeographyAsVarchar => {
                write!(f, "FROM_GEOJSON_GEOMETRY({})", expr)
            }
            StorageTransform::SmallerIntAsInt => write!(f, "{}", expr),
            StorageTransform::TimeAsVarchar => write!(f, "CAST({} AS TIME)", expr),
            StorageTransform::TimestampWithTimeZoneAsTimezone { .. } => {
                write!(f, "({} AT TIME ZONE '+00:00')", expr)
            }
            StorageTransform::TimeWithPrecision { .. } => {
                write!(f, "{}", expr)
            }
            StorageTransform::TimestampWithPrecision { .. } => {
                write!(f, "{}", expr)
            }
            StorageTransform::TimestampWithTimeZoneWithPrecision { .. } => {
                write!(f, "{}", expr)
            }
            StorageTransform::Array { element_transform } => {
                // We need to use `TRANSFORM` to handle each array element.
                write!(
                    f,
                    "TRANSFORM({}, x -> {})",
                    expr,
                    LoadTransformExpr(element_transform, &"x")
                )
            }
            StorageTransform::Row {
                name_anonymous_fields,
                field_transforms,
                original_type,
            } => {
                // This is a bit of a trick. We only want to evaluate `expr`
                // once, but we can't bind local variables. So we construct a
                // one element array, map over it, and then take the first
                // element.
                write!(f, "CAST(TRANSFORM(ARRAY[{}], x -> ROW(", expr)?;
                for (idx, ft) in field_transforms.iter().enumerate() {
                    if idx > 0 {
                        write!(f, ", ")?;
                    }
                    let field_expr = match &ft.name {
                        FieldName::Named(ident) => format!("x.{}", ident),
                        FieldName::Indexed(idx) => format!("x[{}]", idx),
                    };
                    write!(f, "{}", LoadTransformExpr(&ft.transform, &field_expr))?;
                }
                write!(f, "))[1] AS {})", original_type)
            }
        }
    }
}

/// A field name in a row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FieldName {
    Named(TrinoIdent),
    Indexed(usize),
}

/// Transform a field for storage.
pub struct FieldTypeStorageTransform {
    pub name: FieldName,
    pub transform: StorageTransform,
}

impl FieldTypeStorageTransform {
    /// Simplify the storage transform by reducing anything that's just an
    /// identity transform.
    pub(crate) fn simplify(self) -> Self {
        FieldTypeStorageTransform {
            name: self.name,
            transform: self.transform.simplify(),
        }
    }
}

/// Format a store operation with any necessary transform.
pub struct StoreTransformExpr<'a, D: fmt::Display>(&'a StorageTransform, &'a D);

impl<'a, D: fmt::Display> std::fmt::Display for StoreTransformExpr<'a, D> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.0.fmt_store_transform_expr(f, self.1)
    }
}

/// Format a load operation with any necessary transform.
pub struct LoadTransformExpr<'a, D: fmt::Display>(&'a StorageTransform, &'a D);

impl<'a, D: fmt::Display> std::fmt::Display for LoadTransformExpr<'a, D> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.0.fmt_load_transform_expr(f, self.1)
    }
}

#[cfg(test)]
mod tests {
    use chrono::{FixedOffset, NaiveDate, NaiveTime};
    use proptest::prelude::*;

    use super::*;
    use crate::{
        connectors::TrinoConnectorType,
        test::{any_trino_value_with_type, ApproxEqToJson, Client, TrinoValue},
    };

    async fn test_storage_transform_roundtrip_helper(
        connector: TrinoConnectorType,
        value: TrinoValue,
        trino_ty: TrinoDataType,
    ) {
        // How should we transform this type for storage using this connector?
        let storage_transform = connector.storage_transform_for(&trino_ty);

        // Create our client.
        let client = Client::default();

        // We need unique table names for each connector, becauae some
        // of them are actually implemented on top of others and share a
        // table namespace. For example, `hive.default.x` and `iceberg.default.x`
        // would conflict.
        let table_name = format!(
            "{}.{}.test_storage_transform_roundtrip_{}",
            connector.test_catalog(),
            connector.test_schema(),
            connector
        );

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
            storage_transform.storage_type_for(&trino_ty),
        );
        eprintln!();
        eprintln!("create_table_sql: {}", create_table_sql);
        client
            .run_statement(&create_table_sql)
            .await
            .expect("could not create table");

        // Insert a value into the table. We use SELECT to hitting
        // https://github.com/trinodb/trino/discussions/16457.
        let insert_sql = format!(
            "INSERT INTO {} SELECT {} AS x",
            table_name,
            StoreTransformExpr(&storage_transform, &value)
        );
        eprintln!("insert_sql: {}", insert_sql);
        client
            .run_statement(&insert_sql)
            .await
            .expect("could not insert");

        // Read the value back out.
        let select_sql = format!(
            "SELECT {} FROM {}",
            LoadTransformExpr(&storage_transform, &"x"),
            table_name
        );
        eprintln!("select_sql: {}", select_sql);
        let loaded_value =
            client.get_one(&select_sql).await.expect("could not select");
        eprintln!("loaded_value: {:?}", loaded_value);

        if !value.approx_eq_to_json(&loaded_value) {
            panic!(
                "Loaded value does not match (type = {}, value = {}, loaded = {})",
                trino_ty, value, loaded_value
            );
        }

        eprintln!(
            "SUCCESS (type = {}, value = {}, loaded = {})",
            trino_ty, value, loaded_value
        );
    }

    #[tokio::test]
    async fn test_storage_transform_roundtrip_manual() {
        use TrinoConnectorType::*;
        use TrinoDataType as Ty;
        use TrinoValue as Tv;
        let connectors = &[Hive, Iceberg, Memory];
        let examples = &[
            (Tv::Boolean(true), Ty::Boolean),
            (Tv::TinyInt(i8::MAX), Ty::TinyInt),
            (Tv::SmallInt(i16::MAX), Ty::SmallInt),
            (Tv::Int(i32::MAX), Ty::Int),
            (Tv::BigInt(i64::MAX), Ty::BigInt),
            (Tv::Real(1.0), Ty::Real),
            (Tv::Double(1.0), Ty::Double),
            (
                Tv::Decimal("1.3".to_string()),
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
                    serde_json::from_str(
                        r#"{"type": "Point", "coordinates": [1.0, 2.0]}"#,
                    )
                    .unwrap(),
                ),
                Ty::SphericalGeography,
            ),
        ];

        for connector in connectors {
            // Try base types.
            for (value, trino_ty) in examples {
                test_storage_transform_roundtrip_helper(
                    connector.to_owned(),
                    value.to_owned(),
                    trino_ty.to_owned(),
                )
                .await;
            }

            // Try arrays.
            for (value, trino_ty) in examples {
                let array_ty = Ty::Array(Box::new(trino_ty.to_owned()));
                test_storage_transform_roundtrip_helper(
                    connector.to_owned(),
                    Tv::Array {
                        values: vec![value.to_owned()],
                        lit_type: array_ty.clone(),
                    },
                    array_ty,
                )
                .await;
            }

            // Try anonymous single-field rows.
            for (value, trino_ty) in examples {
                let row_ty = Ty::Row(vec![TrinoField {
                    name: None,
                    data_type: trino_ty.to_owned(),
                }]);
                test_storage_transform_roundtrip_helper(
                    connector.to_owned(),
                    Tv::Row {
                        values: vec![value.to_owned()],
                        lit_type: row_ty.clone(),
                    },
                    row_ty,
                )
                .await;
            }

            // Try named single-field rows.
            for (value, trino_ty) in examples {
                let row_ty = Ty::Row(vec![TrinoField {
                    name: Some(TrinoIdent::new("f").unwrap()),
                    data_type: trino_ty.to_owned(),
                }]);
                test_storage_transform_roundtrip_helper(
                    connector.to_owned(),
                    Tv::Row {
                        values: vec![value.to_owned()],
                        lit_type: row_ty.clone(),
                    },
                    row_ty,
                )
                .await;
            }
        }
    }

    proptest! {
       #[test]
        fn test_storage_transform_roundtrip_generated(
            connector in any::<TrinoConnectorType>(),
            (value, trino_ty) in any_trino_value_with_type(),
        ) {
            // We can't use `proptest` with an async function, but we can create
            // a future and run it synchronously using Tokio.
            let fut = test_storage_transform_roundtrip_helper(connector, value, trino_ty);
            tokio::runtime::Runtime::new().unwrap().block_on(fut);
        }
    }
}
// Agg<T>
// ELEM = Stored<ARRAY<T>> -> Stored<T>
