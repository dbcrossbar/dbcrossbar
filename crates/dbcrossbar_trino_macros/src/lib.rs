use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput};

/// Allow a struct type to be used with Trino.
///
/// ### Example
///
/// ```no_compile
/// use dbcrossbar_trino::TrinoRow;
///
/// #[derive(TrinoRow)]
/// struct MyRow {
///    a: i32,
///    b: String,
/// }
/// ```
///
/// This will actually define several `impl`s.
#[proc_macro_derive(TrinoRow)]
pub fn derive_trino_row(item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as DeriveInput);
    // TODO: Handle generics.
    let struct_name = &input.ident;

    let expanded = match input.data {
        syn::Data::Struct(data_struct) => {
            let fields = &data_struct.fields;
            let field_count = fields.len();

            let expected_data_type_fields = fields.iter().map(|f| {
                let name = if let Some(ident) = &f.ident {
                    quote! { Some(::dbcrossbar_trino::Ident::new(stringify!(#ident)).unwrap()) }
                } else {
                    quote! { None }
                };
                let ty = &f.ty;
                quote! {
                    ::dbcrossbar_trino::values::FieldWithDataTypeOrAny {
                        name: #name,
                        data_type: <#ty as ::dbcrossbar_trino::values::ExpectedDataType>::expected_data_type(),
                    },
                }
            });

            // Expressions to extract values from each field in our `Value::Row`.
            let field_try_from_exprs = fields
                .iter()
                .enumerate()
                .map(|(idx, _)| {
                    quote! { values[#idx].clone().try_into()? }
                })
                .collect::<Vec<_>>();

            // Construct a struct using `field_try_from_exprs`, which varies
            // depending on the struct's type (regular, tuple, etc).
            let construct_struct = match &data_struct.fields {
                syn::Fields::Named(_) => {
                    // TODO: Generate a check that our fields are in the right
                    // order when compared to the Value we're converting from.
                    let field_inits = fields
                        .iter()
                        .zip(field_try_from_exprs.iter())
                        .map(|(f, e)| {
                            let name = f.ident.as_ref().unwrap();
                            quote! { #name: #e, }
                        });
                    quote! { #struct_name { #(#field_inits)* } }
                }
                syn::Fields::Unnamed(_) => {
                    quote! { #struct_name(#(#field_try_from_exprs),*) }
                }
                // I'm pretty sure this isn't useful.
                syn::Fields::Unit => quote! { #struct_name },
            };

            quote! {
                impl ::dbcrossbar_trino::values::ExpectedDataType for #struct_name {
                    fn expected_data_type() -> ::dbcrossbar_trino::values::DataTypeOrAny {
                        ::dbcrossbar_trino::values::DataTypeOrAny::Row(vec![
                            #(#expected_data_type_fields)*
                        ])
                    }
                }

                impl ::std::convert::TryFrom<::dbcrossbar_trino::Value> for #struct_name {
                    type Error = ::dbcrossbar_trino::values::ConversionError;

                    fn try_from(
                        value: ::dbcrossbar_trino::Value,
                    ) -> std::result::Result<Self, Self::Error> {
                        match value {
                            ::dbcrossbar_trino::Value::Row { values, .. } if values.len() == #field_count => {
                                Ok(#construct_struct)
                            }
                            _ => Err(::dbcrossbar_trino::values::ConversionError {
                                found: value,
                                expected_type: <Self as ::dbcrossbar_trino::values::ExpectedDataType>::expected_data_type(),
                            }),
                        }
                    }
                }
            }
        }
        _ => panic!("Only structs are supported by `#[derive(TrinoRow)]`"),
    };
    TokenStream::from(expanded)
}
