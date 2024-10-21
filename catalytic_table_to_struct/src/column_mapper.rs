use crate::transformer::{StructTable, Transformer};

use catalytic::table_metadata::{ColumnInTable, ColumnKind, ColumnType};
use proc_macro2::{Ident, TokenStream};
use quote::{format_ident, quote};

#[derive(Clone)]
pub struct Field {
    pub column_name: Ident,
    pub field_name: Ident,
    pub ident_ty: TokenStream,
    pub ty: TokenStream,
    /// When 'ty' is String, this will be 'str'
    pub borrow_ty: TokenStream,
    /// When 'ty' is String, this will be 'to_string()', else 'clone()'
    pub from_borrow_to_owned: TokenStream,
    #[allow(dead_code)]
    pub attributes: TokenStream,
    /// Contains only the primary key attributes
    pub pk_attributes: TokenStream,
}

pub struct StructFieldMetadata {
    /// Contains the ts to write the properties
    pub field_ts: TokenStream,
    /// Contains the primary key fields only
    pub primary_key_fields: Vec<Field>,
    pub fields: Vec<Field>,
    pub non_primary_key_fields: Vec<Field>,
}

impl StructFieldMetadata {
    pub fn pk_fields_ident(&self) -> Vec<Ident> {
        self.primary_key_fields
            .iter()
            .map(|c| format_ident!("{}", c.field_name))
            .collect()
    }
}

/// Transforms a db column to a Rust property
pub(crate) fn column_to_property(
    struct_table: StructTable,
    columns: &[ColumnInTable],
    transformer: &impl Transformer,
) -> StructFieldMetadata {
    let mut pk_fields = vec![];
    let mut fields = vec![];
    let mut non_pk_fields = vec![];
    let mut ts = TokenStream::new();

    for column in columns {
        let pk_attributes = match column.kind() {
            ColumnKind::PartitionKey => {
                quote! {
                    #[partition_key]
                }
            }
            ColumnKind::Regular => quote! {}, // Doesn't need an attribute
            ColumnKind::Clustering => {
                quote! {
                    #[clustering_key]
                }
            }
        };

        let mut field_ts = pk_attributes.clone();
        let struct_field = transformer.struct_field(struct_table.clone(), &column.column_name);
        let attrs = struct_field.attributes;
        let pk_attributes = TokenStream::default();

        field_ts.extend(quote! {
            #attrs
        });

        let clone = "clone()".to_string();

        let (mut ty, mut borrow_ty, from_borrow_to_owned) = if let Some(json) = struct_field.json {
            // Only text columns can be mapped to json
            assert_eq!("text", &column.data_type);

            field_ts.extend(quote! {
                #[json]
            });

            (json.clone(), json, clone)
        } else {
            let ty = ColumnType::new(column.data_type.as_str()).to_ty();
            let (borrow_ty, from_borrow_to_owned) = if column.data_type.as_str() == "text" {
                ("str".to_string(), "to_string()".to_string())
            } else {
                (ty.clone(), clone)
            };

            (ty, borrow_ty, from_borrow_to_owned)
        };

        if struct_field.is_nullable {
            let make_nullable = |t| format!("std::option::Option<{}>", t);

            ty = make_nullable(ty);
            borrow_ty = make_nullable(borrow_ty);
        }

        let ty: TokenStream = ty.parse().unwrap();
        let field_name = if struct_field.field_name.is_empty() {
            &column.column_name
        } else {
            &struct_field.field_name
        };
        let field_name = format_ident!("{}", field_name);
        let attributes = field_ts.clone();

        field_ts.extend(quote! {
            pub #field_name: #ty,
        });

        let field = Field {
            column_name: format_ident!("{}", column.column_name),
            field_name,
            ident_ty: field_ts.clone(),
            ty,
            borrow_ty: borrow_ty.parse().unwrap(),
            from_borrow_to_owned: from_borrow_to_owned.parse().unwrap(),
            attributes,
            pk_attributes,
        };

        fields.push(field.clone());

        if column.kind() == ColumnKind::Regular {
            non_pk_fields.push(field);
        } else {
            pk_fields.push(field);
        }

        ts.extend(field_ts);
    }

    StructFieldMetadata {
        field_ts: ts,
        primary_key_fields: pk_fields,
        fields,
        non_primary_key_fields: non_pk_fields,
    }
}
