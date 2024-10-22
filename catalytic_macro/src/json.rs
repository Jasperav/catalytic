use proc_macro2::TokenStream;
use quote::quote;
use syn::DeriveInput;

pub(crate) fn json(derive_input: DeriveInput) -> TokenStream {
    let name = derive_input.ident;

    quote! {
        impl catalytic::scylla::_macro_internal::SerializeValue for #name {
            fn serialize<'b>(
                &self,
                typ: &catalytic::scylla::_macro_internal::ColumnType,
                writer: catalytic::scylla::_macro_internal::CellWriter<'b>,
            ) -> Result<catalytic::scylla::_macro_internal::WrittenCellProof<'b>, catalytic::scylla::_macro_internal::SerializationError> {
                let serialized: String = serde_json::to_string(&self).unwrap().into();

                serialized.serialize(typ, writer)
            }
        }

        impl catalytic::scylla::frame::value::Value for #name {
            fn serialize(&self, buf: &mut Vec<u8>) -> Result<(), catalytic::scylla::frame::value::ValueTooBig> {
                let serialized: String = serde_json::to_string(&self).unwrap().into();

                serialized.serialize(buf)
            }
        }

        impl catalytic::scylla::cql_to_rust::FromCqlVal<catalytic::scylla::frame::response::result::CqlValue> for #name {
            fn from_cql(cql_val: catalytic::scylla::frame::response::result::CqlValue) -> Result<Self, catalytic::scylla::cql_to_rust::FromCqlValError> {
                let t = match cql_val.as_text() {
                    None => return Err(catalytic::scylla::cql_to_rust::FromCqlValError::BadCqlType),
                    Some(t) => t
                };

                serde_json::from_str(&t).map_err(|_| catalytic::scylla::cql_to_rust::FromCqlValError::BadCqlType)
            }
        }
    }
}
