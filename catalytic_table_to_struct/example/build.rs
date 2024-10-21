use std::env::current_dir;

use catalytic::runtime::{create_test_tables, query};
use catalytic_table_to_struct::generate;
use catalytic_table_to_struct::transformer::{StructField, StructTable, Transformer};

fn main() {
    let _ = dotenvy::dotenv();

    query("drop table if exists UUIDTable", []);
    query(
        "create table if not exists person(name text, age int, email text, type text, primary key((name), age))",
        [],
    );
    query(
        "create materialized view if not exists person_by_email as
            select *
            from person
            where name is not null and age is not null and email is not null
            primary key ((email), name, age)",
        [],
    );
    query("create table if not exists child(birthday int, json text, json_nullable text, enum_json text, primary key((birthday)))", []);
    query("create table if not exists field_name_different_combined(type int, pub text, struct text, primary key((type), pub))", []);

    create_test_tables();

    struct Trans;

    impl Transformer for Trans {
        fn struct_field(&self, _struct_table: StructTable, column_name: &str) -> StructField {
            StructField {
                json: if column_name.starts_with("json") {
                    Some("crate::MyJsonType".to_string())
                } else if column_name.starts_with("enum") {
                    Some("crate::MyJsonEnum".to_string())
                } else {
                    None
                },
                is_nullable: column_name.contains("nullable"),
                attributes: Default::default(),
                field_name: if column_name == "type" {
                    "row_type".to_string()
                } else if column_name == "pub" {
                    "row_pub".to_string()
                } else if column_name == "struct" {
                    "row_struct".to_string()
                } else {
                    "".to_string()
                },
            }
        }
    }

    generate(
        &current_dir().unwrap().join("src").join("generated"),
        Trans {},
    );
}
