#[allow(clippy::derive_partial_eq_without_eq)]
pub mod generated;

#[derive(
    Clone, catalytic_macro::Json, serde::Serialize, serde::Deserialize, Debug, Eq, PartialEq,
)]
pub struct MyJsonType {
    pub age: i32,
}

#[derive(
    Clone, catalytic_macro::Json, serde::Serialize, serde::Deserialize, Debug, Eq, PartialEq,
)]
pub enum MyJsonEnum {
    Something,
    SomethingElse,
}

#[cfg(test)]
mod test {
    use crate::generated::child::{truncate, Child};
    use crate::generated::person::{PersonRef, UpdatableColumnRef};
    use crate::generated::{FieldNameDifferentCombined, Person};
    use crate::{MyJsonEnum, MyJsonType};
    use catalytic::runtime::create_connection;
    use catalytic::scylla;
    use catalytic::scylla::transport::{PagingState, PagingStateResponse};
    use catalytic_macro::{query, query_base_table};
    use futures_util::StreamExt;
    use scylla::frame::value::{LegacySerializedValues, SerializeValuesError};
    use scylla::CachingSession;

    #[tokio::test]
    async fn crud() {
        let session = CachingSession::from(create_connection().await, 1);

        crate::generated::child::truncate(&session).await.unwrap();
        crate::generated::person::truncate(&session).await.unwrap();

        let mut child = Child {
            birthday: 1,
            enum_json: MyJsonEnum::Something,
            json: MyJsonType { age: 1 },
            json_nullable: None,
        };

        // Inserts
        // Insert with ttl
        child.to_ref().insert_ttl(&session, 100).await.unwrap();
        child.to_ref().insert(&session).await.unwrap();

        // Selects
        macro_rules! eq {
            () => {
                assert_eq!(
                    child
                        .primary_key()
                        .select_unique(&session)
                        .await
                        .unwrap()
                        .entity
                        .unwrap(),
                    child
                );
                assert_eq!(
                    child
                        .primary_key()
                        .select_unique_expect(&session)
                        .await
                        .unwrap()
                        .entity,
                    child
                );
            };
        }

        eq!();

        let mut iterator = crate::generated::child::select_all(&session, Some(2))
            .await
            .unwrap();

        while let Some(row) = iterator.next().await {
            assert_eq!(child, row.unwrap());
        }

        let all = crate::generated::child::select_all_in_memory(&session, 123)
            .await
            .unwrap()
            .entities;

        assert_eq!(1, all.len());
        assert_eq!(&child, &all[0]);

        assert_eq!(
            1,
            crate::generated::child::select_all_count(&session)
                .await
                .unwrap()
                .entity
        );

        // Select now for the base table from the mv
        let person = Person {
            name: "name".to_string(),
            age: 1,
            email: "2".to_string(),
            row_type: "test".to_string(),
        };

        person.to_ref().insert(&session).await.unwrap();

        let mut iterator = crate::generated::person_by_email::select_all_base_table(&session, None)
            .await
            .unwrap();

        while let Some(row) = iterator.next().await {
            assert_eq!(person, row.unwrap());
        }

        let pbe = crate::generated::person_by_email::PrimaryKeyRef {
            email: &person.email,
            name: &person.name,
            age: &person.age,
        };

        assert_eq!(
            pbe.select_unique_base_table(&session)
                .await
                .unwrap()
                .entity
                .unwrap(),
            person
        );
        assert_eq!(
            pbe.select_unique_expect_base_table(&session)
                .await
                .unwrap()
                .entity,
            person
        );

        // Single updates, with and without nullable values
        child.json_nullable = Some(MyJsonType { age: 2 });

        child
            .primary_key()
            .update_json_nullable(&session, &child.json_nullable)
            .await
            .unwrap();

        eq!();

        child.json_nullable = None;

        child
            .primary_key()
            .update_json_nullable(&session, &child.json_nullable)
            .await
            .unwrap();

        eq!();

        // Single dynamic update
        child.json = MyJsonType { age: 3 };

        child
            .primary_key()
            .update_dyn(&session, child.updatable_column_json())
            .await
            .unwrap();

        eq!();

        // Single dynamic updates in vec
        child.json = MyJsonType { age: 4 };

        child
            .primary_key()
            .update_dyn_multiple(&session, &[child.updatable_column_json()])
            .await
            .unwrap();

        eq!();

        // Multiple dynamic updates in vec
        child.json = MyJsonType { age: 5 };

        child.json_nullable = Some(MyJsonType { age: 6 });

        child
            .primary_key()
            .update_dyn_multiple(
                &session,
                &[
                    child.updatable_column_json(),
                    child.updatable_column_json_nullable(),
                ],
            )
            .await
            .unwrap();

        eq!();

        // Delete
        child.primary_key().delete(&session).await.unwrap();

        macro_rules! empty {
            () => {
                assert!(child
                    .primary_key()
                    .select_unique(&session)
                    .await
                    .unwrap()
                    .entity
                    .is_none());
                assert!(child
                    .primary_key()
                    .select_unique_expect(&session)
                    .await
                    .is_err());
            };
        }

        empty!();

        // Insert and check truncate
        child.to_ref().insert(&session).await.unwrap();

        eq!();

        truncate(&session).await.unwrap();

        empty!();
    }

    macro_rules! assert_serialized_values {
        ($sv: expr, $($val: expr),*) => {{
            let mut serialized_values = LegacySerializedValues::new();

            $(serialized_values.add_value(&$val).unwrap();)*

            assert_eq!(serialized_values, $sv.qv.values);
        }};
    }

    #[tokio::test]
    async fn paging() -> Result<(), SerializeValuesError> {
        let session = CachingSession::from(create_connection().await, 1);
        let rows_to_generate = 100;
        let page_size = 7;

        crate::generated::person::truncate(&session).await.unwrap();

        let name = "doesnt_matter";

        for index in 0..rows_to_generate {
            PersonRef {
                name,
                age: &index,
                email: "",
                row_type: "34",
            }
            .insert(&session)
            .await
            .unwrap();
        }

        // Let's page
        let select_multiple = query!("select * from person where name = ? order by age", name);
        let mut paging_state = PagingState::start();
        let mut counter_rows = 0;
        let mut counter_loop = 0;

        loop {
            counter_loop += 1;

            let result = select_multiple
                .select_paged(&session, Some(page_size), paging_state.clone())
                .await
                .unwrap();


            for row in result.entities {
                assert_eq!(row.age, counter_rows);

                counter_rows += 1;
            }

            if let PagingStateResponse::HasMorePages { state: next_state } = result.paging_result {
                paging_state = next_state;
            } else {
                break;
            }
        }

        assert_eq!(counter_rows, rows_to_generate);
        assert_eq!(counter_loop, (rows_to_generate / 7) + 1);

        crate::generated::person::truncate(&session).await.unwrap();

        Ok(())
    }

    /// Tests that when a custom field_name is provided, everything keeps working
    #[tokio::test]
    async fn custom_field_name() {
        let connection = CachingSession::from(create_connection().await, 1);

        crate::generated::person::truncate(&connection)
            .await
            .unwrap();

        let mut person = Person {
            name: "name".to_string(),
            age: 1,
            email: "myemail".to_string(),
            row_type: "4".to_string(),
        };

        person.to_ref().insert(&connection).await.unwrap();

        macro_rules! check_values_person {
            () => {
                let all = crate::generated::person::select_all_in_memory(&connection, 2)
                    .await
                    .unwrap();

                assert_eq!(1, all.entities.len());

                assert_eq!(all.entities[0], person);
            };
        }

        check_values_person!();

        person.row_type += "a";

        person
            .primary_key()
            .update_row_type(&connection, &person.row_type)
            .await
            .unwrap();

        check_values_person!();

        person.row_type += "a";

        person
            .primary_key()
            .update_dyn(&connection, UpdatableColumnRef::RowType(&person.row_type))
            .await
            .unwrap();

        check_values_person!();

        person.row_type += "a";
        person.email += "a";

        person
            .primary_key()
            .update_dyn_multiple(
                &connection,
                &[
                    UpdatableColumnRef::RowType(&person.row_type),
                    UpdatableColumnRef::Email(&person.email),
                ],
            )
            .await
            .unwrap();

        check_values_person!();

        crate::generated::field_name_different_combined::truncate(&connection)
            .await
            .unwrap();

        let field_name = FieldNameDifferentCombined {
            row_type: 1,
            row_pub: "a".to_string(),
            row_struct: "b".to_string(),
        };

        field_name.to_ref().insert(&connection).await.unwrap();

        macro_rules! check_values_field_name {
            () => {
                let all = crate::generated::field_name_different_combined::select_all_in_memory(
                    &connection,
                    2,
                )
                .await
                .unwrap();

                assert_eq!(1, all.entities.len());
                assert_eq!(all.entities[0], field_name);
            };
        }

        check_values_field_name!();

        field_name.primary_key().delete(&connection).await.unwrap();

        let result =
            crate::generated::field_name_different_combined::select_all_in_memory(&connection, 2)
                .await
                .unwrap();

        assert!(result.entities.is_empty());
    }

    #[tokio::test]
    async fn qmd() -> Result<(), SerializeValuesError> {
        let session = CachingSession::from(create_connection().await, 1);

        crate::generated::person::truncate(&session).await.unwrap();

        let person = Person {
            name: "name".to_string(),
            age: 1,
            email: "myemail".to_string(),
            row_type: "4".to_string(),
        };

        person.to_ref().insert(&session).await.unwrap();

        let email = &person.email;
        let transformed_type =
            query_base_table!("select * from person_by_email where email = ?", email);
        let mut rows = transformed_type.select(&session, None).await.unwrap();

        while let Some(row) = rows.next().await {
            assert_eq!(person, row.unwrap());
        }

        let c = &1;
        let transformed_type = query!("select * from test_table where b = 1 and c = ?", c);

        // The extracted query should be the columns and not * so it can be used in prepared statements
        assert_eq!(
            "select b, c, d, a, e from test_table where b = 1 and c = ?",
            transformed_type.query
        );

        assert_serialized_values!(transformed_type, c);

        let transformed_type = query!("select * from test_table where b = 1 and c = 2 and d = 3");

        assert_eq!(
            "select b, c, d, a, e from test_table where b = 1 and c = 2 and d = 3",
            transformed_type.query
        );
        assert_eq!(LegacySerializedValues::new(), transformed_type.qv.values);

        let _ = query!("select * from test_table where b = 1 and c = 2 and d > 3");
        let val = &1;
        let _ = query!(
            "select * from test_table where b = 1 and c = 2 and d > ?",
            val
        );

        let transformed_type = query!("select * from test_table limit 1");

        assert_eq!(
            "select b, c, d, a, e from test_table limit 1",
            transformed_type.query
        );
        assert_eq!(LegacySerializedValues::new(), transformed_type.qv.values);

        let transformed_type = query!("select count(*) from test_table where b = 1 and c = 2");

        assert_eq!(
            "select count(*) from test_table where b = 1 and c = 2",
            transformed_type.query
        );
        assert_eq!(LegacySerializedValues::new(), transformed_type.qv.values);

        let transformed_type = query!("delete from test_table where b = 1 and c = 3");

        assert_eq!(
            "delete from test_table where b = 1 and c = 3",
            transformed_type.query
        );
        assert_eq!(LegacySerializedValues::new(), transformed_type.qv.values);

        let a = &1;
        let transformed_type = query!("select * from test_table where b = 1 and c = ?", a);

        assert_eq!(
            "select b, c, d, a, e from test_table where b = 1 and c = ?",
            transformed_type.query
        );
        assert_serialized_values!(transformed_type, a);

        let a = "sadas";
        let transformed_type = query!("select * from another_test_table where a = 1 and b = ?", a);
        assert_serialized_values!(transformed_type, a);

        let a = &vec![1, 2, 3];
        let transformed_type = query!("select * from test_table where b = 1 and c in ?", a);
        assert_serialized_values!(transformed_type, a);

        let a = &vec![1, 2];
        let transformed_type = query!("select * from test_table where b = 1 and c in ? limit 1", a);
        assert_serialized_values!(transformed_type, a);

        let c = &vec![1, 2];
        let b = &1;
        let limit = &5;
        let transformed_type = query!(
            "select * from test_table where b = ? and c in ? limit ?",
            b,
            c,
            limit
        );

        assert_serialized_values!(transformed_type, b, c, limit);

        Ok(())
    }

    macro_rules! write_failing {
        ($file: ident) => {
            #[test]
            fn $file() {
                catalytic::runtime::create_test_tables();

                let t = trybuild::TestCases::new();
                let current_dir = std::env::current_dir()
                    .unwrap()
                    .join("src")
                    .join("non_compiling_code");

                t.compile_fail(current_dir.join(format!("{}.rs", stringify!($file))));
            }
        };
    }

    write_failing!(failing_wrong_type_primitive);
    write_failing!(failing_wrong_type_vec);
    write_failing!(non_complete_insert);
    write_failing!(non_existing_column);
    write_failing!(non_existing_table);
    write_failing!(no_param_but_question_mark);
    write_failing!(param_but_no_question_mark);
}
