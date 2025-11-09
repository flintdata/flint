mod common;

use common::TestDb;
use serial_test::serial;

#[test]
#[serial]
fn test_create_table_and_insert() {
    let db = TestDb::new();

    // CREATE TABLE with PRIMARY KEY
    let result = db.execute_sql("CREATE TABLE users (id INT, name STRING, PRIMARY KEY (id));");
    assert!(result.is_ok(), "CREATE TABLE failed: {:?}", result);

    // INSERT rows
    let result = db.execute_sql("INSERT INTO users VALUES (1, 'alice'), (2, 'bob');");
    assert!(result.is_ok(), "INSERT failed: {:?}", result);

    // SELECT all
    let result = db.execute_sql("SELECT * FROM users;");
    assert!(result.is_ok(), "SELECT failed: {:?}", result);
    let output = result.unwrap();
    assert!(output.contains("alice"), "alice not found in output");
    assert!(output.contains("bob"), "bob not found in output");
}

#[test]
#[serial]
fn test_primary_key_constraint() {
    let db = TestDb::new();

    db.execute_sql("CREATE TABLE test_pk (id INT, name STRING, PRIMARY KEY (id));")
        .expect("CREATE TABLE failed");

    db.execute_sql("INSERT INTO test_pk VALUES (1, 'alice');")
        .expect("first INSERT failed");

    // Attempt to insert duplicate PK - should fail
    let result = db.execute_sql("INSERT INTO test_pk VALUES (1, 'bob');");
    assert!(
        result.is_err() || result.unwrap().contains("ERROR"),
        "duplicate PK should fail"
    );
}

#[test]
#[serial]
fn test_select_with_where() {
    let db = TestDb::new();

    db.execute_sql("CREATE TABLE numbers (id INT, value INT, PRIMARY KEY (id));")
        .expect("CREATE TABLE failed");

    db.execute_sql("INSERT INTO numbers VALUES (1, 100), (2, 200), (3, 300);")
        .expect("INSERT failed");

    // SELECT with WHERE clause
    let result = db
        .execute_sql("SELECT value FROM numbers WHERE id = 2;")
        .expect("SELECT failed");

    assert!(result.contains("200"), "value not found in WHERE result");
}

#[test]
#[serial]
fn test_insert_multiple_types() {
    let db = TestDb::new();

    db.execute_sql(
        "CREATE TABLE mixed_types (id INT, amount FLOAT, name STRING, PRIMARY KEY (id));",
    )
    .expect("CREATE TABLE failed");

    db.execute_sql(
        "INSERT INTO mixed_types VALUES (1, 1.5, 'test');",
    )
    .expect("INSERT failed");

    let result = db
        .execute_sql("SELECT * FROM mixed_types;")
        .expect("SELECT failed");

    assert!(result.contains("1"), "id not found");
    assert!(result.contains("test"), "name not found");
}

#[test]
#[serial]
fn test_scan_empty_table() {
    let db = TestDb::new();

    db.execute_sql("CREATE TABLE empty_table (id INT, data STRING, PRIMARY KEY (id));")
        .expect("CREATE TABLE failed");

    // SELECT from empty table should return no rows, not error
    let result = db
        .execute_sql("SELECT * FROM empty_table;")
        .expect("SELECT on empty table failed");

    // Output should not contain error
    assert!(!result.contains("ERROR"), "empty table SELECT should not error");
}

#[test]
#[serial]
fn test_table_already_exists() {
    let db = TestDb::new();

    db.execute_sql("CREATE TABLE duplicate_test (id INT, PRIMARY KEY (id));")
        .expect("first CREATE TABLE failed");

    // Second CREATE TABLE with same name should fail
    let result = db.execute_sql("CREATE TABLE duplicate_test (id INT, PRIMARY KEY (id));");

    assert!(
        result.is_err() || result.unwrap().contains("ERROR"),
        "duplicate CREATE TABLE should fail"
    );
}