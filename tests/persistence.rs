mod common;

use common::TestDb;
use serial_test::serial;

#[test]
#[serial]
fn test_catalog_persistence() {
    let mut db = TestDb::new();

    // CREATE TABLE
    db.execute_sql("CREATE TABLE persistent_table (id INT, name STRING, PRIMARY KEY (id));")
        .expect("CREATE TABLE failed");

    // Restart database
    db.restart().expect("restart failed");

    // Table should still exist - try to insert into it
    let result = db.execute_sql("INSERT INTO persistent_table VALUES (1, 'test');");
    assert!(result.is_ok(), "table should exist after restart: {:?}", result);
}

#[test]
#[serial]
fn test_data_survives_restart() {
    let mut db = TestDb::new();

    // CREATE TABLE and INSERT data
    db.execute_sql("CREATE TABLE data_test (id INT, value INT, PRIMARY KEY (id));")
        .expect("CREATE TABLE failed");

    db.execute_sql("INSERT INTO data_test VALUES (1, 100), (2, 200), (3, 300);")
        .expect("INSERT failed");

    // Verify data before restart
    let before = db
        .execute_sql("SELECT COUNT(*) FROM data_test;")
        .expect("SELECT COUNT before restart failed");

    // Restart
    db.restart().expect("restart failed");

    // Verify data after restart
    let after = db
        .execute_sql("SELECT COUNT(*) FROM data_test;")
        .expect("SELECT COUNT after restart failed");

    assert_eq!(before.trim(), after.trim(), "data count should be same after restart");

    // Verify specific values
    let result = db
        .execute_sql("SELECT value FROM data_test WHERE id = 2;")
        .expect("SELECT after restart failed");
    assert!(result.contains("200"), "data value should survive restart");
}

#[test]
#[serial]
fn test_multiple_tables_persist() {
    let mut db = TestDb::new();

    // Create multiple tables
    db.execute_sql("CREATE TABLE table1 (id INT, PRIMARY KEY (id));")
        .expect("CREATE TABLE 1 failed");

    db.execute_sql("CREATE TABLE table2 (id INT, name STRING, PRIMARY KEY (id));")
        .expect("CREATE TABLE 2 failed");

    db.execute_sql("CREATE TABLE table3 (id INT, value FLOAT, PRIMARY KEY (id));")
        .expect("CREATE TABLE 3 failed");

    // Insert into all tables
    db.execute_sql("INSERT INTO table1 VALUES (1), (2);")
        .expect("INSERT table1 failed");

    db.execute_sql("INSERT INTO table2 VALUES (1, 'a'), (2, 'b');")
        .expect("INSERT table2 failed");

    db.execute_sql("INSERT INTO table3 VALUES (1, 1.5), (2, 2.5);")
        .expect("INSERT table3 failed");

    // Restart
    db.restart().expect("restart failed");

    // All tables should still exist and have data
    let result1 = db
        .execute_sql("SELECT COUNT(*) FROM table1;")
        .expect("table1 after restart failed");
    assert!(result1.contains("2"), "table1 should have 2 rows");

    let result2 = db
        .execute_sql("SELECT COUNT(*) FROM table2;")
        .expect("table2 after restart failed");
    assert!(result2.contains("2"), "table2 should have 2 rows");

    let result3 = db
        .execute_sql("SELECT COUNT(*) FROM table3;")
        .expect("table3 after restart failed");
    assert!(result3.contains("2"), "table3 should have 2 rows");
}

#[test]
#[serial]
fn test_primary_key_constraint_survives() {
    let mut db = TestDb::new();

    db.execute_sql("CREATE TABLE pk_test (id INT, data STRING, PRIMARY KEY (id));")
        .expect("CREATE TABLE failed");

    db.execute_sql("INSERT INTO pk_test VALUES (1, 'one');")
        .expect("INSERT failed");

    // Restart
    db.restart().expect("restart failed");

    // PK constraint should still be enforced
    let result = db.execute_sql("INSERT INTO pk_test VALUES (1, 'duplicate');");
    assert!(
        result.is_err() || result.unwrap().contains("ERROR"),
        "PK constraint should be enforced after restart"
    );
}

#[test]
#[serial]
fn test_large_dataset_persistence() {
    let mut db = TestDb::new();

    db.execute_sql("CREATE TABLE large_data (id INT, value INT, PRIMARY KEY (id));")
        .expect("CREATE TABLE failed");

    // Insert many rows
    let mut insert_sql = String::from("INSERT INTO large_data VALUES ");
    for i in 1..=100 {
        if i > 1 {
            insert_sql.push(',');
        }
        insert_sql.push_str(&format!("({}, {})", i, i * 10));
    }
    insert_sql.push(';');

    db.execute_sql(&insert_sql)
        .expect("INSERT large dataset failed");

    // Verify count
    let count_before = db
        .execute_sql("SELECT COUNT(*) FROM large_data;")
        .expect("SELECT COUNT before restart failed");

    // Restart
    db.restart().expect("restart failed");

    // Verify count after
    let count_after = db
        .execute_sql("SELECT COUNT(*) FROM large_data;")
        .expect("SELECT COUNT after restart failed");

    assert!(
        count_before.contains("100"),
        "should have 100 rows before restart"
    );
    assert!(
        count_after.contains("100"),
        "should have 100 rows after restart"
    );
}