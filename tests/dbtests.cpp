//
// Created by Mike Loomis on 4/24/2019.
//

#include <catch2/catch.hpp>
#include "../db.hpp"

TEST_CASE("Serialize/deserialize puts rows into raw memory and back to struct") {
  char storage[ROW_SIZE];
  Row output_row{};
  {
    Row row{1, "username", "email"};
    serialize_row(row, storage);
  }
  {
    deserialize_row(storage, output_row);
  }
  REQUIRE(output_row.id == 1);
  REQUIRE(std::string(output_row.username.data()) == "username");
  REQUIRE(std::string(output_row.email.data()) == "email");
}

TEST_CASE("Prepare statement catches syntax errors and unrecognized keywords") {
  Statement statement{};
  REQUIRE(prepare_statement("select", statement) == PrepareResult::SUCCESS);
  REQUIRE(prepare_statement("insert", statement) == PrepareResult::SYNTAX_ERROR);
  REQUIRE(prepare_statement("insert a b c", statement) == PrepareResult::SYNTAX_ERROR);
  REQUIRE(prepare_statement("insert 1 bob bob@test.com", statement) == PrepareResult::SUCCESS);
  REQUIRE(prepare_statement("", statement) == PrepareResult::UNRECOGNIZED_STATEMENT);
  REQUIRE(prepare_statement("test", statement) == PrepareResult::UNRECOGNIZED_STATEMENT);
  REQUIRE(prepare_statement("insert -1 test test@email.com", statement) == PrepareResult::NEGATIVE_ID);
  std::string username(33, 'a');
  std::string email(256, 'a');
  REQUIRE(prepare_statement("insert 1 " + username + " " + email, statement) == PrepareResult::STRING_TOO_LONG);
}

TEST_CASE("Execute_insert returns table full or succeeds if row fits") {
  std::remove("test.db");
  Table table{"test.db"};
  Statement statement{};
  REQUIRE(prepare_statement("insert 1 test test@email.com", statement) == PrepareResult::SUCCESS);
  REQUIRE(execute_insert(statement, table) == ExecuteResult::SUCCESS);

  Table fill_this_table{"filldb.db"};
  ExecuteResult last_execute = ExecuteResult::UNHANDLED_STATEMENT;
  for (auto i = 0; i < TABLE_MAX_ROWS; ++i) {
    char buffer[50];
    sprintf_s(buffer, "insert %d user#%d person#%d@example.com", i, i, i);
    prepare_statement(buffer, statement);
    last_execute = execute_insert(statement, fill_this_table);
  }
  REQUIRE(last_execute == ExecuteResult::SUCCESS);
  REQUIRE(execute_insert(statement, fill_this_table) == ExecuteResult::TABLE_FULL);
  db_close(fill_this_table);
  std::remove("filldb.db");
}

TEST_CASE("Execute_select gives all inserted rows and correct return codes") {
  std::remove("test.db");
  Table table{"test.db"};
  Statement statement{};
  REQUIRE(prepare_statement("insert 1 test test@email.com", statement) == PrepareResult::SUCCESS);
  REQUIRE(execute_insert(statement, table) == ExecuteResult::SUCCESS);

  REQUIRE(prepare_statement("select", statement) == PrepareResult::SUCCESS);
  std::vector<Row> selected_rows;
  REQUIRE(execute_select(statement, table, selected_rows) == ExecuteResult::SUCCESS);
  REQUIRE(selected_rows.size() == 1);
  REQUIRE(selected_rows[0].id == 1);
  REQUIRE(std::string(selected_rows[0].username.data()) == "test");
  REQUIRE(std::string(selected_rows[0].email.data()) == "test@email.com");
  std::remove("test.db");
}

TEST_CASE("Table allows inserting strings that are maximum length") {
  std::remove("test.db");
  Table table{"test.db"};
  Statement statement{};
  std::string username(32, 'a');
  std::string email(255, 'a');
  REQUIRE(prepare_statement("insert 1 " + username + " " + email, statement) == PrepareResult::SUCCESS);
  REQUIRE(execute_insert(statement, table) == ExecuteResult::SUCCESS);
  REQUIRE(prepare_statement("select", statement) == PrepareResult::SUCCESS);
  std::vector<Row> selected_rows;
  REQUIRE(execute_select(statement, table, selected_rows) == ExecuteResult::SUCCESS);
  REQUIRE(selected_rows.size() == 1);
  REQUIRE(selected_rows[0].id == 1);
  REQUIRE(std::string(selected_rows[0].username.data()) == username);
  REQUIRE(std::string(selected_rows[0].email.data()) == email);
  std::remove("test.db");
}

TEST_CASE("Data in table persists after reinitializing table") {
  std::remove("test.db");
  {
    Table table{"test.db"};
    Statement statement{};
    REQUIRE(prepare_statement("insert 1 test test@email.com", statement) == PrepareResult::SUCCESS);
    REQUIRE(execute_insert(statement, table) == ExecuteResult::SUCCESS);
    REQUIRE(prepare_statement("select", statement) == PrepareResult::SUCCESS);
    std::vector<Row> selected_rows;
    REQUIRE(execute_select(statement, table, selected_rows) == ExecuteResult::SUCCESS);
    REQUIRE(selected_rows.size() == 1);
    REQUIRE(selected_rows[0].id == 1);
    REQUIRE(std::string(selected_rows[0].username.data()) == "test");
    REQUIRE(std::string(selected_rows[0].email.data()) == "test@email.com");
    db_close(table);
  }
  for (auto i = 0; i < 10; ++i) {
    Table table{"test.db"};
    Statement statement{};
    REQUIRE(prepare_statement("select", statement) == PrepareResult::SUCCESS);
    std::vector<Row> selected_rows;
    REQUIRE(execute_select(statement, table, selected_rows) == ExecuteResult::SUCCESS);
    REQUIRE(selected_rows.size() == 1);
    REQUIRE(selected_rows[0].id == 1);
    REQUIRE(std::string(selected_rows[0].username.data()) == "test");
    REQUIRE(std::string(selected_rows[0].email.data()) == "test@email.com");
    db_close(table);
  }
  std::remove("test.db");
}