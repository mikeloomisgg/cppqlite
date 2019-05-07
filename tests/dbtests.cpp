//
// Created by Mike Loomis on 4/24/2019.
//

#include <catch2/catch.hpp>
#include "../db.hpp"

TEST_CASE("Serialize/deserialize puts rows into raw memory and back to struct") {
  char storage[Row::row_size];
  Row output_row{};
  {
    Row row(1, "username", "email");
    row.serialize(storage);
  }
  {
    output_row = Row(storage);
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

TEST_CASE("Execute_insert returns table full or succeeds if row fits") {
  std::remove("test.db");
  Table table{"test.db"};
  Statement statement{};
  REQUIRE(prepare_statement("insert 1 test test@email.com", statement) == PrepareResult::SUCCESS);
  REQUIRE(execute_insert(statement, table) == ExecuteResult::SUCCESS);

  Table fill_this_table{"filldb.db"};
  for (auto i = 0; i < LeafBody::max_cells + 2; ++i) {
    char buffer[50];
    sprintf_s(buffer, "insert %d user#%d person#%d@example.com", i + 1, i + 1, i + 1);
    prepare_statement(buffer, statement);
    REQUIRE(execute_insert(statement, fill_this_table) == ExecuteResult::SUCCESS);
    REQUIRE(prepare_statement("select", statement) == PrepareResult::SUCCESS);
    std::vector<Row> selected_rows;
    REQUIRE(execute_select(statement, fill_this_table, selected_rows) == ExecuteResult::SUCCESS);
    REQUIRE(selected_rows.size() == i + 1);
  }
  fill_this_table.db_close();
  std::remove("filldb.db");
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
    table.db_close();
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
    table.db_close();
  }
  std::remove("test.db");
}

TEST_CASE("Inserting duplicate keys returns error") {
  std::remove("test.db");
  Table table{"test.db"};
  Statement statement{};
  REQUIRE(prepare_statement("insert 1 test test@email.com", statement) == PrepareResult::SUCCESS);
  REQUIRE(execute_insert(statement, table) == ExecuteResult::SUCCESS);
  REQUIRE(prepare_statement("insert 1 test test@email.com", statement) == PrepareResult::SUCCESS);
  REQUIRE(execute_insert(statement, table) == ExecuteResult::DUPLICATE_KEY);

  REQUIRE(prepare_statement("select", statement) == PrepareResult::SUCCESS);
  std::vector<Row> selected_rows;
  REQUIRE(execute_select(statement, table, selected_rows) == ExecuteResult::SUCCESS);
  REQUIRE(selected_rows.size() == 1);
  REQUIRE(selected_rows[0].id == 1);

  std::remove("test.db");
}

TEST_CASE("Inserted data comes out of select statement sorted") {
  std::remove("test.db");
  Table table{"test.db"};
  Statement statement{};
  REQUIRE(prepare_statement("insert 3 test test@email.com", statement) == PrepareResult::SUCCESS);
  REQUIRE(execute_insert(statement, table) == ExecuteResult::SUCCESS);
  REQUIRE(prepare_statement("insert 1 test test@email.com", statement) == PrepareResult::SUCCESS);
  REQUIRE(execute_insert(statement, table) == ExecuteResult::SUCCESS);
  REQUIRE(prepare_statement("insert 2 test test@email.com", statement) == PrepareResult::SUCCESS);
  REQUIRE(execute_insert(statement, table) == ExecuteResult::SUCCESS);

  REQUIRE(prepare_statement("select", statement) == PrepareResult::SUCCESS);
  std::vector<Row> selected_rows;
  REQUIRE(execute_select(statement, table, selected_rows) == ExecuteResult::SUCCESS);
  REQUIRE(selected_rows.size() == 3);
  REQUIRE(selected_rows[0].id == 1);
  REQUIRE(selected_rows[1].id == 2);
  REQUIRE(selected_rows[2].id == 3);

  std::remove("test.db");
}