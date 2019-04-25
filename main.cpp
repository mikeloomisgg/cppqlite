#include <iostream>
#include <string>
#include <optional>
#include <array>

enum class ExecuteResult {
  SUCCESS,
  TABLE_FULL,
  UNHANDLED_STATEMENT
};

enum class MetaCommandResult {
  SUCCESS,
  UNRECOGNIZED_COMMAND
};

enum class PrepareResult {
  SUCCESS,
  SYNTAX_ERROR,
  UNRECOGNIZED_STATEMENT
};

struct Row {
  static const std::size_t COLUMN_USERNAME_SIZE = 32;
  static const std::size_t COLUMN_EMAIL_SIZE = 255;

  uint32_t id;
  char username[COLUMN_USERNAME_SIZE];
  char email[COLUMN_EMAIL_SIZE];
};

struct Statement {
  enum StatementType {
    INSERT,
    SELECT
  };

  StatementType statement_type;
  Row row_to_insert; // only used by insert statement
};

const uint32_t ID_SIZE = sizeof(Row::id);
const uint32_t USERNAME_SIZE = sizeof(Row::username);
const uint32_t EMAIL_SIZE = sizeof(Row::email);
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;
const uint32_t PAGE_SIZE = 4096;
const uint32_t TABLE_MAX_PAGES = 100;
const uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
const uint32_t TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;

using Page = std::array<uint8_t, PAGE_SIZE>;

struct Table {
  uint32_t num_rows;
  std::array<Page, TABLE_MAX_PAGES> pages;
};

void print_row(const Row &row) {
  std::cout << "(" << row.id << ", " << row.username << ", " << row.email << ")\n";
}

void serialize_row(const Row &source, uint8_t *destination) {
  memcpy(destination + ID_OFFSET, &source.id, ID_SIZE);
  memcpy(destination + USERNAME_OFFSET, &source.username, USERNAME_SIZE);
  memcpy(destination + EMAIL_OFFSET, &source.email, EMAIL_SIZE);
}

void deserialize_row(uint8_t *source, Row &destination) {
  memcpy(&destination.id, source + ID_OFFSET, ID_SIZE);
  memcpy(&destination.username, source + USERNAME_OFFSET, USERNAME_SIZE);
  memcpy(&destination.email, source + EMAIL_OFFSET, EMAIL_SIZE);
}

uint8_t *row_slot(Table &table, const uint32_t row_num) {
  const uint32_t page_num = row_num / ROWS_PER_PAGE;
  auto &page = table.pages[page_num];
  uint32_t row_offset = row_num % ROWS_PER_PAGE;
  uint32_t byte_offset = row_offset * ROW_SIZE;
  return page.data() + byte_offset;
}

MetaCommandResult do_meta_command(const std::string &command, Table &table) {
  if (command == ".exit") {
    exit(EXIT_SUCCESS);
  } else {
    return MetaCommandResult::UNRECOGNIZED_COMMAND;
  }
}

PrepareResult prepare_statement(const std::string &input, Statement &out_statement) {
  if (input.substr(0, 6) == "insert") {
    out_statement = Statement{Statement::INSERT};
    int args_assigned = sscanf_s(
        input.data(), "insert %d %s %s", &(out_statement.row_to_insert.id),
        out_statement.row_to_insert.username, USERNAME_SIZE, out_statement.row_to_insert.email, EMAIL_SIZE
    );
    if (args_assigned < 3) {
      return PrepareResult::SYNTAX_ERROR;
    }
    return PrepareResult::SUCCESS;
  }
  if (input.substr(0, 6) == "select") {
    out_statement = Statement{Statement::SELECT};
    return PrepareResult::SUCCESS;
  }
  return PrepareResult::UNRECOGNIZED_STATEMENT;
}

ExecuteResult execute_insert(const Statement &statement, Table &table) {
  if (table.num_rows >= TABLE_MAX_ROWS) {
    return ExecuteResult::TABLE_FULL;
  }

  const Row &row_to_insert = statement.row_to_insert;
  serialize_row(row_to_insert, row_slot(table, table.num_rows));
  table.num_rows += 1;

  return ExecuteResult::SUCCESS;
}

ExecuteResult execute_select(const Statement &statement, Table &table) {
  Row row{};
  for (uint32_t i = 0; i < table.num_rows; ++i) {
    deserialize_row(row_slot(table, i), row);
    print_row(row);
  }
  return ExecuteResult::SUCCESS;
}

ExecuteResult execute_statement(const Statement &statement, Table &table) {
  switch (statement.statement_type) {
    case (Statement::INSERT):
      return execute_insert(statement, table);
    case (Statement::SELECT):
      return execute_select(statement, table);
  }
  return ExecuteResult::UNHANDLED_STATEMENT;
}

int main() {
  Table table{};
  std::string input;
  while (true) {
    std::cout << "db > ";
    std::getline(std::cin, input);

    if (input[0] == '.') {
      switch (do_meta_command(input, table)) {
        case (MetaCommandResult::SUCCESS):
          continue;
        case (MetaCommandResult::UNRECOGNIZED_COMMAND):
          std::cout << "Unrecognized command: " << input << '\n';
          continue;
      }
    }
    Statement statement{};
    switch (prepare_statement(input, statement)) {
      case (PrepareResult::SUCCESS):
        break;
      case (PrepareResult::SYNTAX_ERROR):
        std::cout << "Syntax error. Could not parse statement.\n";
        continue;
      case (PrepareResult::UNRECOGNIZED_STATEMENT):
        std::cout << "Unrecognized keyword at start of '" << input << "'.\n";
        continue;
    }

    switch (execute_statement(statement, table)) {
      case (ExecuteResult::SUCCESS):
        std::cout << "Executed.\n";
        break;
      case (ExecuteResult::TABLE_FULL):
        std::cout << "Error: Table full.\n";
        break;
      case (ExecuteResult::UNHANDLED_STATEMENT):
        std::cout << "Error: Unhandled statement.\n";
        break;
    }
  }
}