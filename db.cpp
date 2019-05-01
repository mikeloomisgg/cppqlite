//
// Created by Mike Loomis on 4/25/2019.
//

#include "db.hpp"

#include <cctype>
#include <algorithm>

void print_row(const Row &row) {
  std::cout << "(" << row.id << ", " << row.username.data() << ", " << row.email.data() << ")\n";
}

void serialize_row(const Row &source, char *destination) {
  memcpy(destination + ID_OFFSET, &source.id, ID_SIZE);
  memcpy(destination + USERNAME_OFFSET, source.username.data(), USERNAME_SIZE);
  memcpy(destination + EMAIL_OFFSET, source.email.data(), EMAIL_SIZE);
}

void deserialize_row(const char *source, Row &destination) {
  destination.id = *(uint32_t *) (source + ID_OFFSET);
  destination.username = *(std::array<char, USERNAME_SIZE> *) (source + USERNAME_OFFSET);
  destination.email = *(std::array<char, EMAIL_SIZE> *) (source + EMAIL_OFFSET);
}

char *Table::row_slot(const std::size_t row_num) {
  const std::size_t page_num = row_num / ROWS_PER_PAGE;
  auto &page = pager.get_page(page_num);
  uint32_t row_offset = row_num % ROWS_PER_PAGE;
  uint32_t byte_offset = row_offset * ROW_SIZE;
  return page.data.data() + byte_offset;
}

void Table::db_close() {
  std::size_t num_full_pages = num_rows / ROWS_PER_PAGE;
  for (std::size_t i = 0; i < num_full_pages; ++i) {
    if (pager.pages[i].cached) {
      pager.flush(i, PAGE_SIZE);
    }
  }
  std::size_t num_additional_rows = num_rows % ROWS_PER_PAGE;
  if (num_additional_rows > 0) {
    auto page_num = num_full_pages;
    if (pager.pages[page_num].cached) {
      pager.flush(page_num, num_additional_rows * ROW_SIZE);
    }
  }

  pager.file.close();
}

MetaCommandResult do_meta_command(const std::string &command, Table &table) {
  if (command == ".exit") {
    table.db_close();
    exit(EXIT_SUCCESS);
  } else {
    return MetaCommandResult::UNRECOGNIZED_COMMAND;
  }
}

std::vector<std::string> tokenize(const std::string &str, const std::string &delimiters) {
  std::vector<std::string> tokens;
  std::string::size_type lastPos = str.find_first_not_of(delimiters, 0);
  std::string::size_type pos = str.find_first_of(delimiters, lastPos);

  while (std::string::npos != pos || std::string::npos != lastPos) {
    tokens.emplace_back(str.substr(lastPos, pos - lastPos));
    lastPos = str.find_first_not_of(delimiters, pos);
    pos = str.find_first_of(delimiters, lastPos);
  }

  return tokens;
}

bool is_number(const std::string &s) {
  return !s.empty() && std::find_if(s.begin(), s.end(), [](char c) { return !std::isdigit(c); }) == s.end();
}

PrepareResult prepare_statement(const std::string &input, Statement &out_statement) {
  std::vector<std::string> tokens = tokenize(input, " ");
  if (tokens.empty()) {
    return PrepareResult::UNRECOGNIZED_STATEMENT;
  }
  if (tokens[0] == "insert") {
    out_statement = Statement{Statement::INSERT};
    if (tokens.size() != 4) {
      return PrepareResult::SYNTAX_ERROR;
    }
    int id = std::strtol(tokens[1].data(), nullptr, 10);
    if (id < 0) {
      return PrepareResult::NEGATIVE_ID;
    }
    if (!is_number(tokens[1])) {
      return PrepareResult::SYNTAX_ERROR;
    }
    if (tokens[2].size() >= USERNAME_SIZE || tokens[3].size() >= EMAIL_SIZE) {
      return PrepareResult::STRING_TOO_LONG;
    }

    out_statement.row_to_insert.id = id;
    std::copy(tokens[2].begin(), tokens[2].end(), out_statement.row_to_insert.username.data());
    std::copy(tokens[3].begin(), tokens[3].end(), out_statement.row_to_insert.email.data());
    return PrepareResult::SUCCESS;
  }
  if (tokens[0] == "select") {
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
  serialize_row(row_to_insert, table.row_slot(table.num_rows));
  table.num_rows++;

  return ExecuteResult::SUCCESS;
}

ExecuteResult execute_select(const Statement &statement, Table &table, std::vector<Row> &out_vec) {
  for (uint32_t i = 0; i < table.num_rows; ++i) {
    Row row{};
    deserialize_row(table.row_slot(i), row);
    out_vec.emplace_back(row);
  }
  return ExecuteResult::SUCCESS;
}

ExecuteResult execute_statement(const Statement &statement, Table &table) {
  switch (statement.statement_type) {
    case (Statement::INSERT):
      return execute_insert(statement, table);
    case (Statement::SELECT):
      std::vector<Row> select_rows;
      ExecuteResult result = execute_select(statement, table, select_rows);
      for (auto &row : select_rows) {
        print_row(row);
      }
      return result;
  }
  return ExecuteResult::UNHANDLED_STATEMENT;
}