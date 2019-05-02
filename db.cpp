//
// Created by Mike Loomis on 4/25/2019.
//

#include "db.hpp"

#include <cctype>
#include <algorithm>

Row::Row(const char *source) : id(), username(), email() {
  id = *(uint32_t *) (source);
  username = *(std::array<char, COLUMN_USERNAME_SIZE + 1> *) (source + sizeof(id));
  email = *(std::array<char, COLUMN_EMAIL_SIZE + 1> *) (source + sizeof(id) + sizeof(username));
}

Row::Row(uint32_t id, std::string u, std::string e)
    : id(id),
      username(),
      email() {
  std::copy(u.begin(), u.end(), username.data());
  std::copy(e.begin(), e.end(), email.data());
}

void Row::serialize(char *destination) const {
  memcpy(destination + id_offset(), &id, id_size());
  memcpy(destination + username_offset(), username.data(), username_size());
  memcpy(destination + email_offset(), email.data(), email_size());
}

Pager::Pager(const std::string &filename)
    : file(filename, std::ios::in | std::ios::out | std::ios::app | std::ios::binary), file_length(), pages() {
  file.close();
  file.open(filename, std::ios::in | std::ios::out | std::ios::binary);
  if (!file) {
    std::cerr << "Unable to open file.\n";
    exit(EXIT_FAILURE);
  }
  file.seekg(0, std::fstream::end);
  file_length = file.tellg();
  file.seekg(0, std::fstream::beg);
}

Page &Pager::get_page(std::size_t page_num) {
  if (page_num > Pager::MAX_PAGES) {
    std::cerr << "Tried to fetch page number out of bounds. " << page_num << " > " << Pager::MAX_PAGES << '\n';
    exit(EXIT_FAILURE);
  }

  if (!pages[page_num].cached) {
    std::size_t num_pages = file_length / Page::PAGE_SIZE;

    if (file_length % Page::PAGE_SIZE) {
      num_pages++;
    }
    if (page_num <= num_pages) {
      file.seekg(page_num * Page::PAGE_SIZE, std::fstream::beg);
      file.read(pages[page_num].data.data(), Page::PAGE_SIZE);
      if (file.eof()) {
        file.clear();
      }
    }
    pages[page_num].cached = true;
  }
  return pages[page_num];
}

void Pager::flush(std::size_t page_num, std::size_t size) {
  if (!pages[page_num].cached) {
    std::cerr << "Tried to flush uncached page\n";
    exit(EXIT_FAILURE);
  }

  file.seekg(page_num * Page::PAGE_SIZE, std::fstream::beg);

  file.write(pages[page_num].data.data(), size);
  pages[page_num].cached = false;
}

Table::Table(const std::string &filename)
    : pager(filename),
      num_rows(pager.file_length / Row::row_size()) {}

void Cursor::advance() {
  row_num++;
  if(row_num >= table.num_rows) {
    end_of_table = true;
  }
}

char *Cursor::value() {
  const std::size_t page_num = row_num / ROWS_PER_PAGE;
  auto &page = table.pager.get_page(page_num);
  uint32_t row_offset = row_num % ROWS_PER_PAGE;
  uint32_t byte_offset = row_offset * ROW_SIZE;
  return page.data.data() + byte_offset;
}

char *Table::row_slot(const std::size_t row_num) {
  const std::size_t page_num = row_num / Page::rows_per_page();
  auto &page = pager.get_page(page_num);
  std::size_t row_offset = row_num % Page::rows_per_page();
  std::size_t byte_offset = row_offset * Row::row_size();
  return page.data.data() + byte_offset;
}

Cursor table_start(Table &table) {
  return Cursor{table, 0, table.num_rows == 0};
}

Cursor table_end(Table &table) {
  return Cursor{table, table.num_rows, true};
}

void Table::db_close() {
  std::size_t num_full_pages = num_rows / Page::rows_per_page();
  for (std::size_t i = 0; i < num_full_pages; ++i) {
    if (pager.pages[i].cached) {
      pager.flush(i, Page::PAGE_SIZE);
    }
  }
  std::size_t num_additional_rows = num_rows % Page::rows_per_page();
  if (num_additional_rows > 0) {
    auto page_num = num_full_pages;
    if (pager.pages[page_num].cached) {
      pager.flush(page_num, num_additional_rows * Row::row_size());
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
    if (tokens[2].size() >= sizeof(Row::username) || tokens[3].size() >= sizeof(Row::email)) {
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
  if (table.num_rows >= Table::max_rows()) {
    return ExecuteResult::TABLE_FULL;
  }

  statement.row_to_insert.serialize(table.row_slot(table.num_rows));
  const Row &row_to_insert = statement.row_to_insert;
  auto cursor = table_end(table);
  serialize_row(row_to_insert, cursor.value());
  table.num_rows++;

  return ExecuteResult::SUCCESS;
}

ExecuteResult execute_select(const Statement &statement, Table &table, std::vector<Row> &out_vec) {
  for (uint32_t i = 0; i < table.num_rows; ++i) {
    Row row(table.row_slot(i));
  auto cursor = table_start(table);
  while(!cursor.end_of_table) {
    Row row{};
    deserialize_row(cursor.value(), row);
    out_vec.emplace_back(row);
    cursor.advance();
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
        std::cout << row;
      }
      return result;
  }
  return ExecuteResult::UNHANDLED_STATEMENT;
}