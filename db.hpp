//
// Created by Mike Loomis on 4/24/2019.
//

#ifndef CPPQLITE_DB_HPP
#define CPPQLITE_DB_HPP

#include <iostream>
#include <string>
#include <optional>
#include <array>
#include <vector>
#include <fstream>

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
  NEGATIVE_ID,
  STRING_TOO_LONG,
  SYNTAX_ERROR,
  UNRECOGNIZED_STATEMENT
};

struct Row {
  static const std::size_t COLUMN_USERNAME_SIZE = 32;
  static const std::size_t COLUMN_EMAIL_SIZE = 255;

  uint32_t id;
  std::array<char, COLUMN_USERNAME_SIZE + 1> username;
  std::array<char, COLUMN_EMAIL_SIZE + 1> email;
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

struct Page {
  bool cached;
  std::array<char, PAGE_SIZE> data;
};

struct Pager {
  std::fstream file;
  std::size_t file_length;
  std::array<Page, TABLE_MAX_PAGES> pages;

  explicit Pager(const std::string &filename)
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
  };

  Page &get_page(std::size_t page_num) {
    if (page_num > TABLE_MAX_PAGES) {
      std::cerr << "Tried to fetch page number out of bounds. " << page_num << " > " << TABLE_MAX_PAGES << '\n';
      exit(EXIT_FAILURE);
    }

    if (!pages[page_num].cached) {
      std::size_t num_pages = file_length / PAGE_SIZE;

      if (file_length % PAGE_SIZE) {
        num_pages++;
      }
      if (page_num <= num_pages) {
        file.seekg(page_num * PAGE_SIZE, std::fstream::beg);
        file.read(pages[page_num].data.data(), PAGE_SIZE);
        if (file.eof()) {
          file.clear();
        }
      }
      pages[page_num].cached = true;
    }
    return pages[page_num];
  }

  void flush(std::size_t page_num, std::size_t size) {
    if (!pages[page_num].cached) {
      std::cerr << "Tried to flush uncached page\n";
      exit(EXIT_FAILURE);
    }

    file.seekg(page_num * PAGE_SIZE, std::fstream::beg);

    file.write(pages[page_num].data.data(), size);
    pages[page_num].cached = false;
  }
};

struct Table {
  Pager pager;
  std::size_t num_rows;

  explicit Table(const std::string &filename)
      : pager(filename),
        num_rows(pager.file_length / ROW_SIZE) {};
};

void print_row(const Row &row);

void serialize_row(const Row &source, char *destination);

void deserialize_row(const char *source, Row &destination);

char *row_slot(Table &table, std::size_t row_num);

void db_close(Table &table);

MetaCommandResult do_meta_command(const std::string &command, Table &table);

PrepareResult prepare_statement(const std::string &input, Statement &out_statement);

ExecuteResult execute_insert(const Statement &statement, Table &table);

ExecuteResult execute_select(const Statement &statement, Table &table, std::vector<Row> &out_vec);

ExecuteResult execute_statement(const Statement &statement, Table &table);

#endif //CPPQLITE_DB_HPP
