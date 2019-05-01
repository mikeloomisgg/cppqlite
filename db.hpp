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

  static constexpr std::size_t row_size() { return id_size() + username_size() + email_size(); };

  static constexpr std::size_t id_size() { return sizeof(id); }

  static constexpr std::size_t id_offset() { return 0; }

  static constexpr std::size_t username_size() { return sizeof(username); }

  static constexpr std::size_t username_offset() { return id_size(); }

  static constexpr std::size_t email_size() { return sizeof(email); }

  static constexpr std::size_t email_offset() { return id_size() + username_size(); }

  uint32_t id;
  std::array<char, COLUMN_USERNAME_SIZE + 1> username;
  std::array<char, COLUMN_EMAIL_SIZE + 1> email;

  friend std::ostream &operator<<(std::ostream &out, const Row &s) {
    out << "(" << s.id << ", " << s.username.data() << ", " << s.email.data() << ")\n";
    return out;
  };

  Row() = default;
  explicit Row(const char *source);

  Row(uint32_t id, std::string username, std::string email);

  void serialize(char *destination) const;
};

struct Statement {
  enum StatementType {
    INSERT,
    SELECT
  };

  StatementType statement_type;
  Row row_to_insert; // only used by insert statement
};

struct Page {
  static const uint32_t PAGE_SIZE = 4096;

  static constexpr std::size_t rows_per_page() { return PAGE_SIZE / Row::row_size(); }

  bool cached;
  std::array<char, PAGE_SIZE> data;
};

struct Pager {
  static const uint32_t MAX_PAGES = 100;

  std::fstream file;
  std::size_t file_length;
  std::array<Page, MAX_PAGES> pages;

  explicit Pager(const std::string &filename);

  Page &get_page(std::size_t page_num);

  void flush(std::size_t page_num, std::size_t size);
};

struct Table {
  static constexpr std::size_t max_rows() { return Pager::MAX_PAGES * Page::rows_per_page(); }

  Pager pager;
  std::size_t num_rows;

  explicit Table(const std::string &filename);

  char *row_slot(std::size_t row_num);

  void db_close();
};

MetaCommandResult do_meta_command(const std::string &command, Table &table);

PrepareResult prepare_statement(const std::string &input, Statement &out_statement);

ExecuteResult execute_insert(const Statement &statement, Table &table);

ExecuteResult execute_select(const Statement &statement, Table &table, std::vector<Row> &out_vec);

ExecuteResult execute_statement(const Statement &statement, Table &table);

#endif //CPPQLITE_DB_HPP
