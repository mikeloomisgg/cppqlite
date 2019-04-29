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
  DUPLICATE_KEY,
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

struct Page {
  enum class NodeType {
    INTERNAL,
    LEAF
  };

  bool cached;
  std::array<char, PAGE_SIZE> data;

  explicit Page(NodeType type = NodeType::LEAF)
      : cached(),
        data() {
    set_node_type(type);
  };

  void set_node_type(NodeType type);
  NodeType get_node_type();
};

struct Pager {
  std::fstream file;
  std::size_t file_length;
  std::size_t num_pages;
  std::array<Page, TABLE_MAX_PAGES> pages;

  explicit Pager(const std::string &filename);

  Page &get_page(std::size_t page_num);

  void flush(std::size_t page_num);
};

struct Table {
  Pager pager;
  std::size_t root_page_num;

  explicit Table(const std::string &filename);


};

struct Cursor {
  Table &table;
  std::size_t page_num;
  std::size_t cell_num;
  bool end_of_table;

  char *value();
  void advance();
};

// Common node header layout
const uint32_t NODE_TYPE_SIZE = sizeof(uint8_t);
const uint32_t NODE_TYPE_OFFSET = 0;
const uint32_t IS_ROOT_SIZE = sizeof(uint8_t);
const uint32_t IS_ROOT_OFFSET = NODE_TYPE_SIZE;
const uint32_t PARENT_POINTER_SIZE = sizeof(uint32_t);
const uint32_t PARENT_POINTER_OFFSET = IS_ROOT_OFFSET + IS_ROOT_SIZE;
const uint32_t COMMON_NODE_HEADER_SIZE = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;

// Leaf node header layout
const uint32_t LEAF_NODE_NUM_CELLS_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE;

// Leaf node body layout
const uint32_t LEAF_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_KEY_OFFSET = 0;
const uint32_t LEAF_NODE_VALUE_SIZE = ROW_SIZE;
const uint32_t LEAF_NODE_VALUE_OFFSET = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
const uint32_t LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
const uint32_t LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_MAX_CELLS = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;

uint32_t *leaf_node_num_cells(Page &page);

char *leaf_node_cell(Page &page, std::size_t cell_num);

uint32_t *leaf_node_key(Page &page, std::size_t cell_num);

char *leaf_node_value(Page &page, std::size_t cell_num);

void leaf_node_insert(Cursor &cursor, uint32_t key, const Row &value);

void print_constants();

void print_leaf_node(Page &page);

void print_row(const Row &row);

void serialize_row(const Row &source, char *destination);

void deserialize_row(const char *source, Row &destination);

Cursor table_start(Table &table);

Cursor table_end(Table &table);

Cursor leaf_node_find(Table &table, std::size_t page_num, uint32_t key);

void db_close(Table &table);

MetaCommandResult do_meta_command(const std::string &command, Table &table);

PrepareResult prepare_statement(const std::string &input, Statement &out_statement);

ExecuteResult execute_insert(const Statement &statement, Table &table);

ExecuteResult execute_select(const Statement &statement, Table &table, std::vector<Row> &out_vec);

ExecuteResult execute_statement(const Statement &statement, Table &table);

#endif //CPPQLITE_DB_HPP
