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

  static const std::size_t id_size = sizeof(id);
  static const std::size_t id_offset = 0;
  static const std::size_t username_size = sizeof(username);
  static const std::size_t username_offset = id_size;
  static const std::size_t email_size = sizeof(email);
  static const std::size_t email_offset = id_size + username_size;
  static const std::size_t row_size = id_size + username_size + email_size;

  friend std::ostream &operator<<(std::ostream &out, const Row &s) {
    out << "(" << s.id << ", " << s.username.data() << ", " << s.email.data() << ")";
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

  bool cached;
  std::array<char, PAGE_SIZE> data;
};

struct Pager {
  static const uint32_t MAX_PAGES = 100;

  std::fstream file;
  std::size_t file_length;
  std::array<Page, MAX_PAGES> pages;
  std::size_t num_pages;

  explicit Pager(const std::string &filename);

  Page &get_page(std::size_t page_num);

  void flush(std::size_t page_num);

  std::size_t get_unused_page_num();

  void print_tree(uint32_t page_num, uint32_t indentation_level);
};

struct Table {
  struct Cursor {
    std::size_t page_num;
    std::size_t cell_num;
    bool end_of_table;

    void advance(Table &table);
    Row value(Table &table);
  };

  Pager pager;
  std::size_t root_page_num;

  explicit Table(const std::string &filename);

  Cursor table_start();

  Cursor table_end();

  Cursor find(uint32_t key);

  void db_close();
};

struct CommonHeader {
  enum class NodeType : uint8_t {
    INTERNAL,
    LEAF
  };
  NodeType node_type;
  bool is_root;
  Page *parent;

  static const std::size_t node_type_size = sizeof(node_type);
  static const std::size_t is_root_size = sizeof(is_root);
  static const std::size_t parent_pointer_size = sizeof(parent);
  static const std::size_t size = node_type_size + is_root_size + parent_pointer_size;

  CommonHeader();
  explicit CommonHeader(const Page &page);
};

struct LeafHeader {
  CommonHeader common_header;
  uint32_t num_cells;

  static const std::size_t num_cells_size = sizeof(num_cells);
  static const std::size_t size = CommonHeader::size + num_cells_size;

  LeafHeader();
  explicit LeafHeader(const Page &page);
};

struct LeafCell {
  uint32_t key;
  Row value;

  static const std::size_t key_size = sizeof(key);
  static const std::size_t value_size = Row::row_size;
  static const std::size_t cell_size = key_size + value_size;
};

struct LeafBody {
  static const std::size_t space_for_cells = Page::PAGE_SIZE - LeafHeader::size;
  static const std::size_t max_cells = space_for_cells / LeafCell::cell_size;

  std::array<LeafCell, max_cells> cells;

  LeafBody();
  explicit LeafBody(const Page &page, std::size_t num_cells);
};

struct LeafNode {
  LeafHeader header;
  LeafBody body;

  friend std::ostream &operator<<(std::ostream &out, const LeafNode &s) {
    out << "leaf (size " << s.header.num_cells << ")\n";
    for (auto i = 0U; i < s.header.num_cells; ++i) {
      out << "  - " << i << " : " << s.body.cells[i].key << '\n';
    }
    return out;
  };

  LeafNode();
  explicit LeafNode(const Page &page);

  void serialize(char *destination);

  void push_back(uint32_t key, const Row &row);

  void insert(Table::Cursor cursor, uint32_t key, const Row &row);

  std::size_t find(uint32_t key);
};

// Leaf node body layout
const uint32_t LEAF_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_KEY_OFFSET = 0;
const uint32_t LEAF_NODE_VALUE_SIZE = ROW_SIZE;
const uint32_t LEAF_NODE_VALUE_OFFSET = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
const uint32_t LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
const uint32_t LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_MAX_CELLS = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;
const uint32_t LEAF_NODE_RIGHT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) / 2;
const uint32_t LEAF_NODE_LEFT_SPLIT_COUNT = LEAF_NODE_MAX_CELLS + 1 - LEAF_NODE_RIGHT_SPLIT_COUNT;

// Internal node header layout
const uint32_t INTERNAL_NODE_NUM_KEYS_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_NUM_KEYS_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint32_t INTERNAL_NODE_RIGHT_CHILD_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_RIGHT_CHILD_OFFSET = INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE;
const uint32_t INTERNAL_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE +
    INTERNAL_NODE_NUM_KEYS_SIZE + INTERNAL_NODE_RIGHT_CHILD_SIZE;

// Internal node body layout
const uint32_t INTERNAL_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_CHILD_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_CELL_SIZE = INTERNAL_NODE_CHILD_SIZE + INTERNAL_NODE_KEY_SIZE;

void leaf_node_split_and_insert(Cursor, uint32_t key, const Row &value);

void print_constants();

MetaCommandResult do_meta_command(const std::string &command, Table &table);

PrepareResult prepare_statement(const std::string &input, Statement &out_statement);

ExecuteResult execute_insert(const Statement &statement, Table &table);

ExecuteResult execute_select(const Statement &statement, Table &table, std::vector<Row> &out_vec);

ExecuteResult execute_statement(const Statement &statement, Table &table);

#endif //CPPQLITE_DB_HPP
