//
// Created by Mike Loomis on 4/25/2019.
//

#include "db.hpp"

#include <cctype>
#include <algorithm>

Pager::Pager(const std::string &filename)
    : file(filename, std::ios::in | std::ios::out | std::ios::app | std::ios::binary),
      file_length(),
      num_pages(),
      pages() {
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
  if (page_num > TABLE_MAX_PAGES) {
    std::cerr << "Tried to fetch page number out of bounds. " << page_num << " > " << TABLE_MAX_PAGES << '\n';
    exit(EXIT_FAILURE);
  }

  if (!pages[page_num].cached) {
    if (page_num >= num_pages) {
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

void Pager::flush(std::size_t page_num) {
  if (!pages[page_num].cached) {
    std::cerr << "Tried to flush uncached page\n";
    exit(EXIT_FAILURE);
  }

  file.seekg(page_num * PAGE_SIZE, std::fstream::beg);

  file.write(pages[page_num].data.data(), PAGE_SIZE);
  pages[page_num].cached = false;
}

std::size_t Pager::get_unused_page_num() {
  return num_pages;
}

void indent(uint32_t level) {
  for (auto i = 0U; i < level; i++) {
    std::cout << "  ";
  }
}

void Pager::print_tree(uint32_t page_num, uint32_t indentation_level) {
  auto node = get_page(page_num);
  uint32_t num_keys, child;
  switch (node.node_type()) {
    case Page::NodeType::LEAF:
      num_keys = *node.num_cells();
      indent(indentation_level);
      std::cout << "- leaf (size " << num_keys << ")\n";
      for (auto i = 0U; i < num_keys; i++) {
        indent(indentation_level + 1);
        std::cout << "- " << *node.key(i) << '\n';
      }
      break;
    case Page::NodeType::INTERNAL:
      num_keys = *node.num_keys();
      indent(indentation_level);
      std::cout << "- internal (size " << num_keys << ")\n";
      for (auto i = 0U; i < num_keys; i++) {
        child = *node.child(i);
        print_tree(child, indentation_level + 1);
        indent(indentation_level + 1);
        std::cout << "- key " << *node.key(i) << '\n';
      }
      child = *node.right_child();
      print_tree(child, indentation_level + 1);
      break;
  }
}

Table::Table(const std::string &filename)
    : pager(filename),
      root_page_num(0) {
  pager.get_page(0).node_type(Page::NodeType::INTERNAL);
}

char *Cursor::value() {
  auto &page = table.pager.get_page(page_num);
  return page.value(cell_num);
}

void Cursor::advance() {
  auto node = table.pager.get_page(page_num);
  cell_num++;
  if (cell_num >= *node.num_cells()) {
    end_of_table = true;
  }
}

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

Cursor table_start(Table &table) {
  auto page_num = table.root_page_num;
  auto cell_num = 0U;
  auto root_node = table.pager.get_page(table.root_page_num);
  auto end_of_table = *root_node.num_keys() == 0;
  return Cursor{table, page_num, cell_num, end_of_table};
}

Cursor table_find(Table &table, uint32_t key) {
  auto root_page_num = table.root_page_num;
  auto &root_node = table.pager.get_page(root_page_num);

  if (root_node.node_type() == Page::NodeType::LEAF) {
    std::clog << "root node type was leaf..\n";
    return leaf_node_find(table, root_page_num, key);
  } else {
    std::cerr << "Need to implement searching an internal node.\n";
    exit(EXIT_FAILURE);
  }
}

Cursor leaf_node_find(Table &table, std::size_t page_num, uint32_t key) {
  auto &node = table.pager.get_page(page_num);
  auto num_cells = *node.num_cells();

  Cursor cursor{table, page_num};

  uint32_t min_index = 0;
  uint32_t one_past_max_index = num_cells;
  while (one_past_max_index != min_index) {
    uint32_t index = (min_index + one_past_max_index) / 2;
    uint32_t key_at_index = *node.key(index);
    if (key == key_at_index) {
      cursor.cell_num = index;
      return cursor;
    }
    if (key < key_at_index) {
      one_past_max_index = index;
    } else {
      min_index = index + 1;
    }
  }

  cursor.cell_num = min_index;
  return cursor;
}

Page::Page(NodeType type)
    : cached(),
      data() {
  node_type(type);
};

Page::NodeType Page::node_type() {
  uint8_t value = *(uint8_t *) (data.data() + NODE_TYPE_OFFSET);
  return static_cast<NodeType>(value);
}

void Page::node_type(NodeType type) {
  if (type == NodeType::LEAF) {
    root(false);
    *num_cells() = 0;
  } else {
    root(true);
    *num_keys() = 0;
  }
  *(uint8_t *) (data.data() + NODE_TYPE_OFFSET) = static_cast<uint8_t>(type);
}

uint32_t *Page::num_keys() {
  return (uint32_t *) (data.data() + INTERNAL_NODE_NUM_KEYS_OFFSET);
}

uint32_t *Page::num_cells() {
  return (uint32_t *) (data.data() + LEAF_NODE_NUM_CELLS_OFFSET);
}

char *Page::right_child() {
  return data.data() + INTERNAL_NODE_RIGHT_CHILD_SIZE;
}

char *Page::cell(std::size_t cell_num) {
  switch (node_type()) {
    case NodeType::INTERNAL:
      return data.data() + INTERNAL_NODE_HEADER_SIZE + cell_num * INTERNAL_NODE_CELL_SIZE;
    case NodeType::LEAF:
      std::clog << "Was a leaf...\n";
      return data.data() + LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE;
  }
}

char *Page::value(std::size_t cell_num) {
  return cell(cell_num) + LEAF_NODE_KEY_SIZE;
}

char *Page::child(uint32_t child_num) {
  if (child_num > *num_keys()) {
    std::cerr << "Tried to access child_num " << child_num << " > num_keys " << *num_keys() << '\n';
    exit(EXIT_FAILURE);
  } else if (child_num == *num_keys()) {
    return right_child();
  } else {
    return cell(child_num);
  }
}

uint32_t *Page::key(std::size_t cell_num) {
  switch (node_type()) {
    case Page::NodeType::INTERNAL:
      return (uint32_t *) (cell(cell_num) + INTERNAL_NODE_CHILD_SIZE);
    case Page::NodeType::LEAF:
      return (uint32_t *) cell(cell_num);
  }
}

uint32_t Page::max_key() {
  switch (node_type()) {
    case Page::NodeType::INTERNAL:
      return *key(*num_keys() - 1);
    case Page::NodeType::LEAF:
      return *key(*num_cells() - 1);
  }
}

bool Page::is_root() {
  uint8_t value = *(uint8_t *) (data.data() + IS_ROOT_OFFSET);
  return (bool) value;
}

void Page::root(bool is_root) {
  uint8_t value = is_root;
  *(uint8_t *) (data.data() + IS_ROOT_OFFSET) = value;
}

void db_close(Table &table) {
  for (std::size_t i = 0; i < table.pager.num_pages; i++) {
    if (table.pager.pages[i].cached) {
      table.pager.flush(i);
    }
  }

  table.pager.file.close();
}

MetaCommandResult do_meta_command(const std::string &command, Table &table) {
  if (command == ".exit") {
    db_close(table);
    exit(EXIT_SUCCESS);
  } else if (command == ".btree") {
    std::cout << "Tree:\n";
    table.pager.print_tree(0, 0);
    return MetaCommandResult::SUCCESS;
  } else if (command == ".constants") {
    std::cout << "Constants:\n";
    print_constants();
    return MetaCommandResult::SUCCESS;
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

void leaf_node_insert(Cursor &cursor, uint32_t key, const Row &value) {
  auto &node = cursor.table.pager.get_page(cursor.page_num);

  auto num_cells = *node.num_cells();
  if (num_cells >= LEAF_NODE_MAX_CELLS) {
//    leaf_node_split_and_insert(cursor, key, value);
    return;
  }

  if (cursor.cell_num < num_cells) {
    // Make room for new cell
    for (auto i = num_cells; i > cursor.cell_num; i--) {
      memcpy(node.cell(i), node.cell(i - 1), LEAF_NODE_CELL_SIZE);
    }
  }

  *node.num_cells() += 1;
  *node.key(cursor.cell_num) = key;
  serialize_row(value, node.cell(cursor.cell_num));
}

void create_new_root(Table &table, uint32_t right_child_page_num) {
  auto root = table.pager.get_page(table.root_page_num);
  auto right_child = table.pager.get_page(right_child_page_num);
  auto left_child_page_num = table.pager.get_unused_page_num();
  auto left_child = table.pager.get_page(left_child_page_num);
  memcpy(left_child.data.data(), root.data.data(), PAGE_SIZE);
  left_child.root(false);
  root.root(true);
  *root.num_keys() = 1;
  *root.child(0) = left_child_page_num;
  uint32_t left_child_max_key = left_child.max_key();
  *root.key(0) = left_child_max_key;
  *root.right_child() = right_child_page_num;
}

void leaf_node_split_and_insert(Cursor cursor, uint32_t key, const Row &value) {
  auto &old_node = cursor.table.pager.get_page(cursor.page_num);
  auto new_page_num = cursor.table.pager.get_unused_page_num();
  auto &new_node = cursor.table.pager.get_page(new_page_num);

  for (auto i = LEAF_NODE_MAX_CELLS; i >= 0; i--) {
    Page *destination_node;
    if (i >= LEAF_NODE_LEFT_SPLIT_COUNT) {
      destination_node = &new_node;
    } else {
      destination_node = &old_node;
    }
    uint32_t index_within_node = i % LEAF_NODE_LEFT_SPLIT_COUNT;
    char *destination = destination_node->cell(index_within_node);

    if (i == cursor.cell_num) {
      serialize_row(value, destination);
    } else if (i > cursor.cell_num) {
      memcpy(destination, old_node.cell(i - 1), LEAF_NODE_CELL_SIZE);
    } else {
      memcpy(destination, old_node.cell(i), LEAF_NODE_CELL_SIZE);
    }
  }
  *old_node.num_cells() = LEAF_NODE_LEFT_SPLIT_COUNT;
  *new_node.num_cells() = LEAF_NODE_RIGHT_SPLIT_COUNT;

  if (old_node.is_root()) {
    return create_new_root(cursor.table, new_page_num);
  } else {
    std::cerr << "Need to implement updating parent after split.\n";
    exit(EXIT_FAILURE);
  }
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
  auto &node = table.pager.get_page(table.root_page_num);
  auto num_cells = *node.num_cells();

  const Row &row_to_insert = statement.row_to_insert;
  auto key_to_insert = row_to_insert.id;
  auto cursor = table_find(table, key_to_insert);

  if (cursor.cell_num < num_cells) {
    auto key_at_index = *node.key(cursor.cell_num);
    if (key_at_index == key_to_insert) {
      return ExecuteResult::DUPLICATE_KEY;
    }
  }
  leaf_node_insert(cursor, row_to_insert.id, row_to_insert);

  return ExecuteResult::SUCCESS;
}

ExecuteResult execute_select(const Statement &statement, Table &table, std::vector<Row> &out_vec) {
  auto cursor = table_start(table);
  while (!cursor.end_of_table) {
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
        print_row(row);
      }
      return result;
  }
  return ExecuteResult::UNHANDLED_STATEMENT;
}

void print_constants() {
  std::cout << "ROW_SIZE: " << ROW_SIZE << '\n';
  std::cout << "COMMON_NODE_HEADER_SIZE: " << COMMON_NODE_HEADER_SIZE << '\n';
  std::cout << "LEAF_NODE_HEADER_SIZE: " << LEAF_NODE_HEADER_SIZE << '\n';
  std::cout << "LEAF_NODE_CELL_SIZE: " << LEAF_NODE_CELL_SIZE << '\n';
  std::cout << "LEAF_NODE_SPACE_FOR_CELLS: " << LEAF_NODE_SPACE_FOR_CELLS << '\n';
  std::cout << "LEAF_NODE_MAX_CELLS: " << LEAF_NODE_MAX_CELLS << '\n';
}