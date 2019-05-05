//
// Created by Mike Loomis on 4/25/2019.
//

#include "db.hpp"

#include <cctype>
#include <algorithm>
#include <tuple>

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
  memcpy(destination + id_offset, &id, id_size);
  memcpy(destination + username_offset, username.data(), username_size);
  memcpy(destination + email_offset, email.data(), email_size);
}

CommonHeader::CommonHeader() : node_type(), is_root(), parent() {}

CommonHeader::CommonHeader(const Page &page)
    : node_type(),
      is_root(),
      parent() {
  node_type = *(CommonHeader::NodeType *) (page.data.data());
  is_root = *(bool *) (page.data.data() + CommonHeader::node_type_size);
  parent = *(Page **) (page.data.data() + CommonHeader::node_type_size + CommonHeader::is_root_size);
}

LeafHeader::LeafHeader() : common_header(), num_cells() {}

LeafHeader::LeafHeader(const Page &page)
    : common_header(page), num_cells() {
  num_cells = *(uint32_t *) (page.data.data() + CommonHeader::size);
}

LeafBody::LeafBody() : cells() {}

LeafBody::LeafBody(const Page &page, std::size_t num_cells)
    : cells() {
  for (auto i = 0U; i < num_cells; ++i) {
    cells[i].key = *(uint32_t *) (page.data.data() + LeafHeader::size + i * LeafCell::cell_size);
    cells[i].value = Row{page.data.data() + LeafHeader::size + LeafCell::key_size + i * LeafCell::cell_size};
  }
}

LeafNode::LeafNode()
    : header(),
      body() {
  header.common_header.node_type = CommonHeader::NodeType::LEAF;
}

LeafNode::LeafNode(const Page &page)
    : header(page),
      body(page, header.num_cells) {

  if (header.common_header.node_type != CommonHeader::NodeType::LEAF) {
    std::cerr << "Tried to read page with internal node type as a leaf node.\n";
    exit(EXIT_FAILURE);
  }
}

void LeafNode::serialize(char *dest) {
  memcpy(dest, &header.common_header.node_type, CommonHeader::node_type_size);
  memcpy(dest + CommonHeader::node_type_size, &header.common_header.is_root, CommonHeader::is_root_size);
  memcpy(dest + CommonHeader::node_type_size + CommonHeader::is_root_size,
         &header.common_header.parent,
         CommonHeader::parent_pointer_size);

  memcpy(dest + CommonHeader::size, &header.num_cells, LeafHeader::num_cells_size);

  for (auto i = 0U; i < header.num_cells; ++i) {
    memcpy(dest + LeafHeader::size + i * LeafCell::cell_size, &body.cells[i].key, LeafCell::key_size);
    body.cells[i].value.serialize(dest + LeafHeader::size + LeafCell::key_size + i * LeafCell::cell_size);
  }
}

void LeafNode::push_back(uint32_t key, const Row &row) {
  LeafCell cell{key, row};

  if (header.num_cells >= LeafBody::max_cells) {
    std::cerr << "Need to implement splitting a leaf node.\n";
    exit(EXIT_FAILURE);
  }

  body.cells[header.num_cells] = cell;
  header.num_cells++;
}

void LeafNode::insert(const Table::Cursor cursor, const uint32_t key, const Row &row) {
  LeafCell cell{key, row};

  if (header.num_cells >= LeafBody::max_cells) {
    std::cerr << "Need to implement splitting a leaf node.\n";
    exit(EXIT_FAILURE);
  }

  if (cursor.cell_num < header.num_cells) {
    for (auto i = header.num_cells; i > cursor.cell_num; i--) {
      body.cells[i] = body.cells[i - 1];
    }
  }

  header.num_cells++;
  body.cells[cursor.cell_num] = cell;
}

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
  if (page_num > Pager::MAX_PAGES) {
    std::cerr << "Tried to fetch page number out of bounds. " << page_num << " > " << Pager::MAX_PAGES << '\n';
    exit(EXIT_FAILURE);
  }

  if (!pages[page_num].cached) {
    if (page_num >= num_pages) {
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

void Pager::flush(std::size_t page_num) {
  if (!pages[page_num].cached) {
    std::cerr << "Tried to flush uncached page\n";
    exit(EXIT_FAILURE);
  }

  file.seekg(page_num * Page::PAGE_SIZE, std::fstream::beg);

  file.write(pages[page_num].data.data(), Page::PAGE_SIZE);
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
  auto &root_page = pager.get_page(0);
  if (!CommonHeader{root_page}.is_root) {
    auto node = LeafNode{};
    node.header.common_header.is_root = true;
    node.serialize(root_page.data.data());
  }
}

Row Table::Cursor::value(Table &table) {
  auto &page = table.pager.get_page(page_num);
  return LeafNode{page}.body.cells[cell_num].value;
}

void Table::Cursor::advance(Table &table) {
  auto &page = table.pager.get_page(page_num);
  auto node = LeafNode{page};
  cell_num++;
  if (cell_num >= node.header.num_cells) {
    end_of_table = true;
  }
}

Table::Cursor Table::table_start() {
  auto page_num = root_page_num;
  auto cell_num = 0U;
  auto root_page = pager.get_page(root_page_num);
  LeafNode node{root_page};
  auto end_of_table = node.header.num_cells == 0;
  return Table::Cursor{page_num, cell_num, end_of_table};
}

Table::Cursor Table::table_end() {
  auto page_num = root_page_num;
  auto root_page = pager.get_page(root_page_num);
  LeafNode node{root_page};
  auto cell_num = node.header.num_cells;
  return Table::Cursor{page_num, cell_num, true};
}

void Table::db_close() {
  for (std::size_t i = 0; i < pager.num_pages; i++) {
    if (pager.pages[i].cached) {
      pager.flush(i);
    }
  }

  pager.file.close();
}

Table::Cursor Table::find(uint32_t key) {
  auto &root_node = pager.get_page(root_page_num);

  if (CommonHeader{root_node}.node_type == CommonHeader::NodeType::LEAF) {
    return Cursor{root_page_num, LeafNode{root_node}.find(key)};
  } else {
    std::cerr << "Need to implement searching an internal node.\n";
    exit(EXIT_FAILURE);
  }
}

std::size_t LeafNode::find(uint32_t key) {
  uint32_t min_index = 0;
  uint32_t one_past_max_index = header.num_cells;
  while (one_past_max_index != min_index) {
    uint32_t index = (min_index + one_past_max_index) / 2;
    uint32_t key_at_index = body.cells[index].key;
    if (key == key_at_index) {
      return index;
    }
    if (key < key_at_index) {
      one_past_max_index = index;
    } else {
      min_index = index + 1;
    }
  }

  return min_index;
}

MetaCommandResult do_meta_command(const std::string &command, Table &table) {
  if (command == ".exit") {
    table.db_close();
    exit(EXIT_SUCCESS);
  } else if (command == ".btree") {
    std::cout << "Tree:\n";
    std::cout << LeafNode{table.pager.get_page(0)};
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
  auto &page = table.pager.get_page(table.root_page_num);
  auto node = LeafNode{page};
  if (node.header.num_cells >= LeafBody::max_cells) {
    return ExecuteResult::TABLE_FULL;
  }

  const Row &row_to_insert = statement.row_to_insert;
  auto key_to_insert = row_to_insert.id;
  auto cursor = table.find(key_to_insert);
  if (cursor.cell_num < node.header.num_cells) {
    auto key_at_index = node.body.cells[cursor.cell_num].key;
    if (key_at_index == key_to_insert) {
      return ExecuteResult::DUPLICATE_KEY;
    }
  }
  node.insert(cursor, row_to_insert.id, row_to_insert);
  node.serialize(page.data.data());

  return ExecuteResult::SUCCESS;
}

ExecuteResult execute_select(const Statement &statement, Table &table, std::vector<Row> &out_vec) {
  for (auto i = 0U; i < table.pager.num_pages; ++i) {
    auto &page = table.pager.get_page(i);
    auto node = LeafNode{page};
    for (auto j = 0U; j < node.header.num_cells; ++j) {
      out_vec.emplace_back(node.body.cells[j].value);
    }
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
        std::cout << row << '\n';
      }
      return result;
  }
  return ExecuteResult::UNHANDLED_STATEMENT;
}

void print_constants() {
  std::cout << "ROW_SIZE: " << Row::row_size << '\n';
  std::cout << "COMMON_NODE_HEADER_SIZE: " << CommonHeader::size << '\n';
  std::cout << "LEAF_NODE_HEADER_SIZE: " << LeafHeader::size << '\n';
  std::cout << "LEAF_NODE_CELL_SIZE: " << LeafCell::cell_size << '\n';
  std::cout << "LEAF_NODE_SPACE_FOR_CELLS: " << LeafBody::space_for_cells << '\n';
  std::cout << "LEAF_NODE_MAX_CELLS: " << LeafBody::max_cells << '\n';
}
