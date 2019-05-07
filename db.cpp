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

CommonHeader::CommonHeader() : node_type(), is_root(), parent_page_num() {}

CommonHeader::CommonHeader(const Page &page)
    : node_type(),
      is_root(),
      parent_page_num() {
  node_type = *(CommonHeader::NodeType *) (page.data.data());
  is_root = *(bool *) (page.data.data() + CommonHeader::node_type_size);
  parent_page_num = *(uint32_t *) (page.data.data() + CommonHeader::node_type_size + CommonHeader::is_root_size);
}

LeafHeader::LeafHeader() : common_header(), num_cells(), next_leaf_page_num() {}

LeafHeader::LeafHeader(const Page &page)
    : common_header(page), num_cells(), next_leaf_page_num() {
  num_cells = *(uint32_t *) (page.data.data() + CommonHeader::size);
  next_leaf_page_num = *(uint32_t *) (page.data.data() + CommonHeader::size + LeafHeader::num_cells_size);
}

LeafBody::LeafBody() : cells() {}

LeafBody::LeafBody(const Page &page, std::size_t num_cells)
    : cells() {
  for (auto i = 0U; i < num_cells; ++i) {
    cells[i].key = *(uint32_t *) (page.data.data() + LeafHeader::size + i * LeafCell::size);
    cells[i].value = Row{page.data.data() + LeafHeader::size + LeafCell::key_size + i * LeafCell::size};
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
         &header.common_header.parent_page_num,
         CommonHeader::parent_page_num_size);

  memcpy(dest + CommonHeader::size, &header.num_cells, LeafHeader::num_cells_size);
  memcpy(dest + CommonHeader::size + LeafHeader::num_cells_size,
         &header.next_leaf_page_num,
         LeafHeader::next_leaf_page_num_size);

  for (auto i = 0U; i < header.num_cells; ++i) {
    memcpy(dest + LeafHeader::size + i * LeafCell::size, &body.cells[i].key, LeafCell::key_size);
    body.cells[i].value.serialize(dest + LeafHeader::size + LeafCell::key_size + i * LeafCell::size);
  }
}

void LeafNode::insert(Table::Cursor cursor, const uint32_t key, const Row &row) {
  LeafCell cell{key, row};

  if (header.num_cells >= LeafBody::max_cells) {
    split_and_insert(cursor, key, row);
    return;
  }

  if (cursor.cell_num < header.num_cells) {
    for (auto i = header.num_cells; i > cursor.cell_num; i--) {
      body.cells[i] = body.cells[i - 1];
    }
  }

  header.num_cells++;
  body.cells[cursor.cell_num] = cell;
  serialize(cursor.page().data.data());
}

void create_new_root(Table &table, std::size_t right_child_page_num) {
  auto &root_page = table.pager.get_page(table.root_page_num);
  auto &right_page = table.pager.get_page(right_child_page_num);
  auto left_child_page_num = (uint32_t) table.pager.get_unused_page_num();
  auto &left_page = table.pager.get_page(left_child_page_num);
  left_page = root_page;
  LeafNode left_node{left_page};
  left_node.header.common_header.is_root = false;
  InternalNode new_root{};
  new_root.header.common_header.is_root = true;
  new_root.header.num_keys = 1;
  new_root.body.cells[0] = InternalCell{left_node.max_key(), left_child_page_num};
  new_root.header.right_child_page_num = (uint32_t) right_child_page_num;
  left_node.header.common_header.parent_page_num = (uint32_t) table.root_page_num;
  auto right_node = LeafNode{right_page};
  right_node.header.common_header.parent_page_num = (uint32_t) table.root_page_num;
  right_node.serialize(right_page.data.data());
  left_node.serialize(left_page.data.data());
  new_root.serialize(root_page.data.data());
}

void LeafNode::split_and_insert(Table::Cursor cursor, const uint32_t key, const Row &value) {
  auto &old_page = cursor.page();
  auto new_page_num = cursor.table.pager.get_unused_page_num();
  auto &new_page = cursor.table.pager.get_page(new_page_num);
  auto new_node = LeafNode{};
  new_node.serialize(new_page.data.data());
  auto old_node = LeafNode{old_page};
  auto old_max = old_node.max_key();
  new_node.header.num_cells = right_split_count;
  new_node.header.next_leaf_page_num = old_node.header.next_leaf_page_num;
  new_node.header.common_header.parent_page_num = old_node.header.common_header.parent_page_num;
  old_node.header.num_cells = left_split_count;
  old_node.header.next_leaf_page_num = (uint32_t) new_page_num;

  for (int i = LeafBody::max_cells; i >= 0; i--) {
    auto index_within_node = i % left_split_count;
    if (i >= LeafNode::left_split_count) {
      if (i == cursor.cell_num) {
        new_node.body.cells[index_within_node] = LeafCell{key, value};
      } else if (i > cursor.cell_num) {
        new_node.body.cells[index_within_node] = old_node.body.cells[i - 1];
      } else {
        new_node.body.cells[index_within_node] = old_node.body.cells[i];
      }
    } else {
      if (i == cursor.cell_num) {
        old_node.body.cells[index_within_node] = LeafCell{key, value};
      } else if (i > cursor.cell_num) {
        old_node.body.cells[index_within_node] = old_node.body.cells[i - 1];
      } else {
        old_node.body.cells[index_within_node] = old_node.body.cells[i];
      }
    }
  }

  old_node.serialize(old_page.data.data());
  new_node.serialize(new_page.data.data());

  if (old_node.header.common_header.is_root) {
    create_new_root(cursor.table, new_page_num);
  } else {
    auto parent_page_num = old_node.header.common_header.parent_page_num;
    auto new_max = old_node.max_key();
    auto &parent_page = cursor.table.pager.get_page(parent_page_num);
    auto parent_node = InternalNode{parent_page};
    parent_node.update_key(old_max, new_max);
    cursor.table.insert((uint32_t) parent_page_num, (uint32_t) new_page_num);
  }
}

InternalHeader::InternalHeader() : common_header(), num_keys(), right_child_page_num() {}

InternalHeader::InternalHeader(const Page &page)
    : common_header(page), num_keys(), right_child_page_num() {
  num_keys = *(uint32_t *) (page.data.data() + CommonHeader::size);
  right_child_page_num = *(uint32_t *) (page.data.data() + CommonHeader::size + InternalHeader::num_keys_size);
}

InternalBody::InternalBody() : cells() {}

InternalBody::InternalBody(const Page &page, std::size_t num_cells)
    : cells() {
  for (auto i = 0U; i < num_cells; ++i) {
    cells[i].key = *(uint32_t *) (page.data.data() + InternalHeader::size + i * InternalCell::size);
    cells[i].child_page_num =
        *(uint32_t *) (page.data.data() + InternalHeader::size + InternalCell::key_size + i * InternalCell::size);
  }
}

InternalNode::InternalNode()
    : header(),
      body() {
  header.common_header.node_type = CommonHeader::NodeType::INTERNAL;
}

InternalNode::InternalNode(const Page &page)
    : header(page),
      body(page, header.num_keys) {

  if (header.common_header.node_type != CommonHeader::NodeType::INTERNAL) {
    std::cerr << "Tried to read page with leaf node type as a internal node.\n";
    exit(EXIT_FAILURE);
  }
}

void InternalNode::serialize(char *dest) {
  memcpy(dest, &header.common_header.node_type, CommonHeader::node_type_size);
  memcpy(dest + CommonHeader::node_type_size, &header.common_header.is_root, CommonHeader::is_root_size);
  memcpy(dest + CommonHeader::node_type_size + CommonHeader::is_root_size,
         &header.common_header.parent_page_num,
         CommonHeader::parent_page_num_size);

  memcpy(dest + CommonHeader::size, &header.num_keys, InternalHeader::num_keys_size);
  memcpy(dest + CommonHeader::size + InternalHeader::num_keys_size,
         &header.right_child_page_num,
         InternalHeader::right_child_page_num_size);

  for (auto i = 0U; i < header.num_keys; ++i) {
    memcpy(dest + InternalHeader::size + i * InternalCell::size, &body.cells[i].key, InternalCell::key_size);
    memcpy(dest + InternalHeader::size + InternalCell::key_size + i * InternalCell::size,
           &body.cells[i].child_page_num,
           InternalCell::child_page_num_size);
  }
}

uint32_t InternalNode::max_key() const {
  return body.cells[header.num_keys - 1].key;
}

Table::Cursor InternalNode::find(Table &table, uint32_t page_num, uint32_t key) {
  auto page = table.pager.get_page(page_num);
  auto node = InternalNode{page};

  auto index = find_index(key);

  uint32_t child_page_num;
  if (index == header.num_keys) {
    child_page_num = node.header.right_child_page_num;
  } else {
    child_page_num = node.body.cells[index].child_page_num;
  }

  auto child_page = table.pager.get_page(child_page_num);
  switch (CommonHeader{child_page}.node_type) {
    case CommonHeader::NodeType::LEAF: {
      auto child_node = LeafNode{child_page};
      return Table::Cursor{table, child_page_num, child_node.find(key), child_node.header.next_leaf_page_num == 0};
    }
    case CommonHeader::NodeType::INTERNAL:
      return InternalNode{child_page}.find(table, child_page_num, key);
  }
}

void InternalNode::update_key(uint32_t old_key, uint32_t new_key) {
  auto old_child_index = find_index(old_key);
  if (old_child_index != header.num_keys) {
    body.cells[old_child_index].key = new_key;
  }
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

void indent(std::ostream &out, uint32_t level) {
  for (auto i = 0U; i < level; i++) {
    out << "  ";
  }
}

void Pager::print_tree(uint32_t page_num, uint32_t indentation_level) {
  auto &page = get_page(page_num);
  CommonHeader header{page};
  uint32_t num_keys;
  switch (header.node_type) {
    case CommonHeader::NodeType::LEAF: {
      LeafNode node{page};
      num_keys = node.header.num_cells;
      indent(std::cout, indentation_level);
      std::cout << "- leaf (size " << num_keys << ")\n";
      for (auto i = 0U; i < num_keys; i++) {
        indent(std::cout, indentation_level + 1);
        std::cout << "- " << node.body.cells[i].key << '\n';
      }
      break;
    }
    case CommonHeader::NodeType::INTERNAL: {
      InternalNode node{page};
      num_keys = node.header.num_keys;
      indent(std::cout, indentation_level);
      std::cout << "- internal (size " << num_keys << ")\n";
      for (auto i = 0U; i < num_keys; i++) {
        print_tree(node.body.cells[i].child_page_num, indentation_level + 1);
        indent(std::cout, indentation_level + 1);
        std::cout << "- key " << node.body.cells[i].key << '\n';
      }
      print_tree(node.header.right_child_page_num, indentation_level + 1);
      break;
    }
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

Row Table::Cursor::value() {
  auto &page = table.pager.get_page(page_num);
  return LeafNode{page}.body.cells[cell_num].value;
}

void Table::Cursor::advance() {
  auto &page = table.pager.get_page(page_num);
  cell_num++;
  auto node = LeafNode{page};
  if (cell_num >= node.header.num_cells) {
    auto next_page_num = node.header.next_leaf_page_num;
    if (next_page_num == 0) {
      end_of_table = true;
    } else {
      page_num = next_page_num;
      cell_num = 0;
    }
  }
}

Page &Table::Cursor::page() {
  return table.pager.get_page(page_num);
}

Table::Cursor Table::table_start() {
  return find(0);
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
  auto &root_page = pager.get_page(root_page_num);

  if (CommonHeader{root_page}.node_type == CommonHeader::NodeType::LEAF) {
    auto node = LeafNode{root_page};
    auto cell_num = node.find(key);
    return Cursor{*this, root_page_num, cell_num, cell_num == node.header.num_cells};
  } else {
    return InternalNode{root_page}.find(*this, (uint32_t) root_page_num, key);
  }
}

void Table::insert(uint32_t parent_page_num, uint32_t child_page_num) {
  auto &parent_page = pager.get_page(parent_page_num);
  auto parent_node = InternalNode{parent_page};
  auto &child_page = pager.get_page(child_page_num);
  auto child_max_key = LeafNode{child_page}.max_key();
  auto index = parent_node.find_index(child_max_key);
  auto original_num_keys = parent_node.header.num_keys;
  parent_node.header.num_keys++;

  if (original_num_keys >= InternalBody::max_cells) {
    std::cerr << "Need to implement splitting internal nodes.\n";
    exit(EXIT_FAILURE);
  }

  auto right_child_page_num = parent_node.header.right_child_page_num;
  auto &right_child_page = pager.get_page(right_child_page_num);
  auto right_child_node = LeafNode{right_child_page};

  if (child_max_key > right_child_node.max_key()) {
    parent_node.body.cells[original_num_keys] = InternalCell{right_child_node.max_key(), right_child_page_num};
    parent_node.header.right_child_page_num = child_page_num;
  } else {
    for (auto i = original_num_keys; i > index; i--) {
      parent_node.body.cells[i] = parent_node.body.cells[i - 1];
    }
    parent_node.body.cells[index] = InternalCell{child_max_key, child_page_num};
  }

  parent_node.serialize(parent_page.data.data());
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

uint32_t LeafNode::max_key() const {
  return body.cells[header.num_cells - 1].key;
}

MetaCommandResult do_meta_command(const std::string &command, Table &table) {
  if (command == ".exit") {
    table.db_close();
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
  const Row &row_to_insert = statement.row_to_insert;
  auto key_to_insert = row_to_insert.id;
  auto cursor = table.find(key_to_insert);

  auto node = LeafNode{cursor.page()};
  if (cursor.cell_num < node.header.num_cells) {
    auto key_at_index = node.body.cells[cursor.cell_num].key;
    if (key_at_index == key_to_insert) {
      return ExecuteResult::DUPLICATE_KEY;
    }
  }
  node.insert(cursor, row_to_insert.id, row_to_insert);

  return ExecuteResult::SUCCESS;
}

ExecuteResult execute_select(const Statement &statement, Table &table, std::vector<Row> &out_vec) {
  auto cursor = table.table_start();

  while (!cursor.end_of_table) {
    out_vec.emplace_back(cursor.value());
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
  std::cout << "LEAF_NODE_CELL_SIZE: " << LeafCell::size << '\n';
  std::cout << "LEAF_NODE_SPACE_FOR_CELLS: " << LeafBody::space_for_cells << '\n';
  std::cout << "LEAF_NODE_MAX_CELLS: " << LeafBody::max_cells << '\n';
}
