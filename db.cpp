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
  memcpy(destination + id_offset, &id, id_size);
  memcpy(destination + username_offset, username.data(), username_size);
  memcpy(destination + email_offset, email.data(), email_size);
}

Page::CommonHeader::CommonHeader() : node_type(), is_root(), parent() {}

Page::CommonHeader::CommonHeader(const Page &page)
    : node_type(),
      is_root(),
      parent() {
  node_type = *(Page::CommonHeader::NodeType *) (page.data.data());
  is_root = *(bool *) (page.data.data() + CommonHeader::node_type_size);
  parent = *(Page **) (page.data.data() + CommonHeader::node_type_size + CommonHeader::is_root_size);
}

Page::LeafHeader::LeafHeader() : common_header(), num_cells() {}

Page::LeafHeader::LeafHeader(const Page &page)
    : common_header(page), num_cells() {
  num_cells = *(uint32_t *) (page.data.data() + CommonHeader::size);
}

Page::LeafBody::LeafBody() : cells() {}

Page::LeafBody::LeafBody(const Page &page, std::size_t num_cells)
    : cells() {
  for (auto i = 0U; i < num_cells; ++i) {
    cells[i].key = *(uint32_t *) (page.data.data() + LeafHeader::size + i * LeafCell::cell_size);
    cells[i].value = Row{page.data.data() + LeafHeader::size + LeafCell::key_size + i * LeafCell::cell_size};
  }
}

Page::LeafNode::LeafNode()
    : header(),
      body() {
  header.common_header.node_type = CommonHeader::NodeType::LEAF;
}

Page::LeafNode::LeafNode(const Page &page)
    : header(page),
      body(page, header.num_cells) {

  if (header.common_header.node_type != CommonHeader::NodeType::LEAF) {
    std::cerr << "Tried to read page with internal node type as a leaf node.\n";
    exit(EXIT_FAILURE);
  }
}

void Page::LeafNode::serialize(char *dest) {
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

void Page::LeafNode::insert(uint32_t key, const Row &row) {
  Page::LeafCell cell{key, row};

  if (header.num_cells >= Page::LeafBody::max_cells) {
    std::cerr << "Need to implement splitting a leaf node.\n";
    exit(EXIT_FAILURE);
  }

  body.cells[header.num_cells] = cell;
  header.num_cells++;
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

Table::Table(const std::string &filename)
    : pager(filename),
      root_page_num(0) {
  auto &root_page = pager.get_page(0);
  if (!Page::CommonHeader{root_page}.is_root) {
    auto node = Page::LeafNode{};
    node.header.common_header.is_root = true;
    node.serialize(root_page.data.data());
  }
}

void Table::db_close() {
  for (std::size_t i = 0; i < pager.num_pages; i++) {
    if (pager.pages[i].cached) {
      pager.flush(i);
    }
  }

  pager.file.close();
}

MetaCommandResult do_meta_command(const std::string &command, Table &table) {
  if (command == ".exit") {
    table.db_close();
    exit(EXIT_SUCCESS);
  } else if (command == ".btree") {
    std::cout << "Tree:\n";
    std::cout << Page::LeafNode{table.pager.get_page(0)};
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
  auto node = Page::LeafNode{page};
  if (node.header.num_cells >= Page::LeafBody::max_cells) {
    return ExecuteResult::TABLE_FULL;
  }

  const Row &row_to_insert = statement.row_to_insert;
  node.insert(row_to_insert.id, row_to_insert);
  node.serialize(page.data.data());

  return ExecuteResult::SUCCESS;
}

ExecuteResult execute_select(const Statement &statement, Table &table, std::vector<Row> &out_vec) {
  for (auto i = 0U; i < table.pager.num_pages; ++i) {
    auto &page = table.pager.get_page(i);
    auto node = Page::LeafNode{page};
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
  std::cout << "COMMON_NODE_HEADER_SIZE: " << Page::CommonHeader::size << '\n';
  std::cout << "LEAF_NODE_HEADER_SIZE: " << Page::LeafHeader::size << '\n';
  std::cout << "LEAF_NODE_CELL_SIZE: " << Page::LeafCell::cell_size << '\n';
  std::cout << "LEAF_NODE_SPACE_FOR_CELLS: " << Page::LeafBody::space_for_cells << '\n';
  std::cout << "LEAF_NODE_MAX_CELLS: " << Page::LeafBody::max_cells << '\n';
}
