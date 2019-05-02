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
}

char *Table::Cursor::value() {
  auto &page = table.pager.get_page(page_num);
  return leaf_node_value(page, cell_num);
}

void Table::Cursor::advance() {
  auto node = table.pager.get_page(page_num);
  cell_num++;
  if (cell_num >= *leaf_node_num_cells(node)) {
    end_of_table = true;
  }
}

Table::Cursor Table::table_start() {
  auto page_num = root_page_num;
  auto cell_num = 0U;
  auto root_node = pager.get_page(root_page_num);
  auto end_of_table = *leaf_node_num_cells(root_node) == 0;
  return Cursor{*this, page_num, cell_num, end_of_table};
}

Table::Cursor Table::table_end() {
  auto page_num = root_page_num;
  auto root_node = pager.get_page(root_page_num);
  auto cell_num = *leaf_node_num_cells(root_node);
  return Cursor{*this, page_num, cell_num, true};
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
    print_leaf_node(table.pager.get_page(0));
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

void leaf_node_insert(Table::Cursor &cursor, uint32_t key, const Row &value) {
  auto &node = cursor.table.pager.get_page(cursor.page_num);

  auto num_cells = *leaf_node_num_cells(node);
  if (num_cells >= LEAF_NODE_MAX_CELLS) {
    std::cerr << "Need to implement splitting a leaf node.\n";
    exit(EXIT_FAILURE);
  }

  if (cursor.cell_num < num_cells) {
    // Make room for new cell
    for (auto i = num_cells; i > cursor.cell_num; i--) {
      memcpy(leaf_node_cell(node, i), leaf_node_cell(node, i - 1), LEAF_NODE_CELL_SIZE);
    }
  }

  *leaf_node_num_cells(node) += 1;
  *leaf_node_key(node, cursor.cell_num) = key;
  serialize_row(value, leaf_node_value(node, cursor.cell_num));
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
  auto &node = table.pager.get_page(table.root_page_num);
  if (*leaf_node_num_cells(node) >= LEAF_NODE_MAX_CELLS) {
    return ExecuteResult::TABLE_FULL;
  }

  auto cursor = table.table_end();
  const Row &row_to_insert = statement.row_to_insert;
  leaf_node_insert(cursor, row_to_insert.id, row_to_insert);

  return ExecuteResult::SUCCESS;
}

ExecuteResult execute_select(const Statement &statement, Table &table, std::vector<Row> &out_vec) {
  auto cursor = table.table_start();
  while (!cursor.end_of_table) {
    Row row{cursor.value()};
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

uint32_t *leaf_node_num_cells(Page &page) {
  return (uint32_t *) (page.data.data() + LEAF_NODE_NUM_CELLS_OFFSET);
}

char *leaf_node_cell(Page &page, std::size_t cell_num) {
  return page.data.data() + LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE;
}

uint32_t *leaf_node_key(Page &page, std::size_t cell_num) {
  return (uint32_t *) leaf_node_cell(page, cell_num);
}

char *leaf_node_value(Page &page, std::size_t cell_num) {
  return leaf_node_cell(page, cell_num) + LEAF_NODE_KEY_SIZE;
}

void print_constants() {
  std::cout << "ROW_SIZE: " << Row::row_size() << '\n';
  std::cout << "COMMON_NODE_HEADER_SIZE: " << COMMON_NODE_HEADER_SIZE << '\n';
  std::cout << "LEAF_NODE_HEADER_SIZE: " << LEAF_NODE_HEADER_SIZE << '\n';
  std::cout << "LEAF_NODE_CELL_SIZE: " << LEAF_NODE_CELL_SIZE << '\n';
  std::cout << "LEAF_NODE_SPACE_FOR_CELLS: " << LEAF_NODE_SPACE_FOR_CELLS << '\n';
  std::cout << "LEAF_NODE_MAX_CELLS: " << LEAF_NODE_MAX_CELLS << '\n';
}

void print_leaf_node(Page &page) {
  auto num_cells = *leaf_node_num_cells(page);
  std::cout << "leaf (size " << num_cells << ")\n";
  for (auto i = 0U; i < num_cells; ++i) {
    auto key = *leaf_node_key(page, i);
    std::cout << "  - " << i << " : " << key << '\n';
  }
}
