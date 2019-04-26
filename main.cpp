#include "db.hpp"

int main(int argc, char* argv[]) {
  if(argc < 2) {
    std::cerr << "Must supply a database filename.\n";
    exit(EXIT_FAILURE);
  }

  std::string filename = argv[1];
  Table table{filename};

  std::string input;
  while (true) {
    std::cout << "db > ";
    std::getline(std::cin, input);

    if (input[0] == '.') {
      switch (do_meta_command(input, table)) {
        case (MetaCommandResult::SUCCESS):
          continue;
        case (MetaCommandResult::UNRECOGNIZED_COMMAND):
          std::cout << "Unrecognized command: " << input << '\n';
          continue;
      }
    }
    Statement statement{};
    switch (prepare_statement(input, statement)) {
      case (PrepareResult::SUCCESS):
        break;
      case (PrepareResult::NEGATIVE_ID):
        std::cout << "ID must be positive.\n";
        continue;
      case (PrepareResult::STRING_TOO_LONG):
        std::cout << "String is too long.\n";
        continue;
      case (PrepareResult::SYNTAX_ERROR):
        std::cout << "Syntax error. Could not parse statement.\n";
        continue;
      case (PrepareResult::UNRECOGNIZED_STATEMENT):
        std::cout << "Unrecognized keyword at start of '" << input << "'.\n";
        continue;
    }

    switch (execute_statement(statement, table)) {
      case (ExecuteResult::SUCCESS):
        std::cout << "Executed.\n";
        break;
      case (ExecuteResult::TABLE_FULL):
        std::cout << "Error: Table full.\n";
        break;
      case (ExecuteResult::UNHANDLED_STATEMENT):
        std::cout << "Error: Unhandled statement.\n";
        break;
    }
  }
}