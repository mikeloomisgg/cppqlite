#include <iostream>
#include <string>
#include <optional>

bool do_meta_command(const std::string &command) {
  if (command == ".exit") {
    exit(EXIT_SUCCESS);
  }
  return false;
}

struct Statement {
  enum StatementType {
    INSERT,
    SELECT
  };

  StatementType statement_type;
};

enum class PrepareResult {
  SUCCESS,
  UNRECOGNIZED_STATEMENT
};

PrepareResult prepare_statement(const std::string &input, Statement &out_statement) {
  if (input.substr(0, 6) == "insert") {
    out_statement = Statement{Statement::INSERT};
    return PrepareResult::SUCCESS;
  }
  if (input.substr(0, 6) == "select") {
    out_statement = Statement{Statement::SELECT};
    return PrepareResult::SUCCESS;
  }
  return PrepareResult::UNRECOGNIZED_STATEMENT;
}

void execute_statement(const Statement &statement) {
  switch (statement.statement_type) {
    case (Statement::INSERT):
      std::clog << "This is where we would do an insert.\n";
      break;
    case (Statement::SELECT):
      std::clog << "This is where we would do a select.\n";
      break;
  }
}

int main() {
  std::string input;
  while (true) {
    std::cout << "db > ";
    std::getline(std::cin, input);

    if (input[0] == '.') {
      if (!do_meta_command(input)) {
        std::cout << "Unrecognized command: " << input << '\n';
        continue;
      }
    }
    Statement statement{};
    switch (prepare_statement(input, statement)) {
      case(PrepareResult::SUCCESS):
        break;
      case(PrepareResult::UNRECOGNIZED_STATEMENT):
        std::cout << "Unrecognized keyword at start of '" << input << "'.\n";
        continue;
    }

    execute_statement(statement);
    std::cout << "Executed.\n";
  }
}