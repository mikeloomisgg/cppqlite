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

std::optional<Statement> prepare_statement(const std::string &input) {
  if (input.substr(0, 6) == "insert") {
    return Statement{Statement::INSERT};
  }
  if (input.substr(0, 6) == "select") {
    return Statement{Statement::SELECT};
  }
  return std::nullopt;
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
    auto statement = prepare_statement(input);
    if (statement) {
      execute_statement(*statement);
      std::cout << "Executed.\n";
    } else {
      std::cout << "Inrecognized keyword at start of '" << input << "'.\n";
    }
  }
}