#include "./include/PostgreSQLConnection.h"
#include "./include/PostgreSQLQuery.h"
#include "./include/PostgreSQLUtils.h"
#include <iostream>

int main() {
  try {
    PostgreSQLConnection connection;
    std::string conninfo = "host=localhost port=5432 dbname=avr \
                           user=avr password=1";
    if (!connection.connect(conninfo)) {
      std::cerr << "Failed to connect to database" << std::endl;
      return 1;
    }
    if (!PostgreSQLUtils::testConnection(connection)) {
      std::cerr << "Connection test failed!" << std::endl;
      return 1;
    }
    std::cout << "Database Info:\n"
              << PostgreSQLUtils::getDatabaseInfo(connection) << "\n\n";

    PostgreSQLQuery query(connection);
    connection.beginTransaction();
    try {
      if (!query.executeCommand("CREATE TABLE IF NOT EXISTS users ("
                                "id SERIAL PRIMARY KEY, "
                                "name VARCHAR(100) NOT NULL, "
                                "email VARCHAR(100) UNIQUE NOT NULL, "
                                "age INTEGER)")) {
        throw std::runtime_error("Failed to create table");
      }
      std::vector<std::string> insertParams = {"John Doe", "john@example.com",
                                               "30"};
      PGresult *insertResult = query.executeParams(
          "INSERT INTO users (name, email, age) VALUES ($1, $2, $3)",
          insertParams);
      if (!insertResult) {
        throw std::runtime_error("Failed to insert first user");
      }
      PQclear(insertResult);
      std::vector<std::string> insertParams2 = {"Jane Smith",
                                                "jane@example.com", "25"};
      PGresult *insertResult2 = query.executeParams(
          "INSERT INTO users (name, email, age) VALUES ($1, $2, $3)",
          insertParams2);
      if (!insertResult2) {
        throw std::runtime_error("Failed to insert second user");
      }
      PQclear(insertResult2);
      auto usersResult = PostgreSQLUtils::executeQuery(
          connection, "SELECT id, name, email, age FROM users ORDER BY id");

      std::cout << "All users:" << std::endl;
      PostgreSQLUtils::printResult(usersResult);
      int userCount = query.executeInt("SELECT COUNT(*) FROM users");
      std::cout << "Total users: " << userCount << std::endl;
      connection.commitTransaction();
      std::cout << "Transaction committed successfully!" << std::endl;

    } catch (const std::exception &e) {
      connection.rollbackTransaction();
      std::cerr << "Transaction failed: " << e.what() << std::endl;
      return 1;
    }
  } catch (const std::exception &e) {
    std::cerr << "Error: " << e.what() << std::endl;
    return 1;
  }
  return 0;
}
