#include "../include/PostgreSQLConnection.h"
#include <iostream>

PostgreSQLConnection::PostgreSQLConnection() : connection(nullptr) {}

PostgreSQLConnection::PostgreSQLConnection(const std::string &conninfo)
    : connection(nullptr) {
  connect(conninfo);
}

PostgreSQLConnection::~PostgreSQLConnection() { disconnect(); }

PostgreSQLConnection::PostgreSQLConnection(
    PostgreSQLConnection &&other) noexcept
    : connection(other.connection) {
  other.connection = nullptr;
}

PostgreSQLConnection &
PostgreSQLConnection::operator=(PostgreSQLConnection &&other) noexcept {
  if (this != &other) {
    disconnect();
    connection = other.connection;
    other.connection = nullptr;
  }
  return *this;
}

bool PostgreSQLConnection::connect(const std::string &conninfo) {
  disconnect();
  connection = PQconnectdb(conninfo.c_str());
  if (PQstatus(connection) != CONNECTION_OK) {
    std::cerr << "Connection failed: " << PQerrorMessage(connection)
              << std::endl;
    PQfinish(connection);
    connection = nullptr;
    return false;
  }
  std::cout << "Connected to database successfully!" << std::endl;
  return true;
}

void PostgreSQLConnection::disconnect() {
  if (connection) {
    PQfinish(connection);
    connection = nullptr;
  }
}

bool PostgreSQLConnection::isConnected() const { return connection != nullptr; }

bool PostgreSQLConnection::isOK() const {
  return connection && PQstatus(connection) == CONNECTION_OK;
}

PGconn *PostgreSQLConnection::getRawConnection() const { return connection; }

std::string PostgreSQLConnection::getLastError() const {
  if (connection) {
    return PQerrorMessage(connection);
  }
  return "No connection established";
}

bool PostgreSQLConnection::beginTransaction() {
  if (!isOK())
    return false;
  PGresult *result = PQexec(connection, "BEGIN");
  bool success = (PQresultStatus(result) == PGRES_COMMAND_OK);
  PQclear(result);
  return success;
}

bool PostgreSQLConnection::commitTransaction() {
  if (!isOK())
    return false;
  PGresult *result = PQexec(connection, "COMMIT");
  bool success = (PQresultStatus(result) == PGRES_COMMAND_OK);
  PQclear(result);
  return success;
}

bool PostgreSQLConnection::rollbackTransaction() {
  if (!isOK())
    return false;
  PGresult *result = PQexec(connection, "ROLLBACK");
  bool success = (PQresultStatus(result) == PGRES_COMMAND_OK);
  PQclear(result);
  return success;
}

ConnStatusType PostgreSQLConnection::getStatus() const {
  return connection ? PQstatus(connection) : CONNECTION_BAD;
}
