#include "../include/PostgreSQLQuery.h"
#include <iostream>
#include <stdexcept>

PostgreSQLQuery::PostgreSQLQuery(PostgreSQLConnection &conn)
    : connection(conn) {
  if (!isConnectionOK()) {
    throw std::runtime_error("Database connection is not established");
  }
}

PGresult *PostgreSQLQuery::execute(const std::string &query) {
  if (!isConnectionOK()) {
    std::cerr << "Database connection is not OK" << std::endl;
    return nullptr;
  }
  if (query.empty()) {
    std::cerr << "Query cannot be empty" << std::endl;
    return nullptr;
  }
  PGconn *rawConn = connection.getRawConnection();
  PGresult *result = PQexec(rawConn, query.c_str());
  ExecStatusType status = PQresultStatus(result);
  if (status != PGRES_COMMAND_OK && status != PGRES_TUPLES_OK) {
    std::cerr << "Query failed (" << PQresStatus(status)
              << "): " << connection.getLastError() << std::endl;
    std::cerr << "Failed query: " << query << std::endl;
    PQclear(result);
    return nullptr;
  }
  return result;
}

PGresult *
PostgreSQLQuery::executeParams(const std::string &query,
                               const std::vector<std::string> &params) {
  if (!isConnectionOK()) {
    std::cerr << "Database connection is not OK" << std::endl;
    return nullptr;
  }
  if (query.empty()) {
    std::cerr << "Query cannot be empty" << std::endl;
    return nullptr;
  }
  std::vector<const char *> paramValues;
  for (const auto &param : params) {
    paramValues.push_back(param.c_str());
  }
  PGconn *rawConn = connection.getRawConnection();
  PGresult *result =
      PQexecParams(rawConn, query.c_str(), params.size(),
                   nullptr, // let server infer param types
                   paramValues.empty() ? nullptr : paramValues.data(),
                   nullptr, // param lengths (text means null)
                   nullptr, // param formats (text)
                   0);      // result format (0=text, 1=binary)

  ExecStatusType status = PQresultStatus(result);
  if (status != PGRES_COMMAND_OK && status != PGRES_TUPLES_OK) {
    std::cerr << "Parameterized query failed (" << PQresStatus(status)
              << "): " << connection.getLastError() << std::endl;
    std::cerr << "Failed query: " << query << std::endl;
    std::cerr << "Parameters count: " << params.size() << std::endl;
    PQclear(result);
    return nullptr;
  }

  return result;
}

PGresult *
PostgreSQLQuery::executeParams(const std::string &query,
                               const std::vector<const char *> &params) {
  if (!isConnectionOK()) {
    std::cerr << "Database connection is not OK" << std::endl;
    return nullptr;
  }
  PGconn *rawConn = connection.getRawConnection();
  PGresult *result = PQexecParams(
      rawConn, query.c_str(), params.size(), nullptr,
      params.empty() ? nullptr : params.data(), nullptr, nullptr, 0);
  ExecStatusType status = PQresultStatus(result);
  if (status != PGRES_COMMAND_OK && status != PGRES_TUPLES_OK) {
    std::cerr << "Parameterized query failed (" << PQresStatus(status)
              << "): " << connection.getLastError() << std::endl;
    PQclear(result);
    return nullptr;
  }

  return result;
}

PGresult *
PostgreSQLQuery::executePrepared(const std::string &stmtName,
                                 const std::vector<std::string> &params) {
  if (!isConnectionOK()) {
    std::cerr << "Database connection is not OK" << std::endl;
    return nullptr;
  }
  std::vector<const char *> paramValues;
  for (const auto &param : params) {
    paramValues.push_back(param.c_str());
  }
  PGconn *rawConn = connection.getRawConnection();
  PGresult *result = PQexecPrepared(
      rawConn, stmtName.c_str(), params.size(),
      paramValues.empty() ? nullptr : paramValues.data(), nullptr, nullptr, 0);

  ExecStatusType status = PQresultStatus(result);
  if (status != PGRES_COMMAND_OK && status != PGRES_TUPLES_OK) {
    std::cerr << "Prepared statement failed (" << PQresStatus(status)
              << "): " << connection.getLastError() << std::endl;
    PQclear(result);
    return nullptr;
  }

  return result;
}

bool PostgreSQLQuery::executeCommand(const std::string &query) {
  PGresult *result = execute(query);
  if (result) {
    PQclear(result);
    return true;
  }
  return false;
}

int PostgreSQLQuery::executeInt(const std::string &query, int defaultValue) {
  PGresult *result = execute(query);
  if (!result)
    return defaultValue;
  if (PQntuples(result) > 0 && PQnfields(result) > 0) {
    const char *value = PQgetvalue(result, 0, 0);
    if (value) {
      try {
        int resultValue = std::stoi(value);
        PQclear(result);
        return resultValue;
      } catch (const std::exception &e) {
        std::cerr << "Failed to convert result to int: " << e.what()
                  << std::endl;
      }
    }
  }
  PQclear(result);
  return defaultValue;
}

std::string PostgreSQLQuery::executeString(const std::string &query,
                                           const std::string &defaultValue) {
  PGresult *result = execute(query);
  if (!result)
    return defaultValue;
  if (PQntuples(result) > 0 && PQnfields(result) > 0) {
    const char *value = PQgetvalue(result, 0, 0);
    if (value) {
      std::string resultValue = value;
      PQclear(result);
      return resultValue;
    }
  }
  PQclear(result);
  return defaultValue;
}

bool PostgreSQLQuery::isConnectionOK() const { return connection.isOK(); }

std::string PostgreSQLQuery::getLastError() const {
  return connection.getLastError();
}
