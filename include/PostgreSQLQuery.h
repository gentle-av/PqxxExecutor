#ifndef POSTGRESQL_QUERY_H
#define POSTGRESQL_QUERY_H

#include "PostgreSQLConnection.h"
#include <string>
#include <vector>

class PostgreSQLQuery {
private:
  PostgreSQLConnection &connection;

public:
  explicit PostgreSQLQuery(PostgreSQLConnection &conn);
  ~PostgreSQLQuery() = default;
  PostgreSQLQuery(const PostgreSQLQuery &) = delete;

  PostgreSQLQuery &operator=(const PostgreSQLQuery &) = delete;

  PGresult *execute(const std::string &query);
  PGresult *executeParams(const std::string &query,
                          const std::vector<std::string> &params);
  PGresult *executeParams(const std::string &query,
                          const std::vector<const char *> &params);
  PGresult *executePrepared(const std::string &stmtName,
                            const std::vector<std::string> &params);
  bool executeCommand(const std::string &query);
  int executeInt(const std::string &query, int defaultValue = 0);
  std::string executeString(const std::string &query,
                            const std::string &defaultValue = "");
  bool isConnectionOK() const;
  std::string getLastError() const;
};

#endif // POSTGRESQL_QUERY_H
