#ifndef POSTGRESQL_CONNECTION_H
#define POSTGRESQL_CONNECTION_H

#include <libpq-fe.h>
#include <string>

class PostgreSQLConnection {
private:
  PGconn *connection;

public:
  PostgreSQLConnection();
  explicit PostgreSQLConnection(const std::string &conninfo);
  ~PostgreSQLConnection();
  PostgreSQLConnection(const PostgreSQLConnection &) = delete;
  PostgreSQLConnection &operator=(const PostgreSQLConnection &) = delete;
  PostgreSQLConnection(PostgreSQLConnection &&other) noexcept;
  PostgreSQLConnection &operator=(PostgreSQLConnection &&other) noexcept;

  bool connect(const std::string &conninfo);
  void disconnect();
  bool isConnected() const;
  bool isOK() const;
  PGconn *getRawConnection() const;
  std::string getLastError() const;
  bool beginTransaction();
  bool commitTransaction();
  bool rollbackTransaction();
  ConnStatusType getStatus() const;
};

#endif // POSTGRESQL_CONNECTION_H
