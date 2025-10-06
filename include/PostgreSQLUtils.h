#ifndef POSTGRESQL_UTILS_H
#define POSTGRESQL_UTILS_H

#include "PostgreSQLConnection.h"
#include <iostream>
#include <map>
#include <string>
#include <vector>

// RAII обёртка для PGresult
class PGResultWrapper {
private:
  PGresult *result;

public:
  explicit PGResultWrapper(PGresult *res = nullptr) : result(res) {}
  ~PGResultWrapper();
  PGResultWrapper(const PGResultWrapper &) = delete;
  PGResultWrapper &operator=(const PGResultWrapper &) = delete;
  PGResultWrapper(PGResultWrapper &&other) noexcept;
  PGResultWrapper &operator=(PGResultWrapper &&other) noexcept;
  PGresult *get() const;
  PGresult *operator->() const;
  explicit operator bool() const;
  PGresult *release();
  void reset(PGresult *res = nullptr);
};

class ResultRow {
private:
  std::vector<std::string> values;
  std::vector<std::string> columns;
  std::map<std::string, int> columnIndexMap;

public:
  ResultRow();
  ResultRow(const std::vector<std::string> &colNames,
            const std::vector<std::string> &rowValues);

  std::string getString(const std::string &columnName,
                        const std::string &defaultValue = "") const;
  std::string getString(int columnIndex,
                        const std::string &defaultValue = "") const;
  int getInt(const std::string &columnName, int defaultValue = 0) const;
  int getInt(int columnIndex, int defaultValue = 0) const;
  double getDouble(const std::string &columnName,
                   double defaultValue = 0.0) const;
  double getDouble(int columnIndex, double defaultValue = 0.0) const;
  bool getBool(const std::string &columnName, bool defaultValue = false) const;
  bool getBool(int columnIndex, bool defaultValue = false) const;

  bool isNull(const std::string &columnName) const;
  bool isNull(int columnIndex) const;
  bool hasColumn(const std::string &columnName) const;
  int getColumnCount() const;
  bool isEmpty() const;

  const std::vector<std::string> &getValues() const;
  const std::vector<std::string> &getColumns() const;
};

class QueryResult {
private:
  std::vector<ResultRow> rows;
  std::vector<std::string> columnNames;
  int affectedRows;
  std::string errorMessage;

public:
  QueryResult();
  QueryResult(PGresult *result);

  bool loadFromResult(PGresult *result);
  void clear();

  const ResultRow &getRow(size_t index) const;
  ResultRow &getRow(size_t index);
  const std::vector<ResultRow> &getAllRows() const;
  const std::vector<std::string> &getColumnNames() const;

  size_t getRowCount() const;
  size_t getColumnCount() const;
  int getAffectedRows() const;
  bool hasData() const;
  bool hasError() const;
  const std::string &getErrorMessage() const;
  void setErrorMessage(const std::string &error);
  ResultRow getFirstRow() const;
  std::string getFirstValue(const std::string &columnName,
                            const std::string &defaultValue = "") const;
  int getFirstInt(const std::string &columnName, int defaultValue = 0) const;
};

class PostgreSQLUtils {
public:
  // Выполнение запроса и получение результата
  static QueryResult executeQuery(PostgreSQLConnection &connection,
                                  const std::string &query);
  static QueryResult executeQueryParams(PostgreSQLConnection &connection,
                                        const std::string &query,
                                        const std::vector<std::string> &params);
  static void printResult(const QueryResult &result,
                          std::ostream &output = std::cout);
  static void printResult(PGresult *result, std::ostream &output = std::cout);
  static void printResultTable(const QueryResult &result,
                               std::ostream &output = std::cout);
  static std::vector<std::string> getColumnNames(PGresult *result);
  static int getRowCount(PGresult *result);
  static int getColumnCount(PGresult *result);
  static std::string getValue(PGresult *result, int row, int col,
                              const std::string &defaultValue = "");
  static bool isResultValid(PGresult *result);
  static bool hasRows(PGresult *result);
  static std::string resultStatusToString(ExecStatusType status);
  static bool testConnection(PostgreSQLConnection &connection);
  static std::string getDatabaseInfo(PostgreSQLConnection &connection);
  static bool executeTransaction(PostgreSQLConnection &connection,
                                 const std::vector<std::string> &queries);
  static bool
  executeBatch(PostgreSQLConnection &connection, const std::string &baseQuery,
               const std::vector<std::vector<std::string>> &paramsList);
};

#endif // POSTGRESQL_UTILS_H
