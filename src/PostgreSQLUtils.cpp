#include "../include/PostgreSQLUtils.h"
#include <iomanip>
#include <sstream>

PGResultWrapper::~PGResultWrapper() {
  if (result) {
    PQclear(result);
  }
}

PGResultWrapper::PGResultWrapper(PGResultWrapper &&other) noexcept
    : result(other.result) {
  other.result = nullptr;
}

PGResultWrapper &PGResultWrapper::operator=(PGResultWrapper &&other) noexcept {
  if (this != &other) {
    if (result) {
      PQclear(result);
    }
    result = other.result;
    other.result = nullptr;
  }
  return *this;
}

PGresult *PGResultWrapper::get() const { return result; }

PGresult *PGResultWrapper::operator->() const { return result; }

PGResultWrapper::operator bool() const { return result != nullptr; }

PGresult *PGResultWrapper::release() {
  PGresult *temp = result;
  result = nullptr;
  return temp;
}

void PGResultWrapper::reset(PGresult *res) {
  if (result) {
    PQclear(result);
  }
  result = res;
}

ResultRow::ResultRow() = default;

ResultRow::ResultRow(const std::vector<std::string> &colNames,
                     const std::vector<std::string> &rowValues)
    : values(rowValues), columns(colNames) {
  for (size_t i = 0; i < columns.size(); ++i) {
    columnIndexMap[columns[i]] = static_cast<int>(i);
  }
}

std::string ResultRow::getString(const std::string &columnName,
                                 const std::string &defaultValue) const {
  auto it = columnIndexMap.find(columnName);
  if (it != columnIndexMap.end() &&
      it->second < static_cast<int>(values.size())) {
    return values[it->second];
  }
  return defaultValue;
}

std::string ResultRow::getString(int columnIndex,
                                 const std::string &defaultValue) const {
  if (columnIndex >= 0 && columnIndex < static_cast<int>(values.size())) {
    return values[columnIndex];
  }
  return defaultValue;
}

int ResultRow::getInt(const std::string &columnName, int defaultValue) const {
  std::string value = getString(columnName);
  if (value.empty() || value == "NULL")
    return defaultValue;
  try {
    return std::stoi(value);
  } catch (const std::exception &) {
    return defaultValue;
  }
}

int ResultRow::getInt(int columnIndex, int defaultValue) const {
  std::string value = getString(columnIndex);
  if (value.empty() || value == "NULL")
    return defaultValue;
  try {
    return std::stoi(value);
  } catch (const std::exception &) {
    return defaultValue;
  }
}

double ResultRow::getDouble(const std::string &columnName,
                            double defaultValue) const {
  std::string value = getString(columnName);
  if (value.empty() || value == "NULL")
    return defaultValue;
  try {
    return std::stod(value);
  } catch (const std::exception &) {
    return defaultValue;
  }
}

double ResultRow::getDouble(int columnIndex, double defaultValue) const {
  std::string value = getString(columnIndex);
  if (value.empty() || value == "NULL")
    return defaultValue;
  try {
    return std::stod(value);
  } catch (const std::exception &) {
    return defaultValue;
  }
}

bool ResultRow::getBool(const std::string &columnName,
                        bool defaultValue) const {
  std::string value = getString(columnName);
  if (value == "t" || value == "true" || value == "1" || value == "yes")
    return true;
  if (value == "f" || value == "false" || value == "0" || value == "no")
    return false;
  return defaultValue;
}

bool ResultRow::getBool(int columnIndex, bool defaultValue) const {
  std::string value = getString(columnIndex);
  if (value == "t" || value == "true" || value == "1" || value == "yes")
    return true;
  if (value == "f" || value == "false" || value == "0" || value == "no")
    return false;
  return defaultValue;
}

bool ResultRow::isNull(const std::string &columnName) const {
  return getString(columnName).empty() || getString(columnName) == "NULL";
}

bool ResultRow::isNull(int columnIndex) const {
  return getString(columnIndex).empty() || getString(columnIndex) == "NULL";
}

bool ResultRow::hasColumn(const std::string &columnName) const {
  return columnIndexMap.find(columnName) != columnIndexMap.end();
}

int ResultRow::getColumnCount() const {
  return static_cast<int>(values.size());
}

bool ResultRow::isEmpty() const { return values.empty(); }

const std::vector<std::string> &ResultRow::getValues() const { return values; }

const std::vector<std::string> &ResultRow::getColumns() const {
  return columns;
}

QueryResult::QueryResult() : affectedRows(0) {}

QueryResult::QueryResult(PGresult *result) : affectedRows(0) {
  loadFromResult(result);
}

bool QueryResult::loadFromResult(PGresult *result) {
  clear();

  if (!PostgreSQLUtils::isResultValid(result)) {
    errorMessage = "Invalid result";
    return false;
  }

  ExecStatusType status = PQresultStatus(result);
  if (status == PGRES_TUPLES_OK) {
    columnNames = PostgreSQLUtils::getColumnNames(result);
    int rowCount = PostgreSQLUtils::getRowCount(result);
    int colCount = PostgreSQLUtils::getColumnCount(result);
    for (int i = 0; i < rowCount; ++i) {
      std::vector<std::string> rowValues;
      for (int j = 0; j < colCount; ++j) {
        rowValues.push_back(PostgreSQLUtils::getValue(result, i, j));
      }
      rows.emplace_back(columnNames, rowValues);
    }
    affectedRows = rowCount;
  } else if (status == PGRES_COMMAND_OK) {
    char *affected = PQcmdTuples(result);
    if (affected) {
      affectedRows = std::stoi(affected);
    }
  } else {
    errorMessage = PostgreSQLUtils::resultStatusToString(status);
    return false;
  }
  return true;
}

void QueryResult::clear() {
  rows.clear();
  columnNames.clear();
  affectedRows = 0;
  errorMessage.clear();
}

const ResultRow &QueryResult::getRow(size_t index) const {
  static ResultRow emptyRow;
  if (index < rows.size()) {
    return rows[index];
  }
  return emptyRow;
}

ResultRow &QueryResult::getRow(size_t index) {
  static ResultRow emptyRow;
  if (index < rows.size()) {
    return rows[index];
  }
  return emptyRow;
}

const std::vector<ResultRow> &QueryResult::getAllRows() const { return rows; }

const std::vector<std::string> &QueryResult::getColumnNames() const {
  return columnNames;
}

size_t QueryResult::getRowCount() const { return rows.size(); }

size_t QueryResult::getColumnCount() const { return columnNames.size(); }

int QueryResult::getAffectedRows() const { return affectedRows; }

bool QueryResult::hasData() const { return !rows.empty(); }

bool QueryResult::hasError() const { return !errorMessage.empty(); }

const std::string &QueryResult::getErrorMessage() const { return errorMessage; }

void QueryResult::setErrorMessage(const std::string &error) {
  errorMessage = error;
}

ResultRow QueryResult::getFirstRow() const {
  if (!rows.empty()) {
    return rows[0];
  }
  return ResultRow();
}

std::string QueryResult::getFirstValue(const std::string &columnName,
                                       const std::string &defaultValue) const {
  if (!rows.empty()) {
    return rows[0].getString(columnName, defaultValue);
  }
  return defaultValue;
}

int QueryResult::getFirstInt(const std::string &columnName,
                             int defaultValue) const {
  if (!rows.empty()) {
    return rows[0].getInt(columnName, defaultValue);
  }
  return defaultValue;
}

QueryResult PostgreSQLUtils::executeQuery(PostgreSQLConnection &connection,
                                          const std::string &query) {
  QueryResult result;
  if (!connection.isOK()) {
    result.setErrorMessage("Connection is not established");
    return result;
  }
  PGresult *pgResult = PQexec(connection.getRawConnection(), query.c_str());
  result.loadFromResult(pgResult);
  PQclear(pgResult);
  return result;
}

QueryResult
PostgreSQLUtils::executeQueryParams(PostgreSQLConnection &connection,
                                    const std::string &query,
                                    const std::vector<std::string> &params) {
  QueryResult result;
  if (!connection.isOK()) {
    result.setErrorMessage("Connection is not established");
    return result;
  }
  std::vector<const char *> paramValues;
  for (const auto &param : params) {
    paramValues.push_back(param.c_str());
  }
  PGresult *pgResult = PQexecParams(
      connection.getRawConnection(), query.c_str(), params.size(), nullptr,
      paramValues.empty() ? nullptr : paramValues.data(), nullptr, nullptr, 0);
  result.loadFromResult(pgResult);
  PQclear(pgResult);
  return result;
}

void PostgreSQLUtils::printResult(const QueryResult &result,
                                  std::ostream &output) {
  if (result.hasError()) {
    output << "Error: " << result.getErrorMessage() << std::endl;
    return;
  }
  if (!result.hasData()) {
    output << "No data returned. Affected rows: " << result.getAffectedRows()
           << std::endl;
    return;
  }
  printResultTable(result, output);
}

void PostgreSQLUtils::printResult(PGresult *result, std::ostream &output) {
  QueryResult qr(result);
  printResult(qr, output);
}

void PostgreSQLUtils::printResultTable(const QueryResult &result,
                                       std::ostream &output) {
  const auto &columnNames = result.getColumnNames();
  const auto &rows = result.getAllRows();
  if (columnNames.empty()) {
    output << "No columns" << std::endl;
    return;
  }
  std::vector<size_t> columnWidths;
  for (const auto &colName : columnNames) {
    columnWidths.push_back(colName.length());
  }
  for (const auto &row : rows) {
    const auto &values = row.getValues();
    for (size_t i = 0; i < values.size() && i < columnWidths.size(); ++i) {
      if (values[i].length() > columnWidths[i]) {
        columnWidths[i] = values[i].length();
      }
    }
  }
  output << std::left;
  for (size_t i = 0; i < columnNames.size(); ++i) {
    output << std::setw(columnWidths[i] + 2) << columnNames[i];
  }
  output << std::endl;
  for (size_t i = 0; i < columnNames.size(); ++i) {
    output << std::string(columnWidths[i] + 2, '-');
  }
  output << std::endl;
  for (const auto &row : rows) {
    const auto &values = row.getValues();
    for (size_t i = 0; i < values.size() && i < columnWidths.size(); ++i) {
      output << std::setw(columnWidths[i] + 2) << values[i];
    }
    output << std::endl;
  }
  output << "Total rows: " << rows.size() << std::endl;
}

std::vector<std::string> PostgreSQLUtils::getColumnNames(PGresult *result) {
  std::vector<std::string> columns;
  if (!isResultValid(result))
    return columns;
  int colCount = PQnfields(result);
  for (int i = 0; i < colCount; ++i) {
    columns.push_back(PQfname(result, i));
  }
  return columns;
}

int PostgreSQLUtils::getRowCount(PGresult *result) {
  return isResultValid(result) ? PQntuples(result) : 0;
}

int PostgreSQLUtils::getColumnCount(PGresult *result) {
  return isResultValid(result) ? PQnfields(result) : 0;
}

std::string PostgreSQLUtils::getValue(PGresult *result, int row, int col,
                                      const std::string &defaultValue) {
  if (!isResultValid(result) || row < 0 || row >= getRowCount(result) ||
      col < 0 || col >= getColumnCount(result)) {
    return defaultValue;
  }

  const char *value = PQgetvalue(result, row, col);
  return value ? value : defaultValue;
}

bool PostgreSQLUtils::isResultValid(PGresult *result) {
  return result && (PQresultStatus(result) == PGRES_TUPLES_OK ||
                    PQresultStatus(result) == PGRES_COMMAND_OK);
}

bool PostgreSQLUtils::hasRows(PGresult *result) {
  return getRowCount(result) > 0;
}

std::string PostgreSQLUtils::resultStatusToString(ExecStatusType status) {
  return PQresStatus(status);
}

bool PostgreSQLUtils::testConnection(PostgreSQLConnection &connection) {
  auto result = executeQuery(connection, "SELECT 1");
  return !result.hasError();
}

std::string PostgreSQLUtils::getDatabaseInfo(PostgreSQLConnection &connection) {
  auto result = executeQuery(
      connection, "SELECT version(), current_database(), current_user");
  if (!result.hasError() && result.hasData()) {
    std::stringstream ss;
    ss << "Version: " << result.getFirstValue("version") << "\n"
       << "Database: " << result.getFirstValue("current_database") << "\n"
       << "User: " << result.getFirstValue("current_user");
    return ss.str();
  }
  return "Failed to get database info";
}

bool PostgreSQLUtils::executeTransaction(
    PostgreSQLConnection &connection, const std::vector<std::string> &queries) {
  if (!connection.beginTransaction()) {
    return false;
  }
  try {
    for (const auto &query : queries) {
      auto result = executeQuery(connection, query);
      if (result.hasError()) {
        connection.rollbackTransaction();
        return false;
      }
    }
    return connection.commitTransaction();
  } catch (...) {
    connection.rollbackTransaction();
    throw;
  }
}

bool PostgreSQLUtils::executeBatch(
    PostgreSQLConnection &connection, const std::string &baseQuery,
    const std::vector<std::vector<std::string>> &paramsList) {
  if (!connection.beginTransaction()) {
    return false;
  }
  try {
    for (const auto &params : paramsList) {
      auto result = executeQueryParams(connection, baseQuery, params);
      if (result.hasError()) {
        connection.rollbackTransaction();
        return false;
      }
    }
    return connection.commitTransaction();
  } catch (...) {
    connection.rollbackTransaction();
    throw;
  }
}
