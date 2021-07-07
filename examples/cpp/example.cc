// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <ctime>
#include <iostream>
#include <sstream>

#include "kudu/client/callbacks.h"
#include "kudu/client/client.h"
#include "kudu/client/row_result.h"
#include "kudu/client/stubs.h"
#include "kudu/client/value.h"
#include "kudu/client/write_op.h"
#include "kudu/common/partial_row.h"
#include "kudu/util/monotime.h"
#include "kudu/util/logging.h"

using kudu::client::KuduClient;
using kudu::client::KuduClientBuilder;
using kudu::client::KuduColumnSchema;
using kudu::client::KuduError;
using kudu::client::KuduInsert;
using kudu::client::KuduPredicate;
using kudu::client::KuduScanBatch;
using kudu::client::KuduScanner;
using kudu::client::KuduSchema;
using kudu::client::KuduSchemaBuilder;
using kudu::client::KuduSession;
using kudu::client::KuduStatusFunctionCallback;
using kudu::client::KuduTable;
using kudu::client::KuduTableAlterer;
using kudu::client::KuduTableCreator;
using kudu::client::KuduValue;
using kudu::client::sp::shared_ptr;
using kudu::KuduPartialRow;
using kudu::MonoDelta;
using kudu::Status;

using std::ostringstream;
using std::string;
using std::vector;

static Status CreateClient(const vector<string>& master_addrs,
                           shared_ptr<KuduClient>* client) {
  return KuduClientBuilder()
      .master_server_addrs(master_addrs)
      .default_admin_operation_timeout(MonoDelta::FromSeconds(20))
      .Build(client);
}

static KuduSchema CreateSchema() {
  KuduSchema schema;
  KuduSchemaBuilder b;
  b.AddColumn("key")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
  b.AddColumn("int_val")->Type(KuduColumnSchema::INT32)->NotNull();
  b.AddColumn("string_val")->Type(KuduColumnSchema::STRING)->NotNull();
  b.AddColumn("non_null_with_default")->Type(KuduColumnSchema::INT32)->NotNull()
    ->Default(KuduValue::FromInt(12345));
  KUDU_CHECK_OK(b.Build(&schema));
  return schema;
}

static Status DoesTableExist(const shared_ptr<KuduClient>& client,
                             const string& table_name,
                             bool *exists) {
  shared_ptr<KuduTable> table;
  Status s = client->OpenTable(table_name, &table);
  if (s.ok()) {
    *exists = true;
  } else if (s.IsNotFound()) {
    *exists = false;
    s = Status::OK();
  }
  return s;
}

static Status CreateTable(const shared_ptr<KuduClient>& client,
                          const string& table_name,
                          const KuduSchema& schema,
                          int num_tablets) {
  vector<string> column_names;
  column_names.push_back("key");

  // Set the schema and range partition columns.
  KuduTableCreator* table_creator = client->NewTableCreator();
  table_creator->table_name(table_name)
      .schema(&schema)
      .set_range_partition_columns(column_names);

  // Generate and add the range partition splits for the table.
  int32_t increment = 1000 / num_tablets;
  for (int32_t i = 1; i < num_tablets; i++) {
    KuduPartialRow* row = schema.NewRow();
    KUDU_CHECK_OK(row->SetInt32(0, i * increment));
    table_creator->add_range_partition_split(row);
  }

  Status s = table_creator->Create();
  delete table_creator;
  return s;
}

static Status AlterTable(const shared_ptr<KuduClient>& client,
                         const string& table_name) {
  KuduTableAlterer* table_alterer = client->NewTableAlterer(table_name);
  table_alterer->AlterColumn("int_val")->RenameTo("integer_val");
  table_alterer->AddColumn("another_val")->Type(KuduColumnSchema::BOOL);
  table_alterer->DropColumn("string_val");
  Status s = table_alterer->Alter();
  delete table_alterer;
  return s;
}

static void StatusCB(void* unused, const Status& status) {
  KUDU_LOG(INFO) << "Asynchronous flush finished with status: "
                 << status.ToString();
}

static Status InsertRows(const shared_ptr<KuduTable>& table, int num_rows) {
  shared_ptr<KuduSession> session = table->client()->NewSession();
  KUDU_RETURN_NOT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
  session->SetTimeoutMillis(5000);

  for (int i = 0; i < num_rows; i++) {
    KuduInsert* insert = table->NewInsert();
    KuduPartialRow* row = insert->mutable_row();
    KUDU_CHECK_OK(row->SetInt32("key", i));
    KUDU_CHECK_OK(row->SetInt32("integer_val", i * 2));
    KUDU_CHECK_OK(row->SetInt32("non_null_with_default", i * 5));
    KUDU_CHECK_OK(session->Apply(insert));
  }
  Status s = session->Flush();
  if (s.ok()) {
    return s;
  }

  // Test asynchronous flush.
  KuduStatusFunctionCallback<void*> status_cb(&StatusCB, NULL);
  session->FlushAsync(&status_cb);

  // Look at the session's errors.
  vector<KuduError*> errors;
  bool overflow;
  session->GetPendingErrors(&errors, &overflow);
  if (!errors.empty()) {
    s = overflow ? Status::IOError("Overflowed pending errors in session") :
        errors.front()->status();
    while (!errors.empty()) {
      delete errors.back();
      errors.pop_back();
    }
  }
  KUDU_RETURN_NOT_OK(s);

  // Close the session.
  return session->Close();
}

static Status ScanRows(const shared_ptr<KuduTable>& table) {
  const int kLowerBound = 5;
  const int kUpperBound = 600;

  KuduScanner scanner(table.get());

  // To be guaranteed results are returned in primary key order, make the
  // scanner fault-tolerant. This also means the scanner can recover if,
  // for example, the server it is scanning fails in the middle of a scan.
  KUDU_RETURN_NOT_OK(scanner.SetFaultTolerant());

  // Add a predicate: WHERE key >= 5
  KuduPredicate* p = table->NewComparisonPredicate(
      "key", KuduPredicate::GREATER_EQUAL, KuduValue::FromInt(kLowerBound));
  KUDU_RETURN_NOT_OK(scanner.AddConjunctPredicate(p));

  // Add a predicate: WHERE key <= 600
  p = table->NewComparisonPredicate(
      "key", KuduPredicate::LESS_EQUAL, KuduValue::FromInt(kUpperBound));
  KUDU_RETURN_NOT_OK(scanner.AddConjunctPredicate(p));

  KUDU_RETURN_NOT_OK(scanner.Open());
  KuduScanBatch batch;

  int next_row = kLowerBound;
  while (scanner.HasMoreRows()) {
    KUDU_RETURN_NOT_OK(scanner.NextBatch(&batch));
    for (KuduScanBatch::const_iterator it = batch.begin();
         it != batch.end();
         ++it, ++next_row) {
      KuduScanBatch::RowPtr row(*it);
      int32_t val;
      KUDU_RETURN_NOT_OK(row.GetInt32("key", &val));
      if (val != next_row) {
        ostringstream out;
        out << "Scan returned the wrong results. Expected key "
            << next_row << " but got " << val;
        return Status::IOError(out.str());
      }
    }
  }

  // next_row is now one past the last row we read.
  int last_row_seen = next_row - 1;

  if (last_row_seen != kUpperBound) {
    ostringstream out;
    out << "Scan returned the wrong results. Expected last row to be "
        << kUpperBound << " rows but got " << last_row_seen;
    return Status::IOError(out.str());
  }
  return Status::OK();
}

// A helper class providing custom logging callback. It also manages
// automatic callback installation and removal.
class LogCallbackHelper {
 public:
  LogCallbackHelper() : log_cb_(&LogCallbackHelper::LogCb, NULL) {
    kudu::client::InstallLoggingCallback(&log_cb_);
  }

  ~LogCallbackHelper() {
    kudu::client::UninstallLoggingCallback();
  }

  static void LogCb(void* unused,
                    kudu::client::KuduLogSeverity severity,
                    const char* filename,
                    int line_number,
                    const struct ::tm* time,
                    const char* message,
                    size_t message_len) {
    KUDU_LOG(INFO) << "Received log message from Kudu client library";
    KUDU_LOG(INFO) << " Severity: " << severity;
    KUDU_LOG(INFO) << " Filename: " << filename;
    KUDU_LOG(INFO) << " Line number: " << line_number;
    char time_buf[32];
    // Example: Tue Mar 24 11:46:43 2015.
    KUDU_CHECK(strftime(time_buf, sizeof(time_buf), "%a %b %d %T %Y", time));
    KUDU_LOG(INFO) << " Time: " << time_buf;
    KUDU_LOG(INFO) << " Message: " << string(message, message_len);
  }

 private:
  kudu::client::KuduLoggingFunctionCallback<void*> log_cb_;
};

int main(int argc, char* argv[]) {
  KUDU_LOG(INFO) << "Running with Kudu client version: " <<
      kudu::client::GetShortVersionString();
  KUDU_LOG(INFO) << "Long version info: " <<
      kudu::client::GetAllVersionInfo();

  // This is to install and automatically un-install custom logging callback.
  kudu::InitGoogleLoggingSafeBasic(argv[0]);
  LogCallbackHelper log_cb_helper;

  if (argc < 2) {
    KUDU_LOG(FATAL) << "usage: " << argv[0] << " <master host> ...";
  }
  vector<string> master_addrs;
  for (int i = 1; i < argc; i++) {
    master_addrs.push_back(argv[i]);
  }

  const string kTableName = "test_table";

  // Enable verbose debugging for the client library.
  kudu::client::SetVerboseLogLevel(2);

  // Create and connect a client.
  shared_ptr<KuduClient> client;
  KUDU_CHECK_OK(CreateClient(master_addrs, &client));
  KUDU_LOG(INFO) << "Created a client connection";

  // Disable the verbose logging.
  kudu::client::SetVerboseLogLevel(0);

  // Create a schema.
  KuduSchema schema(CreateSchema());
  KUDU_LOG(INFO) << "Created a schema";

  // Create a table with that schema.
  bool exists;
  KUDU_CHECK_OK(DoesTableExist(client, kTableName, &exists));
  if (exists) {
    client->DeleteTable(kTableName);
    KUDU_LOG(INFO) << "Deleting old table before creating new one";
  }
  KUDU_CHECK_OK(CreateTable(client, kTableName, schema, 10));
  KUDU_LOG(INFO) << "Created a table";

  // Alter the table.
  KUDU_CHECK_OK(AlterTable(client, kTableName));
  KUDU_LOG(INFO) << "Altered a table";

  // Insert some rows into the table.
  shared_ptr<KuduTable> table;
  KUDU_CHECK_OK(client->OpenTable(kTableName, &table));
  KUDU_CHECK_OK(InsertRows(table, 1000));
  KUDU_LOG(INFO) << "Inserted some rows into a table";

  // Scan some rows.
  KUDU_CHECK_OK(ScanRows(table));
  KUDU_LOG(INFO) << "Scanned some rows out of a table";

  // Delete the table.
  KUDU_CHECK_OK(client->DeleteTable(kTableName));
  KUDU_LOG(INFO) << "Deleted a table";

  // Done!
  KUDU_LOG(INFO) << "Done";

  return 0;
}
