using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using NHibernate.Engine;
using Npgsql;

namespace NHibernate.AdoNet {
    static class StringExtensions {
        public static IEnumerable<string> SplitAndTrim(this string s, params string[] delimiters) {
            return SplitAndTrim(s, StringSplitOptions.RemoveEmptyEntries, delimiters);
        }

        public static IEnumerable<string> SplitAndTrim(this string s, StringSplitOptions options, params string[] delimiters) {
            if (s == null) {
                return new string[] {};
            }
            var query = s.Split(delimiters, StringSplitOptions.None).Select(x => x.Trim());
            if (options == StringSplitOptions.RemoveEmptyEntries) {
                query = query.Where(x => x.Trim() != string.Empty);
            }
            return query.ToList();
        }
    }

    public class NpgsqlBatcherFactory : IBatcherFactory {
        public virtual IBatcher CreateBatcher(ConnectionManager connectionManager, IInterceptor interceptor) {
            return new NpgsqlClientBatcher(connectionManager, interceptor);
        }
    }

    public class NpgsqlClientBatcher : AbstractBatcher {

        private int _batchSize;
        private int _commandCount;
        private int _totalExpectedRowsAffected;
        private StringBuilder _batchCommand;
        private int _parameterCounter;

        private IDbCommand _currentBatch;

        public NpgsqlClientBatcher(ConnectionManager connectionManager, IInterceptor interceptor)
            : base(connectionManager, interceptor) {
            _batchSize = Factory.Settings.AdoBatchSize;
        }

        private string NextParam() {
            return ":p" + _parameterCounter++;
        }

        public override void AddToBatch(IExpectation expectation) {
            if (!expectation.CanBeBatched) {
                var cmd = CurrentCommand;
                LogCommand(CurrentCommand);
                var rowCount = ExecuteNonQuery(cmd);
                expectation.VerifyOutcomeNonBatched(rowCount, cmd);
                _currentBatch = null;
                return;
            }

            _totalExpectedRowsAffected += expectation.ExpectedRowCount;
            Log.Info("Adding to batch");

            if (_currentBatch == null) {
                _currentBatch = new NpgsqlCommand();
                _batchCommand = new StringBuilder();
                _parameterCounter = 0;
            }

            if (CurrentCommand.CommandText.StartsWith("INSERT INTO") && CurrentCommand.CommandText.Contains("VALUES")) {
                DoInsertBatch();
            } else {
                DoRegularBatch();
            }
            _commandCount++;

            //check for flush
            if (_commandCount >= _batchSize) {
                DoExecuteBatch(_currentBatch);
            }
        }

        string _insertcmd = string.Empty;
        readonly List<string> _insertparams = new List<string>();

        public void DoInsertBatch() {
            var i = CurrentCommand.CommandText.IndexOf("VALUES", StringComparison.Ordinal) + "VALUES".Length + 1;
            var cmd = CurrentCommand.CommandText.Substring(0, i);

            if (!cmd.Equals(_insertcmd)) {
                AppendInsertsToCmd();
                _insertcmd = cmd;
            }

            var vals = CurrentCommand.CommandText
                .Substring(i + 1, CurrentCommand.CommandText.Length - i - 2)
                .SplitAndTrim(",");

            var paramdict = CurrentCommand.Parameters
                .Cast<NpgsqlParameter>()
                .ToDictionary(param => param.ParameterName);

            var sb = new List<string>();
            foreach (var val in vals) {
                if (paramdict.ContainsKey(val)) {
                    var paramname = NextParam();
                    var newParam = paramdict[val].Clone();
                    newParam.ParameterName = paramname;
                    _currentBatch.Parameters.Add(newParam);
                    sb.Add(paramname);
                } else {
                    sb.Add(val);
                }
                
            }

            _insertparams.Add("( " + string.Join(", ", sb) + " )");
        }

        void AppendInsertsToCmd() {
            if (_insertparams.Any()) {
                _batchCommand
                    .Append(_insertcmd)
                    .Append(string.Join(", ", _insertparams))
                    .Append(";\n");
            }
            _insertcmd = string.Empty;
            _insertparams.Clear();
        }

        public void DoRegularBatch() {
            var preCommand = new StringBuilder(CurrentCommand.CommandText).Append(";\n");

            foreach (NpgsqlParameter param in CurrentCommand.Parameters) {
                var paramname = NextParam();
                preCommand
                    .Replace(param.ParameterName + " ", paramname + " ")
                    .Replace(param.ParameterName + ",", paramname + ",")
                    .Replace(param.ParameterName + ";", paramname + ";")
                    .Replace(param.ParameterName + ")", paramname + ")");

                var newParam = param.Clone();
                newParam.ParameterName = paramname;
                _currentBatch.Parameters.Add(newParam);
            }

            _batchCommand.Append(preCommand);


            _commandCount++;
            //check for flush
            if (_commandCount >= _batchSize) {
                DoExecuteBatch(_currentBatch);
            }
        }

        protected override void DoExecuteBatch(IDbCommand ps) {
            if (_currentBatch == null) return;

            _commandCount= 0;

            Log.Info("Executing batch");
            CheckReaders();

            AppendInsertsToCmd();

            var commandText = _batchCommand.ToString();
            _currentBatch.CommandText = commandText;

            LogCommand(_currentBatch);

            Prepare(_currentBatch);

            int rowsAffected;
            try {
                rowsAffected = _currentBatch.ExecuteNonQuery();
            } catch (Exception e) {
                Log.Error("Error executing batch.", e);
                throw;
            }

            Expectations.VerifyOutcomeBatched(_totalExpectedRowsAffected, rowsAffected);

            _totalExpectedRowsAffected = 0;
            _currentBatch = null;
            _batchCommand = null;
            _parameterCounter = 0;
        }

        protected override int CountOfStatementsInCurrentBatch {
            get { return _commandCount; }
        }

        public override int BatchSize {
            get { return _batchSize; }
            set { _batchSize = value; }
        }
    }
}
