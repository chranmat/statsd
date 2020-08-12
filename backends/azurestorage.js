/*jshint node:true, laxcomma:true */

const util              = require('util');
const { v4: uuidv4 }    = require('uuid');
const AzureStorage      = require('azure-storage');

var table;

function AzureStorageBackend(startupTime, config, emitter){
  var self = this;
  this.lastFlush = startupTime;
  this.lastException = startupTime;
  this.config = config || {};

  // Verify wether AzureStorage is configured properly.
  if(!this.config.azurestorage || !this.config.azurestorage.account || !this.config.azurestorage.accesskey || !this.config.azurestorage.table) {
    util.log('Azure Storage configuration incomplete', 'ERROR');
    process.exit(1);
  }

  // Create Azure table if not exist
  table = AzureStorage.createTableService(this.config.azurestorage.account, this.config.azurestorage.accesskey);

  table.createTableIfNotExists(this.config.azurestorage.table, (err, result, response) => {
      
      if(err) { 
          util.log(err, 'ERROR');
          process.exit(1);
      }

      else if(result && result.isSuccessful && result.created) {
          util.log(`Successfully connected to AzureStorage. Table ${result.TableName} was successfully created.`, 'INFO');
      }

      else if(result && result.isSuccessful) {
        util.log(`Successfully connected to AzureStorage. Table ${result.TableName} did already exist.`, 'INFO');
      }

      else {
          util.log('Unknown result. Printing response to console', 'WARNING');
          util.log(result, 'DEBUG');
          util.log(response, 'DEBUG');
          process.exit(1);
      }
  })

  emitter.on('flush', function(timestamp, metrics) { self.flush(timestamp, metrics); });
  emitter.on('status', function(callback) { self.status(callback); });
};

AzureStorageBackend.prototype.flush = function(timestamp, metrics) {

        const sampleZero = this.config.azurestorage.samplezero || false;
        const debug = this.config.azurestorage.debug || false;
        const received = metrics.counters['statsd.metrics_received'] || 0;

        if(sampleZero || received > 0 ) {

            if(debug) {
                console.log(metrics);
            }

            let record = {
                PartitionKey: this.config.azurestorage.partitionkey,
                RowKey: uuidv4(),
                Metrics: JSON.stringify(metrics)
            }

            table.insertEntity(this.config.azurestorage.table, record, (err, result, response) => {
                if(err) { 
                    util.log(err, 'ERROR');
                }

                if(response && response.isSuccessful) {
                    if(debug) { console.log(JSON.stringify(record, null, 4)); }

                    util.log(`Successfully inserted metric to Azure Storage table ${this.config.azurestorage.table} on account ${this.config.azurestorage.account}`)

                }
                else {
                    util.log(response, 'ERROR')
                }
            })

        }
};

AzureStorageBackend.prototype.status = function(write) {
  ['lastFlush', 'lastException'].forEach(function(key) {
    write(null, 'console', key, this[key]);
  }, this);
};

exports.init = function(startupTime, config, events) {
    var instance = new AzureStorageBackend(startupTime, config, events);
    return true;
};
