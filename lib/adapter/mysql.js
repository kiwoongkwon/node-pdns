var mysql = require('mysql');
var spawn = require('child_process').spawn;
var fs = require('fs');
var Step = require('step');
var checkDomainName = require('../util').checkDomainName;
var trim = require('../util').trim;
var checkString = require('../util').checkString;
var updateSoa = require('../util').updateSoa;

var connection = null;

var adapter = module.exports = {

  init: function(config, callback) {
	if(connection === null){
	    connection = mysql.createConnection(config);
	    createTablesIfNotExist(config, callback);
	} else{
		callback();
	}
  },

  domains: {
    list: function(domain, callback) {
      /*pool.getConnection(function(err, connection){
	    if (err) {
		  callback(err);
		  return;
		}*/
        connection.query('SELECT * FROM domains WHERE name LIKE ? AND type LIKE ?'
	                   +' AND ttl LIKE ? AND created_at LIKE ?',
	                     [domain.name || '%', domain.type || '%',
	                      domain.ttl || '%', domain.created_at || '%'],
	                     function(err, results, fields) {
	      //connection.release();
          if (err) {
        	console.log(err);
	        callback(err);
	        return;
	      }
	      callback(null, results);
	    });
      //});
    },
    
    add: function(domain, callback) {
      if(!domain || !checkDomainName(domain.name)) {
        callback(new Error("domain name invalid"));
        return;
      }
      /*pool.getConnection(function(err, connection){
	    if (err) {
		  callback(err);
		  return;
		}*/
	    connection.query('INSERT INTO domains SET name=?, type=?, '
                        +'ttl=?, created_at=NOW(), updated_at=NOW()',
                         [domain.name, (domain.type || 'NATIVE').toUpperCase(),
                          domain.ttl || 84600],
                         function(err, result){
          //connection.release();
          if (err) {
            console.log(err);
            callback(err);
            return;
          }
          callback(null, results);
        });
      //});
    },

    remove: function(domain, callback) {
      if(!domain || !checkDomainName(domain.name)) {
        callback(new Error("domain name invalid"));
        return;
      }
      /*pool.getConnection(function(err, connection){
	    if (err) {
	      callback(err);
		  return;
		}*/
	    connection.query('DELETE FROM domains WHERE name=? AND type LIKE ?'
                       +' AND ttl LIKE ? AND created_at LIKE ?'
                       +' AND updated_at LIKE ?',
                         [domain.name, domain.type || '%',
                          domain.ttl || '%', domain.created_at || '%',
                          domain.updated_at || '%'],
                         function(err) {
          //connection.release();
          if (err) {
            console.log(err);
          }
          callback(err);
        });
      //});
    }
  },

  records: {
    list: function(domainName, record, callback) {
      if(!checkDomainName(domainName)) {
        callback(new Error("domain invalid"));
        return;
      }
      if(!callback) {
        callback = record;
        record = null;
      }
      record = record || {};
      /*pool.getConnection(function(err, connection){
	    if (err) {
	      callback(err);
		  return;
		}*/
        connection.query('SELECT R.* FROM records R JOIN domains D ON R.domain_id=D.id '
	                    +'WHERE D.name=? AND R.name LIKE ? AND R.type LIKE ? '
	                    +'AND R.content LIKE ? AND R.ttl LIKE ?',
	                     [domainName, record.name || '%', record.type || '%',
	                      record.content || '%', record.ttl || '%'],
	                     function(err, results, fields) {
	      //connection.release();
	      if (err) {
	        console.log(err);
	        callback(err);
	        return;
	      }
	      callback(null, results);
	    });
      //});
    },

    add: function(domainName, record, callback) {
      if(!checkDomainName(domainName)) {
        callback(new Error("domain invalid"));
        return;
      }
      if(!record || !checkString(record.name)) {
        callback(new Error("name invalid"));
        return;
      }
      if(!checkString(record.type)) {
        callback(new Error("type invalid"));
        return;
      }
      if(!checkString(record.content)) {
        callback(new Error("content invalid"));
        return;
      }
      if(!checkDomainName(record.name)) {
        record.name += '.'+domainName;
      }
      /*pool.getConnection(function(err, connection){
	    if (err) {
	      callback(err);
		  return;
		}*/
        connection.query('INSERT INTO records SET '
                        +'domain_id=(SELECT id FROM domains WHERE name=? LIMIT 1), '
                        +'name=?, content=?, type=?, ttl=?, '
                        +'change_date=UNIX_TIMESTAMP(), created_at=NOW(), updated_at=NOW()',
	    [domainName, record.name, record.content, record.type.toUpperCase(), record.ttl || 84600],
        function(err, results) {
          //connection.release();
          if(err) {
            console.log(err);
	        callback(err);
	      } else {
	        updateSOA(domainName, function() {
		        callback(null, results);
	        });
	      }
	    });
      //});
    },

<<<<<<< HEAD
    edit: function(domainName, record, callback) {
      if(!checkDomainName(domainName)) {
        callback(new Error("domain invalid"));
        return;
      }
      if(!record || !checkString(record.name)) {
        callback(new Error("name invalid"));
        return;
      }
      if(!checkString(record.type)) {
        callback(new Error("type invalid"));
        return;
      }
      if(!checkString(record.content)) {
        callback(new Error("content invalid"));
        return;
      }
      if(!checkDomainName(record.name)) {
        record.name += '.'+domainName;
      }
      /*pool.getConnection(function(err, connection){
	    if (err) {
	      callback(err);
		  return;
		}*/
        connection.query('UPDATE records SET '
                        +'domain_id=(SELECT id FROM domains WHERE name=? LIMIT 1), '
                        +'name=?, content=?, type=?, ttl=?, '
                        +'change_date=UNIX_TIMESTAMP(), updated_at=NOW()'
                        +'WHERE id=?',
	    [domainName, record.name, record.content, record.type.toUpperCase(), record.ttl || 84600, record.id],
        function(err, results) {
          //connection.release();
          if(err) {
            console.log(err);
	        callback(err);
	      } else {
	        updateSOA(domainName, function() {
	          callback(null, results);
	        });
	      }
	    });
      //});
    },

=======
>>>>>>> parent of 5cc6c98... record edit function added
    remove: function(domainName, record, callback) {
      if(!checkDomainName(domainName)) {
        callback(new Error("domain invalid"));
        return;
      }
      if(!record || !checkString(record.name)) {
        callback(new Error("record name invalid"));
        return;
      }
      if(!checkDomainName(record.name)) {
        record.name += '.'+domainName;
      }
      console.log([record.name, record.type || '%', record.content || '%',
                    record.ttl || '%', record.change_date || '%', record.created_at || '%',
                    record.updated_at || '%', domainName]);
      /*pool.getConnection(function(err, connection){
	    if (err) {
	      callback(err);
		  return;
	    }*/
        connection.query('DELETE FROM records WHERE name=? AND type LIKE ? AND content LIKE ?'
                       +' AND ttl LIKE ? AND change_date LIKE ? AND created_at LIKE ?'
                       +' AND updated_at LIKE ?'
                       +' AND domain_id=(SELECT id FROM domains WHERE name=? LIMIT 1)',
        [record.name, record.type || '%', record.content || '%',
         record.ttl || '%', record.change_date || '%', record.created_at || '%',
         record.updated_at || '%', domainName],
        function(err, results) {
          //connection.release();
          if(err || !results) {
            consolelog(err);
            callback(err || new Error("no match for query"));
          } else {
            updateSOA(domainName, function() {
              callback(null, results);
            });
          }
        });
      //});
    }
  }
};

function updateSOA(domainName, callback) {
  adapter.records.list(domainName, {type:'SOA'}, function(err, results) {
    if(results && results[0] && results[0].content) {
       var content = updateSoa(results[0].content);
       /*pool.getConnection(function(err, connection){
         if (err) {
	       callback(err);
		   return;
		 }*/
         connection.query("UPDATE records SET content=? WHERE type='SOA'"
                        +" AND domain_id=(SELECT id FROM domains WHERE name=? LIMIT 1)",
                         [content, domainName],
                         function(err) {
           //connection.release();
           if(err){
             console.log(err);
           }
           callback(err);
         });
       //});
    } else {
      callback(new Error("SOA record not found for serial update"));
    }
  });
}

function createTablesIfNotExist(config, callback) {
  Step(
    function() {
      readSQLFile(__dirname+'/mysql_create.sql', this);
    },
    function(err, queries) {
      if(err) {
        this(err);
        return;
      }
      var group = this.group();
      queries.forEach(function(q) {
        /*pool.getConnection(function(err, connection){
          if(err) {
            callback(err);
            return;
          }*/
          connection.query(q, group());
            //connection.release();
        //});
      });
    },
    callback
  );
}

function readSQLFile(filepath, callback) {
  fs.readFile(filepath, 'utf8', function(err, data) {
    if(err) {
      callback(err);
      return;
    }
    var queries = data.match(new RegExp("([^;]*?('.*?')?)*?;\\s*", "gi"))
    callback(null, queries.map(function(q) { return trim(q); }));
  });
}


