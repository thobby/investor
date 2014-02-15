var express = require("express")
var app = express()

var sqlite3 = require("sqlite3").verbose();

app.use(express.static(__dirname + '/public'));

app.get("/dayTopStocks", function(req, res) {
  var db = new sqlite3.Database("stocks.db", sqlite3.OPEN_READONLY);
  sqlite3.verbose();

  db.serialize(function() {
    var data = [];
    db.all("select t.name as name, max(datum) as datum, (close / open - 1) as gain " +
          "from data d, tickers t " +
          "where d.ticker = t.id " +
          "group by d.ticker " +
          "order by gain desc",
      function(err, rows) {
        for (i = 0; i < rows.length; i++) {
          datapoint = {};
          datapoint.name = rows[i].name;
          datapoint.datum = rows[i].datum;
          datapoint.gain = rows[i].gain;
          data.push(datapoint);
        }
      console.log(data);
      res.send(data);
      db.close();
    });
  });
});

app.get("/linearRegression", function(req, res) {
  var exec = require('child_process').exec, child;

  child = exec('math/lr.py',
    function (error, stdout, stderr) {
      console.log('stdout: ' + stdout);
      console.log('stderr: ' + stderr);

      if (error !== null) {
        console.log('exec error: ' + error);
      }

      res.send(stdout);
    });
});

/*
app.get("/linearRegression", function(req, res) {
  var db = new sqlite3.Database("stocks.db", sqlite3.OPEN_READONLY);
  sqlite3.verbose();

  db.serialize(function() {
    var data = [];
    db.all("select t.name as name, gradient, intercept, r_value, p_value, std_err " +
          "from linear_regression l, tickers t " +
          "where l.ticker = t.id " +
          "order by p_value, gradient desc",
      function(err, rows) {
        for (i = 0; i < rows.length; i++) {
          datapoint = {};
          datapoint.name = rows[i].name;
          datapoint.gradient = rows[i].gradient;
          datapoint.intercept = rows[i].intercept;
          datapoint.r_value = rows[i].r_value;
          datapoint.p_value = rows[i].p_value;
          datapoint.std_err = rows[i].std_err;
          data.push(datapoint);
        }
      console.log(data);
      res.send(data);
      db.close();
    });
  });
});
*/

app.use(express.static('public'));
app.listen(8080);
console.log("Listening on port 8080");

