const config = require("./configs.json");
const express = require("express");
const app = express();
var unirest = require("unirest");
const util = require("util");
const where = require("lodash.where");
const { DateTime } = require("luxon");
const mqtt = require("mqtt");
app.listen(1005, () => {
   console.log("Server Start on port 1005");
});
const { tryJSONparse } = require("./lib");
let Options = config.ksqlOptions;
const redis = require("redis");
const Rclient = redis.createClient({
   port: config.redis.port,
   host: config.redis.ip,
});

Rclient.on("error", function (error) {
   console.error(error);
});

const client = mqtt.connect(`mqtt://${config.mqtt.ip}:${config.mqtt.port}`);

client.on("connect", function () {
   console.log("connected!", client.connected);
});
client.on("error", (error) => {
   console.log("Can't connect" + error);
   process.exit(1);
});

let SESSIONID = "";
const gwOptions = {
   hostname: config.gwOption.hostname,
   port: config.gwOption.port,
};
app.post("/flink/:session", (req, res) => {
   SESSIONID = req.params.session;
   console.log("sessionID: ", SESSIONID);
   res.end();
});

function create_flink_DO_table(DOobject) {
   console.log("create DO Table");
   let DOName = DOobject.name;
   gwOptions.path = `/v1/sessions/${SESSIONID}/statements`;
   gwOptions.method = POST;

   let createStreamSQL = {
      statement: ``,
   };

   let sensorList = DOobject.sensor;
   console.log(sensorList);

   if (sensorList.length == 1) {
      createStreamSQL.statement = `CREATE TABLE ${DOName}(tmpA BIGINT, sensor1_id STRING, sensor1_value STRING, sensor1_rowtime TIMESTAMP(3), PRIMARY KEY (tmpA) NOT ENFORCED) WITH ('connector' = 'upsert-kafka', 'topic' = 'DO_${DOName}','properties.bootstrap.servers' = '${config.kafkaHost}', 'key.format' = 'json', 'value.format' = 'json')`;
   } else {
      createStreamSQL.statement = `CREATE TABLE ${DOName} (tmpA BIGINT, sensor1_rowtime TIMESTAMP(3), `;

      for (i = 1; i <= sensorList.length; i++) {
         createStreamSQL.statement += `sensor${i}_id STRING, sensor${i}_value STRING, `;
      }

      createStreamSQL.statement += `PRIMARY KEY (tmpA) NOT ENFORCED) WITH('connector' = 'upsert-kafka', 'topic' = 'DO_${DOName}','properties.bootstrap.servers' = '${config.kafkaHost}', 'key.format' = 'json', 'value.format' = 'json')`;
   }

   console.log("createStreamSQL: ", createStreamSQL);

   let insertTableSQL = {
      statement: `INSERT INTO ${DOName} select `,
   };

   if (sensorList.length == 1) {
      insertTableSQL.statement += `${sensorList[0]}.tmp, ${sensorList[0]}.sensor_id, ${sensorList[0]}.sensor_value, ${sensorList[0]}.sensor_rowtime FROM ${sensorList[0]}`;
   } else {
      insertTableSQL.statement += `${sensorList[0]}.tmp, ${sensorList[0]}.sensor_rowtime, `;

      for (i = 0; i < sensorList.length; i++) {
         insertTableSQL.statement += `${sensorList[i]}.sensor_id, ${sensorList[i]}.sensor_value `;
         if (i != sensorList.length - 1) {
            insertTableSQL.statement += `, `;
         } else if (i == sensorList.length - 1) {
            insertTableSQL.statement += `from  ${sensorList[0]} `;
         }
      }

      for (i = 0; i < sensorList.length - 1; i++) {
         insertTableSQL.statement += `left join ${
            sensorList[i + 1]
         } for system_time as of ${sensorList[i]}.sensor_rowtime on ${
            sensorList[i + 1]
         }.tmp=${sensorList[i]}.tmp `;
      }
   }

   console.log("insertTableSQL: ", insertTableSQL);

   //Send Request to sql-gateway Server
   var request = http.request(gwOptions, function (response) {
      let fullBody = "";

      response.on("data", function (chunk) {
         fullBody += chunk;
      });

      response.on("end", function () {
         console.log(fullBody);
         console.log("Insert Sensor Table to DO Table");

         var insertRequest = http.request(gwOptions, function (insertResponse) {
            let fullBody = "";

            insertResponse.on("data", function (chunk) {
               fullBody += chunk;
            });

            insertResponse.on("end", function () {
               console.log(fullBody);
            });

            insertResponse.on("error", function (error) {
               console.error(error);
            });
         });
         insertRequest.write(JSON.stringify(insertTableSQL));
         insertRequest.end();
      });

      response.on("error", function (error) {
         console.error(error);
      });
   });
   request.write(JSON.stringify(createStreamSQL));
   request.end();
}


//====================================The 404 Route 
app.get("*", function (req, res) {
   res.send("Bad Request (Wrong Url)", 404);
});

app.post("*", function (req, res) {
   res.send("Bad Request (Wrong Url)", 404);
});

app.delete("*", function (req, res) {
   res.send("Bad Request (Wrong Url)", 404);
});

app.put("*", function (req, res) {
   res.send("Bad Request (Wrong Url)", 404);
});