const config = require("./configs.json");
const express = require("express");
const app = express();
const axios = require("axios");
const util = require("util");
const { DateTime } = require("luxon"); 
const port = 1220;
app.listen(port, () => {
   console.log(`Server Start on http://localhost:${port}`);
});

var redis = require("redis");
var Rclient = redis.createClient({
   port: config.redis.port,
   host: config.redis.ip,
});
Rclient.on("error", function (err) {
   console.log("Error " + err);
});

const { tryJSONparse } = require("./lib");
let Options = config.ksqlOptions;
const url = require("url");
const http = require("http");
const POST = "post";
const GET = "get";
const DELETE = "delete";
const PUT = "put";
/** kafka **/
const kafka = require("kafka-node");
var HighLevelProducer = kafka.HighLevelProducer,
   client = new kafka.KafkaClient({
      kafkaHost: config.kafkaHost,
   }),
   producer = new HighLevelProducer(client, {
      partitionerType: 3,
   });
producer.on("ready", function () {
   console.log("Producer is on ready");
});
producer.on("error", function (err) {
   console.log("error", err);
});

let SESSIONID = "";
const gwOptions = {
   hostname: config.gwOption.hostname,
   port: config.gwOption.port,
};
/**
 * @method CreateSession
 * @param planner blink, old
 * @param execution_type streaming, bulk
 *
 * sql-gateway create session
 */
function create_session() {
   gwOptions.method = POST;
   gwOptions.path = "/v1/sessions";
   let createSessionBody = {
      planner: "blink",
      execution_type: "streaming",
   };

   //Send Request to sql-gateway Server
   var request = http.request(gwOptions, function (response) {
      let fullBody = "";

      response.on("data", function (chunk) {
         fullBody += chunk;
      });

      response.on("end", function () {
         console.log(fullBody);
         SESSIONID = JSON.parse(fullBody)["session_id"];
         console.log("SESSION ID : ", SESSIONID);

         var config = {
            method: "post",
            url: `http://localhost:1005/flink/${SESSIONID}`,
            headers: {
               Accept: "application/json",
               "Content-Type": "application/json;ty=4",
               "X-M2M-RI": "1234",
            },
         };

         axios(config)
            .then(function (response) {
            })
            .catch(function (error) {
               console.log(error);
            });
      });

      response.on("error", function (error) {
         console.error(error);
      });
   });
   request.write(JSON.stringify(createSessionBody));
   request.end();
}
create_session();

function create_flink_sensor_table(sensorName) {
   console.log("create Sensor Table");
   gwOptions.path = `/v1/sessions/${SESSIONID}/statements`;
   gwOptions.method = POST;

   let createTableSQL = {
      statement: `CREATE TABLE ${sensorName}(\`tmp\` BIGINT, \`sensor_id\` STRING, \`sensor_value\` STRING, \`sensor_rowtime\` TIMESTAMP(3) METADATA FROM 'timestamp',  WATERMARK FOR sensor_rowtime AS sensor_rowtime, PRIMARY KEY (tmp) NOT ENFORCED) WITH ('connector' = 'upsert-kafka', 'topic' = '${sensorName}', 'properties.bootstrap.servers' = '${config.kafkaHost}', 'key.format' = 'json','value.format' = 'json')`,
   };

   var request = http.request(gwOptions, function (response) {
      let fullBody = "";

      response.on("data", function (chunk) {
         fullBody += chunk;
      });

      response.on("end", function () {
         console.log(fullBody);
      });

      response.on("error", function (error) {
         console.error(error);
      });
   });
   request.write(JSON.stringify(createTableSQL));
   request.end();
}


/*
 * POST sensor Creation
 */
app.post("/DigitalConnector/SensorGroup", function (req, res) {
   let fullBody = "";
   req.on("data", function (chunk) {
      fullBody += chunk;
   });

   req.on("end", async function () {
      let messageObject = fullBody;
      if (tryJSONparse(messageObject)) {
         sensorNameObj = tryJSONparse(messageObject);
         if (sensorNameObj?.name && sensorNameObj?.mq) {
            const flag = await checkSensorNameExist(sensorNameObj.name).then(
               function (flag) {
                  return flag;
               }
            );
            if (flag) {
               res.status(500).send("sensor is already exist");
            } else {
               const sensorName = sensorNameObj.name;
               Rclient.rpush("SensorGroup", sensorName);
               const sensorFields = Object.keys(sensorNameObj);
               for (var i = 0; i < sensorFields.length; i++) {
                  const field = sensorFields[i];
                  if (sensorFields[i] != "name") {
                     Rclient.hset(
                        sensorNameObj.name,
                        sensorFields[i],
                        JSON.stringify(sensorNameObj[field])
                     );
                  }
               }
               create_flink_sensor_table(sensorName);
               res.status(200).send("create sensorGroup");
               //BrokerNotAvailableError: Could not find the leader
               client.refreshMetadata([sensorNameObj.name], (err) => {
                  if (err) {
                     console.warn("Error refreshing kafka metadata", err);
                  }
               });
            }
         } else {
            res.status(500).send("please check mandatory field");
         }
      } else {
         res.status(500).send("is not a json structure");
      }
   });
});

/*
 * sensorGroup Retrieve
 */
app.get("/DigitalConnector/SensorGroup", async (req, res) => {
   let SensorNameList = await getSensorNameList().then((List) => {
      return List;
   });

   res.send({ SensorList: SensorNameList });
});

/*
 * sensorGroup delete
 */
app.delete("/DigitalConnector/SensorGroup", async (req, res) => {
   const resLength = await getListLength_delete().then(function (resLength) {
      return resLength;
   });

   Rclient.DEL("SensorGroup");
   res.send({ deleted: resLength });
});

function getListLength_delete() {
   return new Promise((resolve) => {
      Rclient.lrange("SensorGroup", 0, -1, function (err, keys) {
         if (err) throw err;
         keys.forEach((key) => {
            Rclient.DEL(key);
         });

         resolve(keys.length);
      });
   });
}

function getSensorNameList() {
   return new Promise((resolve) => {
      Rclient.lrange("SensorGroup", 0, -1, function (err, keys) {
         if (err) throw err;
         resolve(keys);
      });
   });
}
async function checkSensorNameExist(SensorName) {
   let SensorNameList = await getSensorNameList().then((List) => {
      return List;
   });
   let flag = false;
   return new Promise((resolve, reject) => {
      for (i in SensorNameList) {
         if (SensorNameList[i] == SensorName) {
            flag = true;
         }
      }
      resolve(flag);
   });
}
/*
 * PUT(update) sensor Creation
 */

app.put("/DigitalConnector/SensorGroup", function (req, res) {
   let fullBody = "";
   req.on("data", function (chunk) {
      fullBody += chunk;
   });

   req.on("end", async function () {
      let messageObject = fullBody;
      if (tryJSONparse(messageObject)) {
         sensorNameObj = tryJSONparse(messageObject);
         if (sensorNameObj?.name && sensorNameObj?.mq) {
            const flag = await checkSensorNameExist(sensorNameObj.name).then(
               function (flag) {
                  return flag;
               }
            );
            if (!flag) {
               res.status(500).send("Unregistered sensor.");
            } else {
               const sensorName = sensorNameObj.name;
               const sensorFields = Object.keys(sensorNameObj);
               for (var i = 0; i < sensorFields.length; i++) {
                  const field = sensorFields[i];
                  if (sensorFields[i] != "name") {
                     Rclient.hset(
                        sensorNameObj.name,
                        sensorFields[i],
                        JSON.stringify(sensorNameObj[field])
                     );
                  }
               }

               res.status(200).send("update sensorGroup");
            }
         } else {
            res.status(500).send("please check mandatory field");
         }
      } else {
         res.status(500).send("is not a json structure");
      }
   });
});

/*
 * sensor Data Creation
 */
app.post("/DigitalConnector/SensorGroup/:sensorName", function (req, res) {
   let fullBody = "";

   req.on("data", function (chunk) {
      fullBody += chunk;
   });

   req.on("end", async function () {
      if (!req.params?.sensorName) {
         res.status(500).send("please check sensorName parameter");
      }
      let messageObject = fullBody;
      if (tryJSONparse(messageObject)) {
         sensorNameObj = tryJSONparse(messageObject);
         if (sensorNameObj?.data) {
            const MQ = await getMessageQueList(req.params.sensorName).then(
               function (MQ) {
                  return MQ;
               }
            );
            if (MQ == null) {
               res.status(200).send("Unregistered sensor.");
            } else {
               res.status(200).send("ok");
               for (let index of JSON.parse(MQ)) {
                  switch (index) {
                     case "kafka":
                        console.log("send to kafka ", sensorNameObj.data);
                        const valueObjectMessage = { tmp: 1, sensor_id: req.params.sensorName, sensor_value: sensorNameObj.data };
                        const kafkaKey = {tmp: 1};
                        kafkaProducer(
                           req.params.sensorName,
                           JSON.stringify(kafkaKey),
                           JSON.stringify(valueObjectMessage)
                        ); 
                        break;
                     case "mqtt":
                        console.log("send to mqtt ", sensorNameObj.data);
                        break;
                  }
               }
            }
         } else {
            res.status(500).send("please check mandatory field");
         }
      } else {
         res.status(500).send("is not a json structure");
      }
   });
});

function kafkaProducer(sensorName, kafkaKey, sensorValue) {
   let payload = [
      {
         topic: sensorName,
         key: kafkaKey,
         messages: sensorValue, 
      },
   ];
   console.log("payload: ", payload);
   producer.send(payload, function (err, data) {
      if (err) console.log(err);
      else console.log(data);
   });
}

function getMessageQueList(sensorName) {
   return new Promise((resolve) => {
      Rclient.hget(sensorName, "mq", function (err, value) {
         resolve(value);
      });
   });
}

/*
 * sensor Data delete
 */
app.delete("/DigitalConnector/SensorGroup/:sensorName", async (req, res) => {
   if (!req.params?.sensorName) {
      res.status(500).send("please check sensorName parameter");
   } else {
      const flag = await checkSensorNameExist(req.params.sensorName).then(
         function (flag) {
            return flag;
         }
      );
      if (!flag) {
         res.status(500).send("Unregistered sensor.");
      } else {
         Rclient.DEL(req.params.sensorName, redis.print);
         Rclient.lrem("SensorGroup", -1, req.params.sensorName);
         res.send({ success: 1 });
      }
   }
});

/*
 * sensor Data Retrieve
 */
app.get("/DigitalConnector/SensorGroup/:sensorName", async (req, res) => {
   if (!req.params?.sensorName) {
      res.status(500).send("please check sensorName parameter");
   } else {
      let resObject = { name: req.params.sensorName };
      const keys = await getKeys(req.params.sensorName).then(function (keys) {
         return keys;
      });
      if (keys.length == 0) {
         res.status(200).send("Unregistered sensor.");
      } else {
         for (let key of keys) {
            const value = await getValue(req.params.sensorName, key);
            resObject[key] = value;
         }
         res.status(200).send(resObject);
      }
   }
});


function getKeys(sensorName) {
   return new Promise((resolve) => {
      Rclient.hkeys(sensorName, function (err, keys) {
         resolve(keys);
      });
   });
}


function getValue(sensorName, key) {
   return new Promise((resolve, reject) => {
      Rclient.hget(sensorName, key, function (err, value) {
         if (err) reject(err);
         resolve(JSON.parse(value));
      });
   });
}


app.delete("*", function (req, res) {
   res.send("Bad Request (Wrong Url)", 404);
});

app.get("*", function (req, res) {
   res.send("Bad Request (Wrong Url)", 404);
});

app.post("*", function (req, res) {
   res.send("Bad Request (Wrong Url)", 404);
});

app.put("*", function (req, res) {
   res.send("Bad Request (Wrong Url)", 404);
});