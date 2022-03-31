/*
 * Copyright 2022 Teppo Kurki <teppo.kurki@iki.fi>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

require('dotenv').config()
const mqtt = require('mqtt');
const mqttClient = mqtt.connect('mqtt://172.30.0.1');
const { Pool, Client } = require('pg')
const pm2 = require('pm2');
const pool = new Pool({
  user: 'username',
  host: '172.30.0.1',
  database: 'example',
  password: 'password',
  port: 5432,
})

pm2.launchBus((err, bus) => {
    console.log('connected', bus);

    bus.on('process:exception', function(data) {
      console.log(arguments);
    });
  
    bus.on('log:err', function(data) {
      console.log('logged error',arguments);
    });
  
    bus.on('reconnect attempt', function() {
      console.log('Bus reconnecting');
    });
  
    bus.on('close', function() {
      console.log('Bus closed');
    });
});

mqttClient.on('connect', () => {
    console.log("Connected!");
    mqttClient.subscribe('+/signalk/delta');
});

mqttClient.on('message', (topic, message) => {
    let json = JSON.parse(message.toString());
    console.log(message.toString());
    insertMetrics(json.time, json.context, json.path, json.value);
});

async function insertMetrics(time, device_id, path, value) {
    let client;
    try {
        client = await pool.connect()
    } catch (e) {
        console.error("Failed to connect to database pool due to: " + e.message)
        return;
    }
    try {
        const result = await client.query('INSERT INTO sensor_metrics(time, device_id, path, value) VALUES($1, $2, $3, $4);', [time, device_id, path, value]);
        if (result.rowCount != 1) {
            console.warn("Row count was not equal to 1 in: ", result);
        }
        console.debug('sql insertMetrics');
        client.query('COMMIT', err => {
          if (err) {
            console.error('Error committing transaction', err.stack)
          }
        })
    } finally {
        client.release()
    }
}