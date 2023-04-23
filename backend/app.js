const express = require('express')
const http = require('http')
const kafka = require('kafka-node')
const WebSocket = require('ws')
const Consumer = kafka.Consumer

const client = new kafka.KafkaClient()
const consumer = new Consumer(client, [{ topic: 'stream' }], {})
const app = express()
const server = http.createServer(app)
const wss = new WebSocket.Server({ server })

server.listen(1337, () => {
	console.log('server running')
})

wss.on('connection', function (ws) {
	consumer.on('message', (message) => {
		ws.send(JSON.stringify(JSON.parse(message.value)))
	})
})
