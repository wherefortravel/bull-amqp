// @flow

import amqp from 'amqplib'

const connections: { [key: string]: any } = {}

export async function connect(connectionString: string) {
  let connection = connections[connectionString]

  if (!connection) {
    connection = await amqp.connect(connectionString)
    connections[connectionString] = connection
  }

  return connection
}
