// @flow

import amqp from 'amqplib'

const connections: { [key: string]: any } = Object.create(null)

export async function connect(connectionString: string) {
  let connection = connections[connectionString]

  if (!connection) {
    connection = amqp.connect(
      connectionString,
      {
        noDelay: true, // disable nagle
        heartbeat: 1,
      },
    )
    connections[connectionString] = connection
    await connection
  }

  return await connection
}

export function purge(connectionString: string) {
  connections[connectionString] = null
}
