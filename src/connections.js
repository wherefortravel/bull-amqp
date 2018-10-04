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
      },
    )
    connections[connectionString] = connection
    const conn = await connection
    conn.once('error', (err) => {
      connections[connectionString] = null
    })
  }

  return await connection
}

export function purge(connectionString: string) {
  connections[connectionString] = null
}
