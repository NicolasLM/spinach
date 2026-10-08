"""Co-commit an application row and a Spinach job.

The DSN is read from the environment. Nothing in this example embeds a
password. Install the extra with ``pip install spinach[postgres]`` and
point SPINACH_POSTGRES_DSN at the database, using SCRAM-SHA-256 and
``sslmode=verify-full`` outside of local development.
``require_ssl`` defaults to True. This sample turns it off for a
local server that has no TLS.
"""
import os

import psycopg

from spinach import Engine
from spinach.brokers.postgres import PostgresBroker

dsn = os.environ['SPINACH_POSTGRES_DSN']
spin = Engine(PostgresBroker(dsn, require_ssl=False))


@spin.task(name='process_order')
def process_order(order_id):
    print('processing {}'.format(order_id))


def create_order(order_id, sku):
    with psycopg.connect(dsn) as conn:
        with conn.transaction():
            conn.execute(
                'CREATE TABLE IF NOT EXISTS demo_orders ('
                'id text PRIMARY KEY, sku text NOT NULL)'
            )
            conn.execute(
                'INSERT INTO demo_orders (id, sku) VALUES (%s, %s)',
                (order_id, sku),
            )
            with spin.join_transaction(conn):
                spin.schedule(process_order, order_id)


if __name__ == '__main__':
    create_order('1001', 'sku-1')
    print('Starting workers, ^C to quit')
    spin.start_workers()
