"""Co-commit an order row and a Spinach job.

Install the extra and point the process at the database::

    pip install spinach[postgres]
    export SPINACH_POSTGRES_DSN=postgresql://localhost/spinach

``require_ssl`` is False here because this sample targets a local server
without TLS.
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
