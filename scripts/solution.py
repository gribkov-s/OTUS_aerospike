from __future__ import print_function
import aerospike
from aerospike import predicates as p
import logging


logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG)
#logging.info('Testig local Aerospike connectivity')

# Configure the client
config = {
  'hosts': [ ('db', 3000) ]
}

ns = 'test'
set = 'otus'

# Create a client and connect it to the cluster
try:
  client = aerospike.client(config).connect()
except:
  import sys
  logging.error("failed to connect to the cluster with", config['hosts'])
  sys.exit(1)

# Functions for working with records from customers set

def print_result(args):
    key, metadata, record = args
    print(record)

def add_customer(customer_id, phone_number, lifetime_value):
    key = (ns, set, customer_id)
    bins = {
            'phone_number': phone_number,
            'lifetime_value': lifetime_value
            }
    client.put(key, bins)

def get_ltv_by_id(customer_id):
    key = (ns, set, customer_id)
    (key, meta, bins) = client.select(key, ('lifetime_value', ''))
    print(bins)

def get_ltv_by_phone(phone_number):
    query = client.query(ns, set)
    query.select('lifetime_value')
    query.where(p.equals('phone_number', phone_number))
    query.foreach(print_result)
  

add_customer(100, '+79876543299', 1000)
get_ltv_by_id(100)
get_ltv_by_phone('+79876543299')