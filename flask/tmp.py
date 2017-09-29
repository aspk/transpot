from redis import StrictRedis


redis_conn = StrictRedis(host='172.31.3.244', port=6379, db=0, password = '3c5bac3ecc34b94cf2ecb65d0b7a2ba7d664221f0116c50ba5557a41804e83e8')

topics = ["bus","bike","taxi"]
for topic in topics:
	print(topic)
	print(redis_conn.georadius(topic, -74.0, 40.65, 1000, 'ft', True, True, False, None, 'ASC'))

