import redis
class RedisTool(object):
    
    def __init__(self, host, port):
        self.host = host
        self.port = port
    
    def get_connect(self):
        pool = redis.ConnectionPool(host=self.host, port=self.port)
        r = redis.StrictRedis(connection_pool=pool)
        return r

if __name__ == "__main__":
    rt_obj = RedisTool('',6379)
    rt = rt_obj.get_connect()
    key = "500cda50-e93c-42d5-bbe4-694aa61b3b27_up"
    result = rt.hgetall(key)
    #result = rt.hgetall("50f50ebc-083c-4290-9a50-779d0e4efe51_up")
    print result
