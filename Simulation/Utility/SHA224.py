from hashlib import sha224

# hash方法具体实现

class _SHA224:
    def __init__(self):
        self.result=""
        
    def SHA224_Hash(self, value=""):
        hash_sha224 = sha224()
        value_bytes = value.encode()
        hash_sha224.update(value_bytes)
        self.result = hash_sha224.hexdigest()
        return self.result