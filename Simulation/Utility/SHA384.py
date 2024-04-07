from hashlib import sha384

# hash方法具体实现

class _SHA384:
    def __init__(self):
        self.result=""
        
    def SHA384_Hash(self, value=""):
        hash_sha384 = sha384()
        value_bytes = value.encode()
        hash_sha384.update(value_bytes)
        self.result = hash_sha384.hexdigest()
        return self.result