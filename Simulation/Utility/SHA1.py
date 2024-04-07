from hashlib import sha1

# hash方法具体实现

class _SHA1:
    def __init__(self):
        self.result=""
        
    def SHA1_Hash(self, value=""):
        hash_sha1 = sha1()
        value_bytes = value.encode()
        hash_sha1.update(value_bytes)
        self.result = hash_sha1.hexdigest()
        return self.result