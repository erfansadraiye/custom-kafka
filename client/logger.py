class Logger():
    def __init__(self):
        self.filename = 'logs.txt'
        with open(self.filename, 'w+') as _:
            pass
    
    def log(self, s):
        with open(self.filename, 'a') as f:
            f.write(f'{s}\n')
