from libinsights_allref.connect import con

def lambda_handler(event, context):
    print(0, 'doom')
    return con()

if __name__=='__main__':
    print(lambda_handler(0, 1))
