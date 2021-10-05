from multinet import Multirequest

# Creating request
request = Multirequest()

# Sync get
data = request.get(("simple.test", "degM"))
degM = data[("simple.test", "degM")]

# Fancy async
@request.async_handler(("simple.test", "sinM"))
def handle_sinM(data, ppm_user):
    print(data, ppm_user)

request.start_asyncs()


