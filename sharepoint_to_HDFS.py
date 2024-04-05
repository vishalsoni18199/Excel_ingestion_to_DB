from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File
import sys

#sharepoint team channel authentication
app_setting = {
    'url' : str(sys.argv[1]),
    'client_id' : str(sys.argv[2]),
    'client_secret' : str(sys.argv[3])
}

ctx_auth = AuthenticationContext(url = app_setting['url'])
ctx_auth.acquire_token_for_app(client_id=app_setting['client_id'], client_secret=app_setting['client_secret'])
ctx = ClientContext(app_setting['url'],ctx_auth)

##reading excel
response = File.open_binary(ctx,str(sys.argv[4]))

#writing sharepoint excel into local
with open(str(sys.argv[5]), "wb") as local_file:
  local_file.write(response.content)
