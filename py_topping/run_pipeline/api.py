## FastAPI Core
from fastapi import FastAPI, Body
from fastapi.responses import JSONResponse
import os, traceback, uvicorn
## Security Basic Auth
import secrets, base64
from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
## Add Doc Security
from fastapi.openapi.docs import get_redoc_html, get_swagger_ui_html
from fastapi.openapi.utils import get_openapi

class lazy_API :
    def __init__(self, title, version, description
                     , username = 'user'
                     , password = 'password'
                     , api_weak_authen = False) :
        """lazy_API class, to create FastAPI with a few lines of code"""
        ## Create Fast API
        self.app = FastAPI(   title= title
                            , version= version
                            , description= description
                            , docs_url=None
                            , redoc_url=None
                            , openapi_url = None)     
        ## Initial Basic Auth
        self.storage = [base64.b64encode(username.encode('ascii')),
                        base64.b64encode(password.encode('ascii'))]
        self.api_weak_authen = api_weak_authen

    ## Basic Authen
    def get_current_username(self, credentials: HTTPBasicCredentials = Depends(HTTPBasic())):
        """Basic Authentication"""
        correct_username = secrets.compare_digest(credentials.username, base64.b64decode(self.storage[0]).decode('ascii'))
        correct_password = secrets.compare_digest(credentials.password, base64.b64decode(self.storage[1]).decode('ascii'))
        if (correct_username and correct_password): return credentials.username
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Incorrect email or password",
                headers={"WWW-Authenticate": "Basic"},)
    ## For API
    def api_get_current_username(self, payload: dict):
        """
        Sub Authentication for API in order to use with GCP Authentication
        , only use this function when 'api_weak_authen' is True
        """
        correct_username = payload.get('username','') == base64.b64decode(self.storage[0]).decode('ascii')
        correct_password = payload.get('password','') == base64.b64decode(self.storage[1]).decode('ascii')
        if (correct_username and correct_password): return 'ok'
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Incorrect email or password",
                headers={"WWW-Authenticate": "Basic"},)

    def gen_doc(self) :           
        """Create Auth Doc"""
        @self.app.get("/openapi.json", include_in_schema=False)
        async def openapi(username: str = Depends(self.get_current_username)):
            return get_openapi(title=app.title, version=app.version, routes=app.routes)
        @self.app.route('/')
        @self.app.get("/docs", include_in_schema=False)
        async def get_swagger_documentation(username: str = Depends(self.get_current_username)):
            return get_swagger_ui_html(openapi_url="/openapi.json", title="docs")
        @self.app.get("/redoc", include_in_schema=False)
        async def get_redoc_documentation(username: str = Depends(self.get_current_username)):
            return get_redoc_html(openapi_url="/openapi.json", title="docs")

    def create_post(self, function, name, tags = [], example = {}) :
        """Create POST Method for FastAPI"""
        @self.app.post(f'/{name}' , tags = tags)
        async def post_function(  payload : dict = Body(... , example = example)
                                , username : str = Depends( self.api_get_current_username if self.api_weak_authen 
                                                            else self.get_current_username)
                                ) :
            try :
                return function(payload)
            except  :
                print(traceback.format_exc())
                return JSONResponse(status_code=500
                                    , content={"error": str(traceback.format_exc())})

    def run(self, port = 8080, host = "0.0.0.0") :
        """To run FastAPI"""
        self.gen_doc()
        if __name__ == "__main__":
            uvicorn.run(self.app, port=int(os.environ.get("PORT", port)), host = host)