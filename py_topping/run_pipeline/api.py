## FastAPI Core
from fastapi import FastAPI, Body, Request
from fastapi.responses import JSONResponse
import os, traceback, uvicorn
## Security Basic Auth
import secrets, base64
from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
## Add Doc Security
from fastapi.openapi.docs import get_redoc_html, get_swagger_ui_html
from fastapi.openapi.utils import get_openapi
# Get Query Parameter
# from pydantic import create_model

class lazy_API :
    def __init__(self, title, version, description
                     , username = 'user'
                     , password = 'password'
                     , api_weak_authen = False
                     , callback = None
                ) :
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
        ## Add Default Callback
        self.callback = callback

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
            return get_openapi(title=self.app.title, version=self.app.version, routes=self.app.routes)
        @self.app.route('/')
        @self.app.get("/docs", include_in_schema=False)
        async def get_swagger_documentation(username: str = Depends(self.get_current_username)):
            return get_swagger_ui_html(openapi_url="/openapi.json", title="docs")
        @self.app.get("/redoc", include_in_schema=False)
        async def get_redoc_documentation(username: str = Depends(self.get_current_username)):
            return get_redoc_html(openapi_url="/openapi.json", title="docs")

    def create_post(self, function, name, tags = [], example = {}, callback = 'default') :
        """Create POST Method for FastAPI"""
        # Assign Default Callback
        if (type(callback) == str) : 
            if callback == 'default' : callback = self.callback
        @self.app.post(f'/{name}' , tags = tags)
        async def post_function(  payload : dict = Body(... , example = example)
                                , username : str = Depends( self.api_get_current_username if self.api_weak_authen 
                                                            else self.get_current_username)
                                ) :
            try :
                return function(payload)
            except Exception as e :
                print(traceback.format_exc())
                if callback == None : 
                    return_error = {"error": str(traceback.format_exc())}
                else : 
                    return_error = callback({ 'traceback' : traceback.format_exc() 
                                            , 'exception' : e 
                                            , 'input' : payload})
                return JSONResponse(status_code=500
                                    , content=return_error)


    def create_get(self, function, name, tags = [], example = {}, callback = 'default') :
        """Create GET Method for FastAPI"""
        # Assign Default Callback
        if (type(callback) == str) : 
            if callback == 'default' : callback = self.callback
        # query_model = create_model("Query", **query_params)
        @self.app.get(f'/{name}' , tags = tags)
        async def get_function(  
                                # params: query_model = Depends()
                                request: Request
                                , username : str = Depends( self.api_get_current_username if self.api_weak_authen 
                                                            else self.get_current_username)
                                ) :
            try :
                params = request.query_params
                return function(**params)
            except Exception as e :
                print(traceback.format_exc())
                if callback == None : 
                    return_error = {"error": str(traceback.format_exc())}
                else : 
                    return_error = callback({ 'traceback' : traceback.format_exc() 
                                            , 'exception' : e 
                                            , 'input' : request.query_params})
                return JSONResponse(status_code=500
                                    , content=return_error)
            
    def run(self, port = 8080, host = "0.0.0.0") :
        """To run FastAPI"""
        self.gen_doc()
        uvicorn.run(self.app, port=int(os.environ.get("PORT", port)), host = host)
