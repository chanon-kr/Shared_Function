## FastAPI Core
from fastapi import FastAPI, Body, Request
from fastapi.responses import JSONResponse
import os, traceback, uvicorn
## Security Basic Auth
import secrets, base64
from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials, HTTPBearer, HTTPAuthorizationCredentials
## Add Doc Security
from fastapi.openapi.docs import get_redoc_html, get_swagger_ui_html
from fastapi.openapi.utils import get_openapi
# Get Query Parameter
# from pydantic import create_model

class lazy_API :
    def __init__(self, title, version, description
                     , username = 'user'
                     , password = 'password'
                     , authen_type = 'basic' 
                     , validate_token = None 
                     , api_weak_authen = False
                     , callback = None
                     , document = True
                ) :
        """lazy_API class, to create FastAPI with a few lines of code
        title : Name of the API that will display in document (Str)
        version : Version of the API that will display in document (Str)
        description : Description of the API that will display in document (Str)
        username :  User for Authenticate in Document and Basic Authen (Str), Default 'user'
        password : Password for Authenticate in Document and Basic Authen (Str), Default 'password'
        authen_type : Type of authentication ['basic','token',None], Default 'basic' 
        validate_token : Validate Function or Token for Token Authen (Str or Function), Default None 
        api_weak_authen : Move lazy_API's Authenticate query or parameter to - 
                          - GET API 
                            - Basic >> query 'username' and 'password' 
                            - Token >> query 'token' 
                          - POST API 
                            - Basic >> body 'username' and 'password' 
                            - Token >> body 'token' 
                          To aviod conflict with GCP's authentication (Boolean), Default False
        callback :  A custom callback when API fail.
                    The custom callback will feed with this dict ;
                    { 'traceback' : traceback.format_exc() 
                    , 'exception' : e 
                    , 'input' : query input for GET or body input for POST}
                    (Function), Default None
        document : To enable FastAPI's document or not (Boolean), Default True
        """
        ## Create Fast API
        self.app = FastAPI(   title= title
                            , version= version
                            , description= description
                            , docs_url=None
                            , redoc_url=None
                            , openapi_url = None)     
        ## INIT authen method ####
        assert authen_type in ['basic', 'token', None], "authen_type must be 'basic', 'token' or None"
        self.authen_type = authen_type
        ## Initial Basic Auth
        self.storage = [base64.b64encode(username.encode('ascii')),
                        base64.b64encode(password.encode('ascii'))]
        ## Initial Bearer Token
        if authen_type == 'token' :
            assert callable(validate_token) | (type(validate_token) == str), "When using authen_type 'token', validate_token must be a function or string"
        if callable(validate_token) : self.validate_token = validate_token
        else : 
            self.storage_token = base64.b64encode(validate_token.encode('ascii'))
            self.validate_token = lambda x : x == base64.b64decode(self.storage_token).decode('ascii')
        ## Other Parameter
        self.api_weak_authen = api_weak_authen
        self.callback = callback
        self.document = document

    ## Basic Authen
    def basic_authen_check(self, credentials: HTTPBasicCredentials = Depends(HTTPBasic())):
        """Basic Authentication"""
        correct_username = secrets.compare_digest(credentials.username, base64.b64decode(self.storage[0]).decode('ascii'))
        correct_password = secrets.compare_digest(credentials.password, base64.b64decode(self.storage[1]).decode('ascii'))
        if (correct_username and correct_password): return credentials.username
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Incorrect email or password",
                headers={"WWW-Authenticate": "Basic"},)

    ## Bearer Token Authen ####
    def token_authen_check(self, auth: HTTPAuthorizationCredentials = Depends(HTTPBearer())):
        if not auth or auth.scheme != "Bearer":
            raise HTTPException(status_code=401, detail="Invalid authentication credentials")
        token = auth.credentials
        # Perform token validation logic here
        if self.validate_token(token): return token
        raise HTTPException(status_code=401, detail="Invalid token", headers={"WWW-Authenticate": "Bearer Token"})    

    ## For API
    def weak_authen_check(self, input_dict):
        """
        Sub Authentication for API in order to use with GCP Authentication
        , only use this function when 'api_weak_authen' is True
        """
        if self.authen_type == None : return 'ok'
        elif self.authen_type == 'basic' :
            correct_username = input_dict.get('username','') == base64.b64decode(self.storage[0]).decode('ascii')
            correct_password = input_dict.get('password','') == base64.b64decode(self.storage[1]).decode('ascii')
            if (correct_username and correct_password): return 'ok'
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Incorrect email or password",
                    headers={"WWW-Authenticate": "Basic"},)
        elif self.authen_type == 'token' :
            if self.validate_token(input_dict.get('token','')) : return 'ok'
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Invalid token",
                    headers={"WWW-Authenticate": "Bearer Token"},)

    def get_weak_authen_check(self, request: Request) :
        params = request.query_params
        self.weak_authen_check(params)

    def post_weak_authen_check(self, payload: dict):
        self.weak_authen_check(payload)

    def select_authen_type(self, weak_method) :
        if self.api_weak_authen | (self.authen_type == None) :
            return weak_method
        elif self.authen_type == 'basic' :
            return self.basic_authen_check
        elif self.authen_type == 'token' :
            return self.token_authen_check

    def gen_doc(self) :           
        """Create Auth Doc"""
        @self.app.get("/openapi.json", include_in_schema=False)
        async def openapi(username: str = Depends(self.basic_authen_check)):
            return get_openapi(title=self.app.title, version=self.app.version, description= self.app.description, routes=self.app.routes)
        @self.app.route('/')
        @self.app.get("/docs", include_in_schema=False)
        async def get_swagger_documentation(username: str = Depends(self.basic_authen_check)):
            return get_swagger_ui_html(openapi_url="/openapi.json", title="docs")
        @self.app.get("/redoc", include_in_schema=False)
        async def get_redoc_documentation(username: str = Depends(self.basic_authen_check)):
            return get_redoc_html(openapi_url="/openapi.json", title="docs")

    def create_post(self, function, name, tags = [], example = {}, callback = 'default') :
        """Create POST Method for FastAPI"""
        # Assign Default Callback
        if (type(callback) == str) : 
            if callback == 'default' : callback = self.callback
        # Assign Authen ####
        authen_selected = self.select_authen_type(self.post_weak_authen_check)
        @self.app.post(f'/{name}' , tags = tags)
        async def post_function(  payload : dict = Body(... , example = example)
                                , username : str = Depends( authen_selected)
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
        # Assign Authen ####
        authen_selected = self.select_authen_type(self.get_weak_authen_check)
        # query_model = create_model("Query", **query_params)
        @self.app.get(f'/{name}' , tags = tags)
        async def get_function(  
                                # params: query_model = Depends()
                                request: Request
                                , username : str = Depends( authen_selected)
                                ) :
            try :
                params = request.query_params
                return function(params)
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
        if self.document : self.gen_doc()
        uvicorn.run(self.app, port=int(os.environ.get("PORT", port)), host = host)
