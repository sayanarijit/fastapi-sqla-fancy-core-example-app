from fastapi import FastAPI

import app_1_hidden_context
import app_2_dependency_injection
import app_3_optional_param

app = FastAPI()

app.include_router(app_1_hidden_context.app.router, prefix="/v1")
app.include_router(app_2_dependency_injection.app.router, prefix="/v2")
app.include_router(app_3_optional_param.app.router, prefix="/v3")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app:app")
