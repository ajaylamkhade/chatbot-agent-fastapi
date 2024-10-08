from fastapi import FastAPI
import uvicorn
app = FastAPI()

@app.get("/")
def home():
    return {"Hello" : "World"}
