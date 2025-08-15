from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pymongo.errors import ServerSelectionTimeoutError
from database import init_db
from dotenv import load_dotenv
from auth.auth import router as auth_router
from auth.get_client_token import router as get_client_token_router
from auth.token_validation_router import router as validate_token_router
from routes.adm1 import router as adm1
from routes.adm2 import router as adm2
from routes.adm3 import router as adm3
from routes.farm import router as farm
from routes.farmpolygons import router as farmpolygons
from routes.enterprise import router as enterprise
from routes.suppliers import router as suppliers
from routes.deforestation import router as deforestation
from routes.protectedareas import router as protectedareas
from routes.farmingareas import router as farmingareas
from routes.analysis import router as analysis
from routes.adm3risk import router as adm3risk
from routes.farmrisk import router as farmrisk
from routes.enterpriserisk import router as enterpriserisk
from routes.movement import router as movement
from routes.analisys_risk_router import router as get_farmrisk_by_analyses
from routes.get_analysis import router as get_analysis_router
from routes.adm3risk_by_analysis_and_adm3 import router as adm3risk_by_analysis_and_adm3
from routes.enum import router as enum
from tools.logger import logger

app = FastAPI(
    title="Ganabosques search api"
)

load_dotenv()

try:
    init_db()
    logger.info("Conexión a MongoDB exitosa")
except ServerSelectionTimeoutError as e:
    logger.exception("No se pudo conectar con MongoDB al iniciar")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.exception_handler(ServerSelectionTimeoutError)
async def db_connection_error_handler(request: Request, exc: ServerSelectionTimeoutError):
    return JSONResponse(
        status_code=503,
        content={"detail": "Error de conexión con la base de datos. Verifica si el servidor está en línea."},
    )

# Auth
app.include_router(auth_router)
app.include_router(get_client_token_router)
app.include_router(validate_token_router)

# Administrative levels
app.include_router(adm1)
app.include_router(adm2)
app.include_router(adm3)

# Farm and Enterprise
app.include_router(farm)
app.include_router(farmpolygons)
app.include_router(enterprise)
app.include_router(suppliers)

# Spatial data
app.include_router(deforestation)
app.include_router(protectedareas)
app.include_router(farmingareas)

# Analysis risk
app.include_router(analysis)
app.include_router(adm3risk)
app.include_router(farmrisk)
app.include_router(enterpriserisk)

# Movements
app.include_router(movement)

# Enumns
app.include_router(enum)
app.include_router(get_farmrisk_by_analyses)
app.include_router(get_analysis_router)
app.include_router(adm3risk_by_analysis_and_adm3)


# uvicorn main:app --reload